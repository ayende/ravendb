using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;
using JsonSerializer = System.Text.Json.JsonSerializer;


namespace Raven.Server.Documents.AI.AiGen;

public abstract class AbstractChatCompletionClient : IDisposable
{
    private readonly string _model;
    private readonly HttpClient _client;
    private readonly string _structuredOutputSchema;
    private readonly JsonContextPool _contextPool = new JsonContextPool();
    private static readonly DocumentConventions Conventions = new DocumentConventions { UseHttpCompression = false };

    public AbstractChatCompletionClient(Uri baseUri, string model, string apiKey, string structuredOutputSchema)
    {
        _model = model;
        _client = new()
        {
            BaseAddress = baseUri,
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue("Bearer", apiKey),
                Accept = { new MediaTypeWithQualityHeaderValue("application/json") }
            }
        };
        _structuredOutputSchema = structuredOutputSchema;
    }

    public async Task<(string Result, string Usage)> CompleteAsync(string prompt, string context, CancellationToken token = default)
    {
        using var _ = _contextPool.AllocateOperationContext(out var ctx);
        using var request = GetRequest(ctx, prompt, context);
        using var response = await _client.SendAsync(request, token).ConfigureAwait(false);
        using var responseContent = await GetResponseContentAsync(ctx, response, token);

        if (response.IsSuccessStatusCode == false)
        {
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        if (responseContent.TryGet("choices", out BlittableJsonReaderArray choices) == false || choices.Length == 0)
        {
            throw new GenAiUnexpectedResponseException("No choices in response: " + responseContent)
            {
                RequestId = GetRequestId(response.Headers)
            };
        }

        var choice0 = (BlittableJsonReaderObject)choices[0];
        if (choice0.TryGet("message", out BlittableJsonReaderObject msg) == false ||
             msg.TryGet("content", out string content) == false)
        {
            throw new GenAiUnexpectedResponseException("No message/content property in choice: " + responseContent)
            {
                RequestId = GetRequestId(response.Headers)
            };
        }

        if (string.IsNullOrEmpty(content))
        {
            choice0.TryGet("finish_reason", out string finishReason);
            choice0.TryGet("refusal", out string refusal);
        
            throw new GenAiRefusedToAnswerException("The request was refused by the model")
            {
                Refusal = refusal,
                FinishReason = finishReason,
                RequestId = GetRequestId(response.Headers)
            };
        }

        if (responseContent.TryGet("usage", out BlittableJsonReaderObject usage) == false)
            throw new GenAiUnexpectedResponseException("No choices property in response: " + responseContent)
            {
                RequestId = GetRequestId(response.Headers)
            };

        return (content, usage.ToString());
    }

    private HttpRequestMessage GetRequest(JsonOperationContext ctx, string prompt, string context)
    {
        var content = new BlittableJsonContent(async stream =>
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
            {
                writer.WriteStartObject();

                if (_forTestingPurposes?.ModifyPayload != null)
                {
                    _forTestingPurposes?.ModifyPayload.Invoke(writer);
                    writer.WriteEndObject();
                }

                writer.WritePropertyName("model");
                writer.WriteString(_model);
                writer.WriteComma();
        
                writer.WritePropertyName("messages");
                writer.WriteStartArray();
                writer.WriteStartObject();
                writer.WritePropertyName("role");
                writer.WriteString("system");
                writer.WriteComma();
                writer.WritePropertyName("content");
                writer.WriteString(prompt);
                writer.WriteEndObject();
                writer.WriteComma();
                writer.WriteStartObject();
                writer.WritePropertyName("role");
                writer.WriteString("user");
                writer.WriteComma();
                writer.WritePropertyName("content");
                writer.WriteString(context);
                writer.WriteEndObject();
                writer.WriteEndArray();
                writer.WriteComma();
        
                writer.WritePropertyName("response_format");
                writer.WriteStartObject();
                writer.WritePropertyName("type");
                writer.WriteString("json_schema");
                writer.WriteComma();
                writer.WritePropertyName("json_schema");
                writer.WriteObject(await GetStructuredOutputSchemaAsBlittable());
                writer.WriteEndObject();
        
                writer.WriteEndObject();
            }
        }, Conventions);

        
        content.Headers.Add("Content-Type", "application/json");

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = content,
            RequestUri = new Uri("/v1/chat/completions", UriKind.Relative)
        };

        async Task<BlittableJsonReaderObject> GetStructuredOutputSchemaAsBlittable()
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(_structuredOutputSchema)))
            {
                return await ctx.ReadForMemoryAsync(stream, "json");
            }
        }
    }

    public virtual async Task<BlittableJsonReaderObject> GetResponseContentAsync(JsonOperationContext context, HttpResponseMessage response, CancellationToken token)
    {
        await using (var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false))
        {
            var contentLength = response.Content.Headers.ContentLength;
            if (contentLength.HasValue && contentLength == 0)
                return null;

            // we intentionally don't dispose the reader here, we'll be using it
            // in the command, any associated memory will be released on context reset
            await using (var stream = new StreamWithTimeout(responseStream))
            {
                return await context.ReadForMemoryAsync(stream, "response/object").ConfigureAwait(false);
            }
        }
    }

    [DoesNotReturn]
    private void HandleUnsuccessfulResponse(HttpResponseMessage response, BlittableJsonReaderObject responseContent)
    {
        var headers = response.Headers;
        var reqId = GetRequestId(headers);

        if (responseContent.TryGet("error", out BlittableJsonReaderObject errBjo) is false || errBjo.TryGet("message", out string message) is false)
            throw new GenAiUnexpectedResponseException("Unexpected response: " + responseContent)
            {
                RequestId = reqId
            };

        switch (response.StatusCode)
        {
            case HttpStatusCode.TooManyRequests:

                if (errBjo.TryGet("type", out string type) == false)
                    throw new GenAiUnexpectedResponseException($"No type specified (status {HttpStatusCode.TooManyRequests}): " + responseContent)
                    {
                        RequestId = reqId
                    };

                switch (type)
                {
                    case "insufficient_quota":
                        throw new GenAiInsufficientQuotaException(message)
                        {
                            RequestId = reqId
                        };

                    case "tokens":
                    case "requests":

                        var retryAfter = TimeSpan.Zero;
                        if (headers.Contains("retry-after-ms") == false)
                        {
                            throw new GenAiTooManyTokensException(message)
                            {
                                RequestId = reqId
                            };
                        }

                        if (headers.TryGetValues("x-ratelimit-reset-tokens", out var resetTokensValues))
                        {
                            // TPM
                            var retryAfterAsString = resetTokensValues.FirstOrDefault();
                            if (TryParseResetTime(retryAfterAsString, out retryAfter) == false)
                                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");
                        }

                        if (headers.TryGetValues("x-ratelimit-reset-requests", out var resetRequestsValues))
                        {
                            // RPM
                            var retryAfterAsString = resetRequestsValues.FirstOrDefault();
                            if (TryParseResetTime(retryAfterAsString, out var retryAfterForReqs) == false)
                                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

                            retryAfter = retryAfterForReqs > retryAfter ? retryAfterForReqs : retryAfter;
                        }

                        // TPM/RPM - should retry only for this exception
                        throw new GenAiRateLimitException(message)
                        {
                            RetryAfter = retryAfter,
                            RequestId = reqId
                        };
                    default:
                        throw new GenAiTooManyRequestsException(message)
                        {
                            RequestId = reqId
                        };
                }
            default:
                throw new GenUnsuccessfulRequestException(message, response.StatusCode)
                {
                    RequestId = reqId
                };
        }
    }

    private string GetRequestId(HttpResponseHeaders headers)
    {
        if (headers.TryGetValues("X-Request-ID", out var values) == false || values.IsNullOrEmpty())
        {
            return string.Empty;
        }
        return  values.FirstOrDefault();
    }

    private static bool TryParseResetTime(string input, out TimeSpan time)
    {
        time = TimeSpan.Zero;

        // As int: 1684293600
        if (int.TryParse(input, out var seconds1))
        {
            time = TimeSpan.FromSeconds(seconds1);
            return true;
        }

        // As double: 33011.382867097855
        if (double.TryParse(input, out var seconds2))
        {
            time = TimeSpan.FromSeconds(seconds2);
            return true;
        }

        // As Duration (go style): 17ms, 1m8.754s, 5m, 1h
        var pattern = @"(?<value>\d+(?:\.\d+)?)(?<unit>ns|us|µs|ms|s|m|h)";
        var matches = Regex.Matches(input, pattern);
        if (matches.Count == 0)
            throw new FormatException($"Invalid Go‐duration: '{input}'");

        TimeSpan total = TimeSpan.Zero;
        foreach (Match m in matches)
        {
            var v = double.Parse(m.Groups["value"].Value, CultureInfo.InvariantCulture);
            switch (m.Groups["unit"].Value)
            {
                case "h":
                    total += TimeSpan.FromHours(v);
                    break;
                case "m":
                    total += TimeSpan.FromMinutes(v);
                    break;
                case "s":
                    total += TimeSpan.FromSeconds(v);
                    break;
                case "ms":
                    total += TimeSpan.FromMilliseconds(v);
                    break;
                case "us":
                case "µs":
                    total += TimeSpan.FromTicks((long)(v * 10));
                    break; // 1 µs = 10 ticks
                case "ns":
                    total += TimeSpan.FromTicks((long)(v / 100));
                    break; // 1 ns = 1/100 tick
                default:
                    return false;
            }
        }
        time = total;
        return true;
    }

    public void Dispose()
    {
        _client?.Dispose();
        _contextPool.Dispose();
    }

    internal static string GetSchemaFor(string schemaOrSampleObject)
    {
        var doc = JsonDocument.Parse(schemaOrSampleObject);
        if (doc.RootElement.TryGetProperty("type", out _) &&
            doc.RootElement.TryGetProperty("additionalProperties", out _) &&
            doc.RootElement.TryGetProperty("properties", out _) &&
            doc.RootElement.TryGetProperty("required", out _))
            return schemaOrSampleObject; // probably a schema, let's use that

        var schema = new JsonObject
        {
            ["name"] = GetAllowedUniqueName(schemaOrSampleObject), // ensures a unique name
            ["strict"] = true,
            ["schema"] = GenerateJsonObjectFromSampleObject(doc.RootElement)
        };

        return JsonSerializer.Serialize(schema, new JsonSerializerOptions { WriteIndented = true });

        JsonObject GenerateJsonObjectFromSampleObject(JsonElement element)
        {
            var jsonObj = new JsonObject();

            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    jsonObj["type"] = "object";
                    var props = new JsonObject();
                    var required = new JsonArray();
                    foreach (JsonProperty prop in element.EnumerateObject())
                    {
                        props[prop.Name] = GenerateJsonObjectFromSampleObject(prop.Value);
                        required.Add(prop.Name);
                    }
                    jsonObj["properties"] = props;
                    jsonObj["required"] = required;
                    jsonObj["additionalProperties"] = false;

                    break;

                case JsonValueKind.Array:
                    jsonObj["type"] = "array";
                    var content = element.EnumerateArray().FirstOrDefault();
                    if (content.ValueKind is not JsonValueKind.Undefined)
                    {
                        jsonObj["items"] = GenerateJsonObjectFromSampleObject(content);
                    }
                    else
                    {
                        jsonObj["items"] = new JsonObject
                        {
                            ["type"] = "null",
                        };
                    }
                    break;

                case JsonValueKind.String:
                    jsonObj["type"] = "string";
                    jsonObj["description"] = element.GetString();
                    break;

                case JsonValueKind.Number:
                    if (element.TryGetInt32(out _))
                    {
                        jsonObj["type"] = "integer";
                    }
                    else
                    {
                        jsonObj["type"] = "number";
                    }
                    break;

                case JsonValueKind.True:
                case JsonValueKind.False:
                    jsonObj["type"] = "boolean";
                    break;

                case JsonValueKind.Null:
                    jsonObj["type"] = "null";
                    break;

                default:
                    jsonObj["type"] = "none";
                    break;
            }

            return jsonObj;
        }
    }

    internal static string GetAllowedUniqueName(string schemaOrSampleObject)
    {
        var originalHash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(schemaOrSampleObject.AsSpan()));
        byte[] hashBytes = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(originalHash));
        string hashWithAllowedChars = Base64UrlEncoder.Encode(hashBytes);
        return hashWithAllowedChars;
    }

    private TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff();
    }

    internal sealed class TestingStuff
    {
        internal TestingStuff()
        {
        }

        internal Action<AsyncBlittableJsonTextWriter> ModifyPayload;
    }
}
