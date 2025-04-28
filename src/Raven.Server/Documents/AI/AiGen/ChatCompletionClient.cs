using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI.AiGen;

public class ChatCompletionClient(Uri baseUri, string model, string apiKey, string structuredOutputSchema) : IDisposable
{
    private readonly JsonObject _schema = new()
    {
        ["type"] = "json_schema",
        ["json_schema"] = JsonNode.Parse(structuredOutputSchema)
    };

    private readonly HttpClient _client = new()
    {
        BaseAddress = baseUri,
        DefaultRequestHeaders =
        {
            Authorization = new AuthenticationHeaderValue("Bearer", apiKey),
            Accept = { new MediaTypeWithQualityHeaderValue("application/json") }
        }
    };

    public async Task<(string Result, string Usage)> CompleteAsync(string prompt, string context)
    {
        var req = new JsonObject
        {
            ["model"] = model,
            ["messages"] = new JsonArray
            {
                new JsonObject
                {
                    ["role"] = "system",
                    ["content"] = prompt
                },
                new JsonObject
                {
                    ["role"] = "user",
                    ["content"] = context
                },
            },
            ["response_format"] = _schema.DeepClone()
        };

        Console.WriteLine(JsonSerializer.Serialize(req, new JsonSerializerOptions { WriteIndented = true }));

        using var reply = await _client.PostAsync("/v1/chat/completions", JsonContent.Create(req)).ConfigureAwait(false);
        using var stream = await reply.Content.ReadAsStreamAsync();
        var response = await JsonDocument.ParseAsync(stream);

        Console.WriteLine(JsonSerializer.Serialize(response.RootElement, new JsonSerializerOptions { WriteIndented = true }));

        var msg = response.RootElement.GetProperty("choices")[0].GetProperty("message").GetProperty("content").GetString()!;
        var usage = response.RootElement.GetProperty("usage");
        return (msg, usage.ToString());
    }


    public void Dispose()
    {
        _client?.Dispose();
    }

    public static string GetSchemaFor(string schemaOrSampleObject)
    {
        var doc = JsonDocument.Parse(schemaOrSampleObject);
        if (doc.RootElement.TryGetProperty("type", out _) &&
            doc.RootElement.TryGetProperty("additionalProperties", out _) &&
            doc.RootElement.TryGetProperty("properties", out _) &&
            doc.RootElement.TryGetProperty("required", out _))
            return schemaOrSampleObject; // probably a schema, let's use that

        var schema = new JsonObject
        {
            ["name"] = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(schemaOrSampleObject.AsSpan())), // ensures a unique name
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

}
