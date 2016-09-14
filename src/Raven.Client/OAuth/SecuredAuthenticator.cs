using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Client.Extensions;
using Raven.Client.Platform;
using Raven.Json.Linq;

namespace Raven.Client.OAuth
{
    public class SecuredAuthenticator
    {
        public string CurrentToken { get; set; }
        private static readonly ILog Logger = LogManager.GetLogger(typeof(SecuredAuthenticator));
        private bool disposed;
        private readonly CancellationTokenSource disposedToken = new CancellationTokenSource();
        private Uri uri;
        private string serverRSAExponent;
        private string serverRSAModulus;
        private string challenge;
        private const int MaxWebSocketRecvSize = 4096;

        protected void SetAuthorization(HttpClient e)
        {
            if (string.IsNullOrEmpty(CurrentToken))
                return;

            try
            {
                e.DefaultRequestHeaders.Add("Raven-Authorization", CurrentToken);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("Could not set the Authorization to the value 'Bearer {0}'", CurrentToken), ex);
            }
        }

        public void ConfigureRequest(object sender, WebRequestEventArgs e)
        {
            if (CurrentToken != null)
            {
                SetAuthorization(e.Client);
            }
        }

        public async Task<Action<HttpClient>> DoOAuthRequestAsync(string url, string apiKey)
        {
            ThrowIfBadUrlOrApiKey(url, apiKey);
            uri = new Uri(url.ToWebSocketPath());

            using (var webSocket = new RavenClientWebSocket())
            {
                try
                {
                    Logger.Info("Trying to connect using WebSocket to {0} for authentication", uri);
                    try
                    {
                        await webSocket.ConnectAsync(uri, CancellationToken.None);
                    }
                    catch (WebSocketException webSocketException)
                    {
                        throw new InvalidOperationException($"Cannot connect using WebSocket to {uri} for authentication", webSocketException);
                    }
                    var recvRavenJObject = await Recieve(webSocket);
                    var challenge = ComputeChallenge(recvRavenJObject, apiKey);
                    await Send(webSocket, "ChallengeResponse", challenge);
                    recvRavenJObject = await Recieve(webSocket);

                    try
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Close from client", disposedToken.Token);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                    SetCurrentTokenFromReply(recvRavenJObject);
                    return SetAuthorization;
                }
                catch (Exception ex)
                {
                    Logger.DebugException($"Failed to DoOAuthRequest to {url} with {apiKey}", ex);
                    throw;
                }
            }
        }

        private void ThrowIfBadUrlOrApiKey(string url, string apiKey)
        {
            if (string.IsNullOrEmpty(url))
                throw new InvalidOperationException("DoAuthRequest provided with invalid url");

            if (apiKey == null)
                throw new InvalidApiKeyException("DoAuthRequest provided with null apiKey");
        }

        private async Task Send(RavenClientWebSocket webSocket, string command, string commandParameter)
        {
            Logger.Info($"Sending WebSocket Authentication Command {command} - {commandParameter} to {uri}");

            var ravenJObject = new RavenJObject
            {
                [command] = commandParameter,
            };

            var stream = new MemoryStream();
            ravenJObject.WriteTo(stream);
            ArraySegment<byte> bytes;
            stream.TryGetBuffer(out bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None).ConfigureAwait(false);
        }

        private string ComputeChallenge(RavenJObject challengeJObject, string apiKey)

        {
            if (challengeJObject == null)
                throw new InvalidDataException("Got null json object in ComputeChallenge");

            serverRSAExponent = challengeJObject.Value<string>(OAuthHelper.Keys.RSAExponent);
            serverRSAModulus = challengeJObject.Value<string>(OAuthHelper.Keys.RSAModulus);
            challenge = challengeJObject.Value<string>(OAuthHelper.Keys.Challenge);

            if (string.IsNullOrEmpty(serverRSAExponent) || string.IsNullOrEmpty(serverRSAModulus) || string.IsNullOrEmpty(challenge))
                throw new InvalidOperationException("Invalid authentication information");

            var apiKeyParts = ExtractApiKeyAndSecret(apiKey);
            var apiKeyName = apiKeyParts[0].Trim();
            var apiSecret = apiKeyParts[1].Trim();

            var data = OAuthHelper.DictionaryToString(new Dictionary<string, string>
                {
                    {OAuthHelper.Keys.RSAExponent, serverRSAExponent},
                    {OAuthHelper.Keys.RSAModulus, serverRSAModulus},
                    {
                        OAuthHelper.Keys.EncryptedData,
                        OAuthHelper.EncryptAsymmetric(OAuthHelper.ParseBytes(serverRSAExponent),
                        OAuthHelper.ParseBytes(serverRSAModulus),
                        OAuthHelper.DictionaryToString(new Dictionary<string, string>
                        {
                            {OAuthHelper.Keys.APIKeyName, apiKeyName},
                            {OAuthHelper.Keys.Challenge, challenge},
                            {OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret))}
                        }))
                    }
                });

            return data;
        }

        private string[] ExtractApiKeyAndSecret(string apiKey)
        {
            var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);

            if (apiKeyParts.Length > 2)
            {
                apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
            }

            if (apiKeyParts.Length < 2)
                throw new InvalidApiKeyException("Invalid Api-Key. Contains less then two parts separated with slash");

            return apiKeyParts;
        }

        private async Task<RavenJObject> Recieve(RavenClientWebSocket webSocket)
        {
            try
            {
                if (webSocket.State != WebSocketState.Open)
                    throw new InvalidOperationException(
                        $"Trying to 'ReceiveAsync' WebSocket while not in Open state. State is {webSocket.State}");

                using (var ms = new MemoryStream())
                {
                    ArraySegment<byte> bytes;
                    ms.SetLength(MaxWebSocketRecvSize);
                    ms.TryGetBuffer(out bytes);
                    var arraySegment = new ArraySegment<byte>(bytes.Array, 0, MaxWebSocketRecvSize);
                    var result = await webSocket.ReceiveAsync(arraySegment, disposedToken.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("Client got close message from server and is closing connection");

                        // actual socket close from dispose
                        return null;
                    }

                    if (result.EndOfMessage == false)
                    {
                        var err = $"In Recieve got response longer then {MaxWebSocketRecvSize}";
                        var ex = new InvalidOperationException(err);
                        Logger.DebugException(err, ex);
                        throw ex;
                    }

                    using (var reader = new StreamReader(ms, Encoding.UTF8, true, MaxWebSocketRecvSize, true))
                    using (var jsonReader = new RavenJsonTextReader(reader)
                    {
                        SupportMultipleContent = true
                    })
                    {
                        if (jsonReader.Read() == false)
                            throw new InvalidDataException("Couldn't read recieved websocket json message");
                        return RavenJObject.Load(jsonReader);
                    }
                }
            }
            catch (WebSocketException ex)
            {
                Logger.DebugException("Failed to receive a message, client was probably disconnected", ex);
                throw;
            }
        }

        private void SetCurrentTokenFromReply(RavenJObject recvRavenJObject)
        {
            var errorMsg = recvRavenJObject.Value<string>("Error");

            if (errorMsg != null)
            {
                var exceptionType = recvRavenJObject.Value<string>("ExceptionType");
                if (exceptionType == null || exceptionType.Equals("InvalidOperationException"))
                    throw new InvalidOperationException("Server returned error: " + errorMsg);

                if (exceptionType.Equals("InvalidApiKeyException"))
                    throw new InvalidApiKeyException(errorMsg);
            }

            var currentOauthToken = recvRavenJObject.Value<string>("CurrentToken");

            if (currentOauthToken == null)
                throw new InvalidOperationException("Missing 'CurrentToken' in response message");

            CurrentToken = currentOauthToken;
        }
    }
}
