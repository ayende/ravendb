using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;

using System;
using System.Collections.Generic;

using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.OAuth
{
    public class SecuredAuthenticator : AbstractAuthenticator, IDisposable
    {

        private readonly bool autoRefreshToken;
        private Timer autoRefreshTimer; 
        private readonly object locker = new object();

        private readonly TimeSpan defaultRefreshTimeInMilis = TimeSpan.FromMinutes(29);

        public SecuredAuthenticator(bool autoRefreshToken)
        {
            this.autoRefreshToken = autoRefreshToken;
        }

        public void Dispose()
        {
            autoRefreshTimer?.Dispose();
            autoRefreshTimer = null;
        }

        public override void ConfigureRequest(object sender, WebRequestEventArgs e)
        {
            if (CurrentOauthToken != null)
            {
                base.ConfigureRequest(sender, e);
                return;
            }

            if (e.Credentials?.ApiKey != null)
            {
                e.Client?.DefaultRequestHeaders.Add("Has-Api-Key", "true");
            }
        }

        public async Task<Action<HttpClient>> DoOAuthRequestAsync(string baseUrl, string oauthSource, string apiKey)
        {
            if (oauthSource == null)
                throw new ArgumentNullException("oauthSource");

            string serverRSAExponent = null;
            string serverRSAModulus = null;
            string challenge = null;

            // Note that at two tries will be needed in the normal case.
            // The first try will get back a challenge,
            // the second try will try authentication. If something goes wrong server-side though
            // (e.g. the server was just rebooted or the challenge timed out for some reason), we
            // might get a new challenge back, so we try a third time just in case.
            int tries = 0;
            while (true)
            {
                tries++;
                var handler = new WinHttpHandler();

                using (var httpClient = new HttpClient(handler))
                {
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("grant_type", "client_credentials");
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "UTF-8" });

                    string data = null;
                    if (!string.IsNullOrEmpty(serverRSAExponent) && !string.IsNullOrEmpty(serverRSAModulus) && !string.IsNullOrEmpty(challenge))
                    {
                        var exponent = OAuthHelper.ParseBytes(serverRSAExponent);
                        var modulus = OAuthHelper.ParseBytes(serverRSAModulus);

                        var apiKeyParts = apiKey.Split(new[] { '/' }, StringSplitOptions.None);
                        if (apiKeyParts.Length > 2)
                        {
                            apiKeyParts[1] = string.Join("/", apiKeyParts.Skip(1));
                        }
                        if (apiKeyParts.Length < 2) throw new InvalidOperationException("Invalid API key");

                        var apiKeyName = apiKeyParts[0].Trim();
                        var apiSecret = apiKeyParts[1].Trim();

                        data = OAuthHelper.DictionaryToString(new Dictionary<string, string> { { OAuthHelper.Keys.RSAExponent, serverRSAExponent }, { OAuthHelper.Keys.RSAModulus, serverRSAModulus }, { OAuthHelper.Keys.EncryptedData, OAuthHelper.EncryptAsymmetric(exponent, modulus, OAuthHelper.DictionaryToString(new Dictionary<string, string> { { OAuthHelper.Keys.APIKeyName, apiKeyName }, { OAuthHelper.Keys.Challenge, challenge }, { OAuthHelper.Keys.Response, OAuthHelper.Hash(string.Format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret)) } })) } });
                    }

                    var requestUri = oauthSource;

                    var response = await httpClient.PostAsync(requestUri, data != null ? (HttpContent)new CompressedStringContent(data, true) : new StringContent("")).AddUrlIfFaulting(new Uri(requestUri)).ConvertSecurityExceptionToServerNotFound().ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        // We've already tried three times and failed
                        if (tries >= 3) throw ErrorResponseException.FromResponseMessage(response);

                        if (response.StatusCode != HttpStatusCode.PreconditionFailed) throw ErrorResponseException.FromResponseMessage(response);

                        var header = response.Headers.GetFirstValue("WWW-Authenticate");
                        if (header == null || header.StartsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey) == false) throw new ErrorResponseException(response, "Got invalid WWW-Authenticate value");

                        var challengeDictionary = OAuthHelper.ParseDictionary(header.Substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.Length).Trim());
                        serverRSAExponent = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAExponent);
                        serverRSAModulus = challengeDictionary.GetOrDefault(OAuthHelper.Keys.RSAModulus);
                        challenge = challengeDictionary.GetOrDefault(OAuthHelper.Keys.Challenge);

                        if (string.IsNullOrEmpty(serverRSAExponent) || string.IsNullOrEmpty(serverRSAModulus) || string.IsNullOrEmpty(challenge))
                        {
                            throw new InvalidOperationException("Invalid response from server, could not parse raven authentication information: " + header);
                        }

                        continue;
                    }

                    using (var stream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
                    using (var reader = new StreamReader(stream))
                    {
                        var currentOauthToken = reader.ReadToEnd();
                        CurrentOauthToken = currentOauthToken;
                        CurrentOauthTokenWithBearer = "Bearer " + currentOauthToken;

                        ScheduleTokenRefresh(oauthSource, apiKey);

                        return (Action<HttpClient>)(SetAuthorization);
                    }
                }
            }
        }

        private void ScheduleTokenRefresh(string oauthSource, string apiKey)
        {
            if (!autoRefreshToken)
            {
                return;
            }

            lock (locker)
            {
                if (autoRefreshTimer != null)
                {
                    autoRefreshTimer.Change(defaultRefreshTimeInMilis, Timeout.InfiniteTimeSpan);
                }
                else
                {
                    autoRefreshTimer = new Timer(_ => DoOAuthRequestAsync(null, oauthSource, apiKey), null, defaultRefreshTimeInMilis, Timeout.InfiniteTimeSpan);
                }
            }
        }
    }
}
