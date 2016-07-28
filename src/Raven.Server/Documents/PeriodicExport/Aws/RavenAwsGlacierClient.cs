// -----------------------------------------------------------------------
//  <copyright file="RavenAwsGlacierClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Http;

namespace Raven.Server.Documents.PeriodicExport.Aws
{
    public class RavenAwsGlacierClient : RavenAwsClient
    {
        public RavenAwsGlacierClient(string awsAccessKey, string awsSecretKey, string awsRegionName)
            : base(awsAccessKey, awsSecretKey, awsRegionName)
        {
        }

        public async Task<string> UploadArchive(string glacierVaultName, Stream stream, string archiveDescription, int timeoutInSeconds)
        {
            await ValidateAwsRegion();
            var url = $"{GetUrl(null)}/-/vaults/{glacierVaultName}/archives";

            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);
            var payloadTreeHash = RavenAwsHelper.CalculatePayloadTreeHash(stream);

            var content = new StreamContent(stream)
            {
                Headers =
                {
                    {"x-amz-glacier-version", "2012-06-01"},
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash},
                    {"x-amz-sha256-tree-hash", payloadTreeHash},
                    {"x-amz-archive-description", archiveDescription}
                }
            };

            var headers = ConvertToHeaders(glacierVaultName, content.Headers);

            var client = GetClient(TimeSpan.FromSeconds(timeoutInSeconds));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.PostAsync(url, content);
            if (response.IsSuccessStatusCode)
                return ReadArchiveId(response);

            throw ErrorResponseException.FromResponseMessage(response);
        }

        public override string ServiceName => "glacier";

        public override string GetHost(string glacierVaultName)
        {
            return $"glacier.{AwsRegion}.amazonaws.com";
        }

        private static string ReadArchiveId(HttpResponseMessage response)
        {
            return response.Headers
                .GetValues("x-amz-archive-id")
                .First();
        }
    }
}