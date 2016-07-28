﻿using System.Net.Http;
using Raven.Client.Documents.Data;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class PutDocumentCommand : RavenCommand<PutResult>
    {
        public string Id;
        public long? Etag;
        public BlittableJsonReaderObject Document;

        public override HttpRequestMessage CreateRequest()
        {
            EnsureIsNotNullOrEmpty(Id, nameof(Id));

            var request = new HttpRequestMessage(HttpMethod.Put, $"{Url}/databases/{Database}/docs?id={UrlEncode(Id)}")
            {
                Content = new BlittableJsonContent(Document, Context),
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserialization.PutResult(response);
        }
    }
}