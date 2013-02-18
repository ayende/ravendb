using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Storage.RavenLevelDB
{
    public class DocumentStorageActions : IDocumentStorageActions
    {
        private readonly LevelDBAccessor accessor;

        public DocumentStorageActions(LevelDBAccessor accessor)
        {
            this.accessor = accessor;
        }

        public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take, long? maxSize = null, Etag untilEtag = null)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
        {
            throw new NotImplementedException();
        }

        public long GetDocumentsCount()
        {
            throw new NotImplementedException();
        }

        public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
        {
            throw new NotImplementedException();
        }

        public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
        {
            throw new NotImplementedException();
        }

        public bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag)
        {
            throw new NotImplementedException();
        }

        public AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata)
        {
            throw new NotImplementedException();
        }

        public AddDocumentResult PutDocumentMetadata(string key, RavenJObject metadata)
        {
            throw new NotImplementedException();
        }

        public void IncrementDocumentCount(int value)
        {
            throw new NotImplementedException();
        }

        public AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool checkForUpdates)
        {
            throw new NotImplementedException();
        }

        public void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag)
        {
            throw new NotImplementedException();
        }

        public Etag GetBestNextDocumentEtag(Etag etag)
        {
            throw new NotImplementedException();
        }
    }
}
