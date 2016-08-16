using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Documents.Commands;
using Sparrow.Logging;

namespace Raven.Client.Document.SessionOperations
{
    public class LoadOperation1
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggerSetup.Instance.GetLogger<LoadOperation1>("Raven.Client");
        private GetDocumentResult _result;

        public LoadOperation1(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public RavenCommand<GetDocumentResult> ById<T>(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id), "The document id cannot be null");

            if (_session.IsDeleted(id))
                return default(T);

            object existingEntity;
            if (_session.EntitiesByKey.TryGetValue(id, out existingEntity))
                return (T)existingEntity;

            JsonDocument value;
            if (_session.IncludedDocumentsByKey.TryGetValue(id, out value))
            {
                _session.IncludedDocumentsByKey.Remove(id);
                return _session.TrackEntity<T>(value);
            }

            _session.IncrementRequestCount();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Loading {id} from {_session.StoreIdentifier}");

            var command = new GetDocumentCommand
            {
                Id = id
            };
            return command;
        }


        public RavenCommand<T> ByIds<T>(string[] ids)
        {
            if (ids.Length == 0)
                return new T[0];

            if (_session.IsDeleted(id))
                return default(T);

            object existingEntity;
            if (_session.EntitiesByKey.TryGetValue(id, out existingEntity))
                return (T)existingEntity;

            JsonDocument value;
            if (_session.IncludedDocumentsByKey.TryGetValue(id, out value))
            {
                _session.IncludedDocumentsByKey.Remove(id);
                return _session.TrackEntity<T>(value);
            }

            _session.IncrementRequestCount();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Loading {id} from {_session.StoreIdentifier}");

            /*if (_logger.IsInfoEnabled)
                            _logger.Info("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), sessionOperations.StoreIdentifier);*/

            var command = new GetDocumentCommand
            {
                Id = id
            };
            return command;
        }

        public T GetDocument<T>()
        {
            var document = _result.Results.FirstOrDefault();
            if (document == null)
            {
                _session.RegisterMissing(id);
                return default(T);
            }
            return _session.TrackEntity<T>(document);
        }

        public void SetResult(GetDocumentResult result)
        {
            _result = result;
            foreach (var include in result.Includes)
            {
                _session.TrackIncludedDocument(include);
            }

            var finalResults = ids != null ?
                ReturnResultsById<T>() :
                ReturnResults<T>();
            for (var i = 0; i < finalResults.Length; i++)
            {
                var finalResult = finalResults[i];
                if (ReferenceEquals(finalResult, null))
                    sessionOperations.RegisterMissing(ids[i]);
            }

            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
            sessionOperations.RegisterMissingIncludes(results.Where(x => x != null).Select(x => x.DataAsJson), includePaths);

            return finalResults;
        }

        public T[] GetDocuments<T>()
        {
            
        }
    }

    public class LoadOperation
    {
        private readonly static ILog log = LogManager.GetLogger(typeof(LoadOperation));

        private readonly InMemoryDocumentSessionOperations sessionOperations;
        internal Func<IDisposable> disableAllCaching { get; set; }
        private readonly string[] ids;
        private readonly KeyValuePair<string, Type>[] includes;
        bool firstRequest = true;
        JsonDocument[] results;
        JsonDocument[] includeResults;

        public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string[] ids, KeyValuePair<string, Type>[] includes = null)
        {
            this.sessionOperations = sessionOperations;
            this.disableAllCaching = disableAllCaching;
            this.ids = ids;
            this.includes = includes;
        }

        public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string id) 
            : this(sessionOperations, disableAllCaching, new [] {id}, null)
        {
        }

        public void LogOperation()
        {
            if (ids == null)
                return;
            if (log.IsDebugEnabled)
                log.Debug("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), sessionOperations.StoreIdentifier);
        }

        public IDisposable EnterLoadContext()
        {
            if (firstRequest == false) // if this is a repeated request, we mustn't use the cached result, but have to re-query the server
                return disableAllCaching();

            return null;
        }

        public bool SetResult(JsonDocument document)
        {
            firstRequest = false;
            includeResults = new JsonDocument[0];
            results = new[] {document};

            return false;
        }


        public bool SetResult(LoadResult loadResult)
        {
            firstRequest = false;
            includeResults = SerializationHelper.RavenJObjectsToJsonDocuments(loadResult.Includes).ToArray();
            results = SerializationHelper.RavenJObjectsToJsonDocuments(loadResult.Results).ToArray();

            return false;
        }

        public T[] Complete<T>()
        {
            for (var i = 0; i < includeResults.Length; i++)
            {
                var include = includeResults[i];
                sessionOperations.TrackIncludedDocument(include);
            }

            var finalResults = ids != null ?
                ReturnResultsById<T>() :
                ReturnResults<T>();
            for (var i = 0; i < finalResults.Length; i++)
            {
                var finalResult = finalResults[i];
                if (ReferenceEquals(finalResult, null))
                    sessionOperations.RegisterMissing(ids[i]);
            }

            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
            sessionOperations.RegisterMissingIncludes(results.Where(x => x != null).Select(x => x.DataAsJson), includePaths);

            return finalResults;
        }

        private T[] ReturnResults<T>()
        {
            var finalResults = new T[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != null)
                    finalResults[i] = sessionOperations.TrackEntity<T>(results[i]);
            }
            return finalResults;
        }

        private T[] ReturnResultsById<T>()
        {
            var finalResults = new T[ids.Length];
            var dic = new Dictionary<string, FinalResultPositionById>(ids.Length, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < ids.Length; i++)
            {
                if (ids[i] == null)
                    continue;

                FinalResultPositionById position;
                if (dic.TryGetValue(ids[i], out position) == false)
                {
                    dic[ids[i]] = new FinalResultPositionById
                    {
                        SingleReturn = i
                    };
                }
                else
                {
                    if (position.SingleReturn != null)
                    {
                        position.MultipleReturns = new List<int>(2)
                        {
                            position.SingleReturn.Value
                        };

                        position.SingleReturn = null;
                    }

                    position.MultipleReturns.Add(i);
                }  
            }

            foreach (var jsonDocument in results)
            {
                if (jsonDocument == null)
                    continue;

                var id = jsonDocument.Metadata.Value<string>("@id");

                if (id == null)
                    continue;

                FinalResultPositionById position;

                if (dic.TryGetValue(id, out position))
                {
                    if (position.SingleReturn != null)
                    {
                        finalResults[position.SingleReturn.Value] = sessionOperations.TrackEntity<T>(jsonDocument);
                    }
                    else if (position.MultipleReturns != null)
                    {
                        T trackedEntity = sessionOperations.TrackEntity<T>(jsonDocument);

                        foreach (var pos in position.MultipleReturns)
                        {
                            finalResults[pos] = trackedEntity;
                        }
                    }
                }
            }

            return finalResults;
        }

        private class FinalResultPositionById
        {
            public int? SingleReturn;

            public List<int> MultipleReturns;
        }
    }
}
