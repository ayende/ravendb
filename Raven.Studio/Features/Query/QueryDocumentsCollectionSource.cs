﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Query
{
    public class QueryDocumentsCollectionSource : DocumentsVirtualCollectionSourceBase
    {
        private IndexQuery _templateQuery;
        private string _indexName;
        private readonly object _lockObject = new object();

        public event EventHandler<QueryErrorEventArgs> QueryError;
        public event EventHandler<QueryStatisticsUpdatedEventArgs> QueryStatisticsUpdated;

        public IndexQuery TemplateQuery
        {
            get
            {
                lock (_lockObject)
                {
                    return _templateQuery;
                }
            }
        }

        public void UpdateQuery(string indexName, IndexQuery templateQuery)
        {
            lock (_lockObject)
            {
                _templateQuery = templateQuery;
                _indexName = indexName;
            }

            Refresh(RefreshMode.ClearStaleData);
        }

        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
	        return GetQueryResults(start, pageSize)
		        .ContinueWith(task =>
		        {
			        var documents =
				        SerializationHelper.RavenJObjectsToJsonDocuments(task.Result.Results)
				                           .Select(x => new ViewableDocument(x))
				                           .ToArray();

			        SetCount(Math.Max(task.Result.TotalResults - task.Result.SkippedResults, documents.Length));

			        return (IList<ViewableDocument>) documents;
		        });
        }

        private Task<QueryResult> GetQueryResults(int start, int pageSize)
        {
            IndexQuery templateQuery;
            string indexName;

            lock (_lockObject)
            {
                templateQuery = TemplateQuery;
                indexName = _indexName;
            }

            if (templateQuery == null || string.IsNullOrEmpty(indexName))
                return TaskEx.FromResult(new QueryResult());

            var query = templateQuery.Clone();
            query.Start = start;
            query.PageSize = pageSize;

			var queryStartTime = SystemTime.UtcNow.Ticks;
            var queryEndtime = DateTime.MinValue.Ticks;

	        return ApplicationModel.DatabaseCommands
	                               .QueryAsync(indexName,
	                                           query,
	                                           new string[] {}, MetadataOnly)
	                               .ContinueWith(task =>
	                               {
		                               queryEndtime = SystemTime.UtcNow.Ticks;

		                               var queryTime = new TimeSpan(queryEndtime - queryStartTime);

		                               RavenQueryStatistics statistics;
		                               if (!task.IsFaulted)
		                               {
			                               statistics = new RavenQueryStatistics
			                               {
				                               IndexEtag = task.Result.IndexEtag,
				                               IndexName = task.Result.IndexName,
				                               IndexTimestamp =
					                               task.Result.IndexTimestamp,
				                               IsStale = task.Result.IsStale,
				                               SkippedResults =
					                               task.Result.SkippedResults,
				                               Timestamp = SystemTime.UtcNow,
				                               TotalResults = task.Result.TotalResults
			                               };
		                               }
		                               else
		                               {
			                               statistics = new RavenQueryStatistics {Timestamp = SystemTime.UtcNow};
		                               }

		                               OnQueryStatisticsUpdated(new QueryStatisticsUpdatedEventArgs()
		                               {
			                               QueryTime = queryTime,
			                               Statistics = statistics
		                               });

		                               if (task.IsFaulted)
		                               {
			                               OnQueryError(new QueryErrorEventArgs {Exception = task.Exception});
		                               }

		                               return task.Result;
	                               }, TaskContinuationOptions.ExecuteSynchronously);
        }

        protected void OnQueryStatisticsUpdated(QueryStatisticsUpdatedEventArgs e)
        {
            var handler = QueryStatisticsUpdated;
            if (handler != null) handler(this, e);
        }

        protected void OnQueryError(QueryErrorEventArgs e)
        {
            var handler = QueryError;
            if (handler != null)
            {
	            if(e.Exception.GetBaseException().Message.Contains("NotFound"))
					handler(this, new QueryErrorEventArgs
					{
						Exception = new ArgumentException("Server Not Found", e.Exception)
					});
	            else
					handler(this, e);
            }
        }

        public async override Task<IAsyncEnumerator<JsonDocument>> StreamAsync(Reference<long> totalResults)
        {
            IndexQuery templateQuery;
            string indexName;

            lock (_lockObject)
            {
                templateQuery = TemplateQuery;
                indexName = _indexName;
            }

            var query = templateQuery.Clone();
            query.Start = 0;
            query.PageSize = int.MaxValue;

            var reference = new Reference<QueryHeaderInformation>();

            var enumerator = await ApplicationModel.DatabaseCommands.StreamQueryAsync(indexName, query,reference);

            totalResults.Value = reference.Value.TotalResults;

            return new ConvertingEnumerator<JsonDocument, RavenJObject>(enumerator, doc => doc.ToJsonDocument());
        }
    }

    public class QueryStatisticsUpdatedEventArgs : EventArgs
    {
        public RavenQueryStatistics Statistics { get; set; }

        public TimeSpan QueryTime { get; set; }
    }

    public class QueryErrorEventArgs : EventArgs
    {
        public Exception Exception { get; set; }
    }
}