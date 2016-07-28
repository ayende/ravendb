﻿using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;

using Sparrow;

using Xunit;

namespace FastTests.Client.Indexing
{
    public class DebugIndexing : RavenTestBase
    {
        private class Person
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task QueriesRunning()
        {
            using (var store = await GetDocumentStore())
            {
                IndexQuery q;
                using (var session = store.OpenSession())
                {
                    var people = session.Query<Person>()
                        .Where(x => x.Name == "John")
                        .ToList(); // create index

                    q = session.Advanced.DocumentQuery<Person>()
                        .WhereEquals(x => x.Name, "John")
                        .Take(20)
                        .GetIndexQuery(isAsync: false);
                }

                var query = new IndexQueryServerSide
                {
                    Transformer = q.Transformer,
                    Start = q.Start,
                    AllowMultipleIndexEntriesForSameDocumentToResultTransformer = q.AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                    CutoffEtag = q.CutoffEtag,
                    DebugOptionGetIndexEntries = q.DebugOptionGetIndexEntries,
                    DefaultField = q.DefaultField,
                    DefaultOperator = q.DefaultOperator,
                    DisableCaching = q.DisableCaching,
                    DynamicMapReduceFields = q.DynamicMapReduceFields,
                    ExplainScores = q.ExplainScores,
                    FieldsToFetch = q.FieldsToFetch,
                    HighlightedFields = q.HighlightedFields,
                    HighlighterKeyName = q.HighlighterKeyName,
                    HighlighterPostTags = q.HighlighterPostTags,
                    HighlighterPreTags = q.HighlighterPreTags,
                    Includes = q.Includes,
                    IsDistinct = q.IsDistinct,
                    PageSize = q.PageSize,
                    Query = q.Query,
                    ShowTimings = q.ShowTimings,
                    SkipDuplicateChecking = q.SkipDuplicateChecking,
                    SortedFields = q.SortedFields,
                    WaitForNonStaleResults = q.WaitForNonStaleResults,
                    WaitForNonStaleResultsAsOfNow = q.WaitForNonStaleResultsAsOfNow,
                    WaitForNonStaleResultsTimeout = q.WaitForNonStaleResultsTimeout
                };

                var database = await Server
                    .ServerStore
                    .DatabasesLandlord
                    .TryGetOrCreateResourceStore(new StringSegment(store.DefaultDatabase, 0));

                var index = database.IndexStore.GetIndex(1);

                var now = SystemTime.UtcNow;
                index.CurrentlyRunningQueries.TryAdd(new ExecutingQueryInfo(now, query, 10, OperationCancelToken.None));

                string jsonString;
                using (var client = new HttpClient())
                {
                    jsonString = await client.GetStringAsync($"{store.Url.ForDatabase(store.DefaultDatabase)}/debug/queries/running");
                }

                var json = RavenJObject.Parse(jsonString);
                var array = json.Value<RavenJArray>(index.Name);

                Assert.Equal(1, array.Length);

                var info = array[0];

                Assert.NotNull(array[0].Value<string>(nameof(ExecutingQueryInfo.Duration)));
                Assert.Equal(10, info.Value<int>(nameof(ExecutingQueryInfo.QueryId)));
                Assert.Equal(now, info.Value<DateTime>(nameof(ExecutingQueryInfo.StartTime)));
                Assert.Null(info.Value<OperationCancelToken>(nameof(ExecutingQueryInfo.Token)));

                var output = info
                    .Value<RavenJObject>(nameof(ExecutingQueryInfo.QueryInfo))
                    .JsonDeserialization<IndexQuery>();

                Assert.True(q.Equals(output));
            }
        }
    }
}