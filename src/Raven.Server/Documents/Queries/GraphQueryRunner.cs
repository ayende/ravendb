﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Client;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Diagnostics;

namespace Raven.Server.Documents.Queries
{
    public class GraphQueryRunner : AbstractQueryRunner
    {
        public GraphQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        // this code is first draft mode, meant to start working. It is known that 
        // there are LOT of allocations here that we'll need to get under control
        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var q = query.Metadata.Query;

            var ir = new IntermediateResults();

            foreach (var documentQuery in q.GraphQuery.WithDocumentQueries)
            {
                var queryMetadata = new QueryMetadata(documentQuery.Value, query.QueryParameters, 0);
                var results = await Database.QueryRunner.ExecuteQuery(new IndexQueryServerSide(queryMetadata),
                    documentsContext, existingResultEtag, token);

                foreach (var result in results.Results)
                {
                    var match = new Match();
                    match.Set(documentQuery.Key, result);
                    match.Populate(ref ir);
                }
            }

            var matchResults = ExecutePatternMatch(documentsContext, q, ir) ?? new List<Match>();

            //TODO: handle order by, load, select clauses

            var final = new DocumentQueryResult();
            foreach (var match in matchResults)
            {
                var result = new DynamicJsonValue();
                match.Populate(result);

                final.AddResult(new Document
                {
                    Data = documentsContext.ReadObject(result, "graph/result"),
                });

            }
            final.TotalResults = final.Results.Count;
            return final;
        }

        private List<Match> ExecutePatternMatch(DocumentsOperationContext documentsContext, Query q, IntermediateResults ir)
        {
            var results = new DocumentQueryResult();
            var visitor = new GraphExecuteVisitor(ir, q.GraphQuery, documentsContext);
            visitor.VisitExpression(q.GraphQuery.MatchClause);
            return visitor.Output;

        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamDocumentQueryResultWriter writer, OperationCancelToken token)
        {
            throw new NotImplementedException("Streaming graph queries is not supported at this time");
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot delete based on graph query");
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Graph queries do not expose index queries");
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, Patch.PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException("You cannot patch based on graph query");
        }


        private struct Match // using struct because we have a single field 
        {
            private Dictionary<string, Document> _inner;

            public object Key { get => _inner; }

            public IEnumerable<string> Aliases => _inner.Keys;

            public Document Get(string alias)
            {
                Document result = null;
                _inner?.TryGetValue(alias, out result);
                return result;
            }

            public void Set(StringSegment alias, Document val)
            {
                if (_inner == null)
                    _inner = new Dictionary<string, Document>();

                _inner.Add(alias, val);
            }

            public void Populate(DynamicJsonValue j)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    item.Value.EnsureMetadata();
                    j[item.Key] = item.Value.Data;
                }
            }

            public void Populate(ref IntermediateResults i)
            {
                if (_inner == null)
                    return;

                foreach (var item in _inner)
                {
                    i.Add(item.Key, this, item.Value);
                }
            }
        }

        private struct IntermediateResults// using struct because we have a single field 
        {
            private Dictionary<string, Dictionary<string, Match>> _matchesByAlias;
            private Dictionary<string, Dictionary<string, Match>> MatchesByAlias
            {
                get =>  _matchesByAlias ??( _matchesByAlias = new Dictionary<string, Dictionary<string, Match>>());
            }

            public void Add(Match match)
            {
                foreach (var alias in match.Aliases)
                {
                    Add(alias, match, match.Get(alias));
                }
            }

            public void Add(string alias, Match match, Document instance)
            {
                if (MatchesByAlias.TryGetValue(alias, out var aliasDic) == false)
                    MatchesByAlias[alias] = aliasDic = new Dictionary<string, Match>();

                //TODO: need to handle map/reduce results?
                aliasDic[instance.Id] = match;
            }

            public bool TryGetByAlias(string alias, out Dictionary<string,Match> value)
            {
                return MatchesByAlias.TryGetValue(alias, out value);
            }
        }

        private class GraphExecuteVisitor : QueryVisitor
        {
            private readonly IntermediateResults _source;
            private readonly GraphQuery _gq;
            private readonly DocumentsOperationContext _ctx;
            public List<Match> Output;
            public GraphExecuteVisitor(IntermediateResults source, GraphQuery gq, DocumentsOperationContext documentsContext)
            {
                _source = source;
                _gq = gq;
                _ctx = documentsContext;
            }


            public override void VisitPatternMatchElementExpression(PatternMatchElementExpression ee)
            {
                Debug.Assert(ee.Path[0].EdgeType == EdgeType.Right);
                if (_source.TryGetByAlias(ee.Path[0].Alias, out var nodeResults) == false || 
                    nodeResults.Count == 0)
                    return; // if root is empty, the entire thing is empty

                var currentResults = new List<Match>();
                foreach (var item in nodeResults)
                {
                    var match = new Match();
                    match.Set(ee.Path[0].Alias, item.Value.Get(ee.Path[0].Alias));
                    currentResults.Add(match);
                }

                // TODO: for now, we require node->edge->node->edge syntax
                for (int pathIndex = 1; pathIndex < ee.Path.Length-1; pathIndex+=2)
                {
                    Debug.Assert(ee.Path[pathIndex].IsEdge);

                    var prevNodeAlias = ee.Path[pathIndex - 1].Alias;
                    var nextNodeAlias = ee.Path[pathIndex + 1].Alias;


                    var edge = _gq.WithEdgePredicates[ee.Path[pathIndex].Alias].EdgeType.Value;

                    _source.TryGetByAlias(nextNodeAlias, out var edgeResults);

                    for (int resultIndex = 0; resultIndex < currentResults.Count; resultIndex++)
                    {
                        var item = currentResults[resultIndex];

                        var prev = item.Get(prevNodeAlias);
                        if (TryGetRelatedMatch(edge, nextNodeAlias, edgeResults, prev, out var relatedMatch) == false)
                        {
                            currentResults.RemoveAt(resultIndex);
                            resultIndex--;
                            continue;
                        }

                        var realted = relatedMatch.Get(nextNodeAlias);
                        item.Set(nextNodeAlias, realted);
                    }
                }

                Output = currentResults;
            }

            private bool TryGetRelatedMatch(string edge, string alias, Dictionary<string, Match> edgeResults, Document prev, out Match relatedMatch)
            {
                relatedMatch = default;
                if (prev.Data.TryGet(edge, out string nextId) == false || nextId == null)
                    return false;

                if (edgeResults?.TryGetValue(nextId, out relatedMatch) == true)
                    return true;

                var doc = _ctx.DocumentDatabase.DocumentsStorage.Get(_ctx, nextId, false);
                if (doc == null)
                    return false;

                relatedMatch = new Match();
                relatedMatch.Set(alias, doc);
                return true;
            }
        }
    }
}
