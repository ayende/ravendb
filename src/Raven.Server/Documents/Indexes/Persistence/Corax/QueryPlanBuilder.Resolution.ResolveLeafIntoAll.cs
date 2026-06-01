using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Spatial4n.Shapes;
using Voron;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    private static void ResolveLeafIntoAll(ResolutionContext walkerCtx,
        ClauseExecution clauseExec, QueryExecution root, bool planHasBoost,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (clauseExec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup:
                foreach (var it in clauseExec.SubExecutions)
                {
                    ResolveLeafIntoAll(walkerCtx, it, root, planHasBoost, matches, leaves);
                }
                break;
            case ClauseType.AllIn or ClauseType.In:
            {
                MatchDispatch dispatch = planHasBoost ? MatchDispatch.QueryMatch : MatchDispatch.PostingList;
                for (int i = 0; i < clauseExec.InTermCount; i++)
                {
                    AddInTermSlot(dispatch,  i);
                }
                AddNullTermSlot(dispatch); // Null-term slot is always allocated; dispatch decides how it resolves.
                break;
            }
            default:
            {
                AddDefaultSlot(planHasBoost ? MatchDispatch.QueryMatch : GetDispatch(clauseExec));
                break;
            }
        }
        
        void AddInTermSlot(MatchDispatch dispatch, int termIndex)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                    matches.Add(ResolveInTerm(clauseExec, termIndex, root, walkerCtx));
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    return;
                default: 
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TermPosting,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue.WithTermOffset(termIndex),
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
            }
        }
        
        void AddNullTermSlot(MatchDispatch dispatch)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                    var indexSearcher = walkerCtx.IndexSearcher;
                    FieldMetadata nullMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx);
                    matches.Add(clauseExec.HasNullTerm
                        ? indexSearcher.TermQuery(nullMeta, null)
                        : TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator));
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    return;
                default: 
                    matches.Add(null);
                    LeafResolveInfo ret = clauseExec.HasNullTerm is false
                        ? new LeafResolveInfo
                        {
                            Kind = clauseExec.ClauseType == ClauseType.AllIn ? LeafResolveKind.AllPosting : LeafResolveKind.EmptyPosting
                        }
                        : new LeafResolveInfo
                        {
                            Kind = LeafResolveKind.NullPosting,
                            ClauseType = clauseExec.ClauseType,
                            FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                        };

                    leaves.Add(ret);
                    break;
            }
        }
        
        void AddDefaultSlot(MatchDispatch dispatch)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                {
                    IQueryMatch match = ResolveClause(clauseExec, root, walkerCtx);
                    if (clauseExec.BoostFactor is not 0)
                        match = walkerCtx.IndexSearcher.Boost(match, clauseExec.BoostFactor);
                    matches.Add(match);
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    break;
                }
                case MatchDispatch.PostingList:
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TermPosting,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue,
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
                case MatchDispatch.TreeScan:
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TreeScan,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue,
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
                default:
                    throw new ArgumentOutOfRangeException(dispatch.ToString()); 
            }
        }
    }
}
