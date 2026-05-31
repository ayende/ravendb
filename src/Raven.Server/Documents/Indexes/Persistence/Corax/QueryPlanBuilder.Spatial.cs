using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Spatial4n.Shapes;
using Sparrow.Json;
using RavenConstants = Raven.Client.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using SpatialRelation = Corax.Utils.Spatial.SpatialRelation;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    private static void ResolveSpatialFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = exec.Clause.Bindings;
        var sp = new SpatialParams();

        // [0] = distanceErrorPct
        if (bindings.Length > 0 && bindings[BindingIndex.SpatialDistErrPct] != null)
        {
            var (depVal, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialDistErrPct], queryParameters, builderParameters: null);
            sp.DistanceErrorPct = depVal != null ? Convert.ToDouble(depVal) : -1;
        }

        switch (bindings.Length) // Shape type determined by the number of bindings
        {
            case >= BindingIndex.SpatialCircleBindingCount - 1: // circle: at least distErrPct + radius + lat + lng
            {
                sp.ShapeType = SpatialShapeType.Circle;
                var (r, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialRadius], queryParameters, builderParameters: null);
                var (lat, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLatitude], queryParameters, builderParameters: null);
                var (lng, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLongitude], queryParameters, builderParameters: null);
                sp.CircleRadius = Convert.ToDouble(r);
                sp.CircleLatitude = Convert.ToDouble(lat);
                sp.CircleLongitude = Convert.ToDouble(lng);
                if (bindings.Length > BindingIndex.SpatialUnits && bindings[BindingIndex.SpatialUnits] != null)
                {
                    var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialUnits], queryParameters, builderParameters: null);
                    if (u != null && Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su))
                        sp.Units = ToCoraxUnits(su);
                }

                break;
            }
            default: // WKT: distErrPct, wkt, [units]
            {
                sp.ShapeType = SpatialShapeType.Wkt;
                if (bindings.Length > BindingIndex.SpatialWkt && bindings[BindingIndex.SpatialWkt] != null)
                {
                    var (wkt, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWkt], queryParameters, builderParameters: null);
                    sp.Wkt = wkt?.ToString();
                    if (bindings.Length > BindingIndex.SpatialWktUnits && bindings[BindingIndex.SpatialWktUnits] != null)
                    {
                        var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWktUnits], queryParameters, builderParameters: null);
                        if (u != null && Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su))
                            sp.Units = ToCoraxUnits(su);
                    }
                }

                break;
            }
        }

        exec.Spatial = sp;

        global::Corax.Utils.Spatial.SpatialUnits ToCoraxUnits(object su) =>
            (SpatialUnits)su == SpatialUnits.Kilometers
                ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                : global::Corax.Utils.Spatial.SpatialUnits.Miles;
    }

    private static void AttachSpatialAndVectorClauses(QueryExecution exec, PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ValueWriter writer)
    {
        if (template.SpatialClauses == null && template.VectorClauses == null)
            return;

        ClauseInfo[] spatialArr = null;
        ClauseInfo[] vectorArr = null;
        ClauseExecution[] spatialExecs = null;
        ClauseExecution[] vectorExecs = null;

        if (template.SpatialClauses != null)
        {
            int sLen = template.SpatialClauses.Count;
            spatialArr = new ClauseInfo[sLen];
            spatialExecs = new ClauseExecution[sLen];
            for (int si = 0; si < sLen; si++)
            {
                var sc = template.SpatialClauses[si];
                var scExec = new ClauseExecution(sc);
                PopulateClauseValues(scExec, planParams.QueryParameters, writer, builderParameters);
                spatialArr[si] = sc;
                spatialExecs[si] = scExec;
            }
        }

        if (template.VectorClauses != null)
        {
            int vLen = template.VectorClauses.Count;
            vectorArr = new ClauseInfo[vLen];
            vectorExecs = new ClauseExecution[vLen];
            for (int vi = 0; vi < vLen; vi++)
            {
                var vc = template.VectorClauses[vi];
                var vcExec = new ClauseExecution(vc);
                PopulateClauseValues(vcExec, planParams.QueryParameters, writer, builderParameters);
                vectorArr[vi] = vc;
                vectorExecs[vi] = vcExec;
            }
        }

        AttachPostFilterPhases(exec, spatialArr, spatialExecs, vectorArr, vectorExecs);
    }

    private static void AttachPostFilterPhases(QueryExecution exec, ClauseInfo[] spatialClauses, ClauseExecution[] spatialExecs, ClauseInfo[] vectorClauses, ClauseExecution[] vectorExecs)
    {
        if (spatialClauses == null && vectorClauses == null)
            return;

        var execs = exec.Executions ??= [];

       
        if (spatialClauses != null)
        {
            // compute where to put the match (after all existing clauses, or if there are none, the first (maybe after the FillAllEntries if we have that)
            int matchIndex = exec.Cardinalities?.Length ?? (exec.IsAllEntries ? 1 : 0);
            exec.SpatialFilters = new SpatialFilterOp[spatialClauses.Length];
            for (int i = 0; i < spatialClauses.Length; i++)
            {
                var spatialExec = spatialExecs?[i] ?? new ClauseExecution(spatialClauses[i]);
                execs.Add(spatialExec);
                exec.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialClauses[i], Exec = spatialExec };
            }
        }

        if (vectorClauses != null)
        {
            exec.VectorSelects = new VectorSearchOp[vectorClauses.Length];
            for (int i = 0; i < vectorClauses.Length; i++)
            {
                var vectorExec = vectorExecs?[i] ?? new ClauseExecution(vectorClauses[i]);
                execs.Add(vectorExec);
                exec.VectorSelects[i] = new VectorSearchOp
                {
                    Clause = vectorClauses[i], Exec = vectorExec
                };
            }
        }

        exec.Executions = execs;
    }
}
