using System;
using Corax.Querying.Planning;
using Raven.Client.Exceptions;
using Sparrow.Json;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static void ResolveSpatialFromBindings(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        var bindings = exec.Clause.Bindings;
        var sp = new SpatialParams();
        string fieldName = exec.Clause.FieldName;
        string queryText = builderParameters?.Metadata?.QueryText;

        // [0] = distanceErrorPct
        if (bindings.Length > 0 && bindings[BindingIndex.SpatialDistErrPct] != null)
        {
            var (depVal, depType) = ResolveBindingScalar(bindings[BindingIndex.SpatialDistErrPct], slotBindings, queryParameters, builderParameters);
            if (depVal != null)
                sp.DistanceErrorPct = AsDouble(depVal, depType, "distanceErrorPct");
        }

        switch (bindings.Length) // Shape type determined by the number of bindings
        {
            case >= BindingIndex.SpatialCircleBindingCount - 1: // circle: at least distErrPct + radius + lat + lng
            {
                sp.ShapeType = SpatialShapeType.Circle;
                sp.CircleRadius = ResolveDouble(BindingIndex.SpatialRadius, "radius");
                sp.CircleLatitude = ResolveDouble(BindingIndex.SpatialLatitude, "latitude");
                sp.CircleLongitude = ResolveDouble(BindingIndex.SpatialLongitude, "longitude");
                if (bindings.Length > BindingIndex.SpatialUnits && bindings[BindingIndex.SpatialUnits] != null)
                    ApplyUnits(BindingIndex.SpatialUnits);

                break;
            }
            default: // WKT: distErrPct, wkt, [units]
            {
                sp.ShapeType = SpatialShapeType.Wkt;
                if (bindings.Length > BindingIndex.SpatialWkt && bindings[BindingIndex.SpatialWkt] != null)
                {
                    var (wkt, wktType) = ResolveBindingScalar(bindings[BindingIndex.SpatialWkt], slotBindings, queryParameters, builderParameters);
                    if (wkt == null || wktType != ParamValueType.String)
                        throw new InvalidQueryException($"Spatial WKT value for field '{fieldName}' must be a string, but got '{(wkt == null ? "null" : wktType.ToString())}'.", queryText, queryParameters);
                    sp.Wkt = wkt.ToString();
                    if (bindings.Length > BindingIndex.SpatialWktUnits && bindings[BindingIndex.SpatialWktUnits] != null)
                        ApplyUnits(BindingIndex.SpatialWktUnits);
                }

                break;
            }
        }

        exec.Spatial = sp;

        double ResolveDouble(int bindingIndex, string component)
        {
            var (value, valueType) = ResolveBindingScalar(bindings[bindingIndex], slotBindings, queryParameters, builderParameters);
            return AsDouble(value, valueType, component);
        }

        double AsDouble(object value, ParamValueType valueType, string component)
        {
            if (valueType is not (ParamValueType.Long or ParamValueType.Double))
                throw new InvalidQueryException($"Spatial {component} for field '{fieldName}' must be a number, but got '{(value == null ? "null" : valueType.ToString())}'.", queryText, queryParameters);
            return Convert.ToDouble(value);
        }

        void ApplyUnits(int bindingIndex)
        {
            var (u, uType) = ResolveBindingScalar(bindings[bindingIndex], slotBindings, queryParameters, builderParameters);
            if (u == null)
                return; // units omitted -> field default

            if (uType != ParamValueType.String)
                throw new InvalidQueryException($"Spatial units for field '{fieldName}' must be a string, but got '{uType}'.", queryText, queryParameters);
            if (Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su) == false)
                throw new InvalidQueryException($"{nameof(SpatialUnits)} value must be either '{SpatialUnits.Kilometers}' or '{SpatialUnits.Miles}' but was '{u}'.", queryText, queryParameters);
            sp.Units = ToCoraxUnits(su);
        }

        global::Corax.Utils.Spatial.SpatialUnits ToCoraxUnits(object su) =>
            (SpatialUnits)su == SpatialUnits.Kilometers
                ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                : global::Corax.Utils.Spatial.SpatialUnits.Miles;
    }

    internal static void AttachSpatialAndVectorClauses(QueryExecution exec, PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ValueWriter writer)
    {
        if (template.SpatialClauses == null && template.VectorClauses == null)
            return;

        ClauseExecution[] spatialExecs = null;
        ClauseExecution[] vectorExecs = null;

        if (template.SpatialClauses != null)
        {
            int sLen = template.SpatialClauses.Count;
            spatialExecs = new ClauseExecution[sLen];
            for (int si = 0; si < sLen; si++)
            {
                var scExec = new ClauseExecution(template.SpatialClauses[si]);
                PopulateClauseValues(scExec, planParams.SlotBindings, planParams.QueryParameters, writer, builderParameters, Span<ulong>.Empty);
                spatialExecs[si] = scExec;
            }
        }

        if (template.VectorClauses != null)
        {
            int vLen = template.VectorClauses.Count;
            vectorExecs = new ClauseExecution[vLen];
            for (int vi = 0; vi < vLen; vi++)
            {
                var vcExec = new ClauseExecution(template.VectorClauses[vi]);
                PopulateClauseValues(vcExec, planParams.SlotBindings, planParams.QueryParameters, writer, builderParameters, Span<ulong>.Empty);
                vectorExecs[vi] = vcExec;
            }
        }

        AttachPostFilterPhases(exec, spatialExecs, vectorExecs);
    }

    private static void AttachPostFilterPhases(QueryExecution exec, ClauseExecution[] spatialExecs, ClauseExecution[] vectorExecs)
    {
        if (spatialExecs == null && vectorExecs == null)
            return;

        var execs = exec.Executions ??= [];

        if (spatialExecs != null)
        {
            // compute where to put the match (after all existing clauses, or if there are none, the first (maybe after the FillAllEntries if we have that)
            int matchIndex = exec.Cardinalities?.Length ?? (exec.IsAllEntries ? 1 : 0);
            exec.SpatialFilters = new SpatialFilterOp[spatialExecs.Length];
            for (int i = 0; i < spatialExecs.Length; i++)
            {
                var spatialExec = spatialExecs[i];
                execs.Add(spatialExec);
                exec.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialExec.Clause, Exec = spatialExec };
            }
        }

        if (vectorExecs != null)
        {
            exec.VectorSelects = new ClauseExecution[vectorExecs.Length];
            for (int i = 0; i < vectorExecs.Length; i++)
            {
                var vectorExec = vectorExecs[i];
                execs.Add(vectorExec);
                exec.VectorSelects[i] = vectorExec;
            }
        }

        exec.Executions = execs;
    }
}
