using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Mappings;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Corax;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Spatial4n.Shapes;
using Sparrow;
using Sparrow.Json;
using RavenConstants = Raven.Client.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using IndexSearcher = Corax.Querying.IndexSearcher;
using SpatialRelation = Corax.Utils.Spatial.SpatialRelation;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Spatial and vector query materialization: resolves spatial shapes, vector
/// embeddings, and attaches post-filter phases (spatial AND, vector select)
/// to the query execution pipeline.
///
/// Extracted from Resolution.cs — all methods are pure extraction, no logic changes.
/// </summary>
internal static partial class QueryPlanBuilder
{
    // ── Spatial / Vector binding resolution ──────────────────────────────

    /// <summary>Resolve spatial parameters from cached bindings (no MethodExpression dependency).</summary>
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

        // Shape type determined by the number of bindings:
        // circle has 5 (distErrPct, radius, lat, lng, units), WKT has 3 (distErrPct, wkt, units)
        if (bindings.Length >= BindingIndex.SpatialCircleBindingCount - 1) // circle: at least distErrPct + radius + lat + lng
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
                    sp.Units = (SpatialUnits)su == SpatialUnits.Kilometers
                        ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                        : global::Corax.Utils.Spatial.SpatialUnits.Miles;
            }
        }
        else // WKT: distErrPct, wkt, [units]
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
                        sp.Units = (SpatialUnits)su == SpatialUnits.Kilometers
                            ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                            : global::Corax.Utils.Spatial.SpatialUnits.Miles;
                }
            }
        }

        exec.Spatial = sp;
    }

    /// <summary>Resolve vector parameters from cached bindings (no MethodExpression dependency).</summary>
    private static void ResolveVectorFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = exec.Clause.Bindings;

        var vec = exec.Vector = new VectorParams { Method = exec.Clause.VectorMethod };

        // [1]=minimumMatch, [2]=numberOfCandidates, [3]=aiTask
        if (bindings.Length > BindingIndex.VectorMinMatch && bindings[BindingIndex.VectorMinMatch] != null)
        {
            var (simVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorMinMatch], queryParameters, builderParameters: null);
            vec.MinimumMatch = simVal switch
            {
                double d => (float)d,
                long l => l,
                _ => -1
            };
        }

        if (bindings.Length > BindingIndex.VectorCandidates && bindings[BindingIndex.VectorCandidates] != null)
        {
            var (candVal, candType) = ResolveBindingScalar(bindings[BindingIndex.VectorCandidates], queryParameters, builderParameters: null);
            if (candType != ParamValueType.Null)
                vec.NumberOfCandidates = Convert.ToInt32(candVal);
        }

        if (bindings.Length > BindingIndex.VectorAiTask && bindings[BindingIndex.VectorAiTask] != null)
        {
            var (taskVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorAiTask], queryParameters, builderParameters: null);
            vec.AiTaskName = taskVal?.ToString();
        }

        // [0]=vector value (may be scalar, array, or blittable object)
        if (bindings.Length > BindingIndex.VectorValue && bindings[BindingIndex.VectorValue] != null)
        {
            var (val, valType) = ResolveBindingRaw(bindings[BindingIndex.VectorValue], queryParameters);
            vec.ResolvedValue = val;
            vec.ResolvedValueType = valType;
            // For scalar parameters, resolve the native type
            if (valType == ParamValueType.Parameter && val is not (BlittableJsonReaderArray or BlittableJsonReaderObject))
            {
                var (resolved, resolvedType) = ResolveParameterValue(val);
                vec.ResolvedValue = resolved;
                vec.ResolvedValueType = ToParamValueType(resolvedType);
            }
        }
    }

    // ── Spatial / Vector post-filter attachment ─────────────────────────

    /// <summary>Populate ClauseExecution slots for any template-recorded spatial / vector
    /// post-filter clauses (each a separate phase outside the main bitmap pipeline), then
    /// call <see cref="AttachPostFilterPhases"/> to wire them onto the plan. No-op when
    /// the template has neither.</summary>
    private static void AttachSpatialAndVectorClauses(
        QueryExecution exec, bool allNegated, PlanTemplate template, QueryPlanBuilder.PlanParameters planParams,
        QueryBuilderParameters builderParameters, ValueWriter writer)
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

        AttachPostFilterPhases(exec, allNegated, spatialArr, spatialExecs, vectorArr, vectorExecs);
    }

    private static void AttachPostFilterPhases(QueryExecution exec, bool allNegated,
        ClauseInfo[] spatialClauses, ClauseExecution[] spatialExecs,
        ClauseInfo[] vectorClauses, ClauseExecution[] vectorExecs)
    {
        if (spatialClauses == null && vectorClauses == null)
        {
            return;
        }

        // Extend Executions array to include spatial/vector post-filter clauses.
        int extraCount = (spatialClauses?.Length ?? 0) + (vectorClauses?.Length ?? 0);
        var execs = exec.Executions ??= [];

        int matchIndex = CountMatchSlots(execs, exec.IsAllEntries, allNegated);

        if (spatialClauses != null)
        {
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

    // ── Vector / Spatial resolution ──────────────────────────────────────

    /// <summary>
    /// Resolve vector select operations from the plan into CoraxVectorItem instances.
    /// These are NOT materialized yet — the caller materializes them with the bitmap-producing
    /// match as the filterQuery. Returns null if the plan has no vectors.
    /// </summary>
    private static List<CoraxVectorItem> ResolveVectorItems(QueryExecution exec, QueryBuilderParameters builderParams)
    {
        var items = new List<CoraxVectorItem>(exec.VectorSelects.Length);
        foreach (var vec in exec.VectorSelects)
        {
            items.Add(HandleVector(builderParams, vec.Exec, false));
        }
        return items;
    }

    private static IQueryMatch HandleSpatial(QueryBuilderParameters builderParameters, ClauseInfo clause, ClauseExecution exec, SpatialOperationType spatialMethod)
    {
        var index = builderParameters.Index;
        var allocator = builderParameters.Allocator;

        // Field name was pre-resolved during parsing.
        string fieldName = clause.FieldName
                           ?? throw new InvalidOperationException("Spatial clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, builderParameters.IndexFieldsMapping,
            builderParameters.FieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);

        var sp = exec.Spatial;
        var distanceErrorPct = sp.DistanceErrorPct >= 0
            ? sp.DistanceErrorPct
            : RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;

        var spatialField = builderParameters.Factories.GetSpatialFieldFactory(fieldName);

        // Build shape from pre-resolved parameters — no GetValue calls
        IShape shape;
        SpatialUnits? units = sp.Units.HasValue ? (SpatialUnits)sp.Units.Value : null;
        if (sp.ShapeType == SpatialShapeType.Circle)
        {
            shape = spatialField.ReadCircle(sp.CircleRadius, sp.CircleLatitude, sp.CircleLongitude, units);
        }
        else if (sp.Wkt != null)
        {
            shape = spatialField.ReadShape(sp.Wkt, units);
        }
        else
        {
            throw new InvalidOperationException("Spatial clause has no pre-resolved shape parameters.");
        }

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), (SpatialRelation)spatialMethod, token: builderParameters.Token);
    }

    private static CoraxVectorItem HandleVector(QueryBuilderParameters builderParameters, ClauseExecution exec, bool exact)
    {
        Debug.Assert(exec.ClauseType ==ClauseType.Vector);
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;

        var vec = exec.Vector;
        var minimumMatch = vec.MinimumMatch >= 0
            ? vec.MinimumMatch
            : builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;

        int numberOfCandidates = vec.NumberOfCandidates >= 0
            ? vec.NumberOfCandidates
            : builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;

        var fieldName = exec.Clause.FieldName
                        ?? throw new InvalidOperationException("Vector clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);

        // Use pre-resolved vector value and method kind from parsing
        object methodParameter = vec.ResolvedValue;
        ValueTokenType valueTokenType = ToValueTokenType(vec.ResolvedValueType);

        if (vec.Method != VectorSourceKind.Inline)
        {
            var method = vec.Method switch
            {
                VectorSourceKind.FromDocument => VectorHelpers.MethodVectorValue.ForDocument,
                VectorSourceKind.FromText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException($"Unknown vector source kind: {vec.Method}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata,
                        GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata,
                        GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString()), numberOfCandidates, minimumMatch, exact),
                    (_, BlittableJsonReaderArray { Length: > 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({vec.Method}. Parameter type: {methodParameter?.GetType().FullName}, Value: {methodParameter}")
                };
            }

            embeddingsGenerationTaskIdentifier = vec.AiTaskName;
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            if (vectorOptions != null)
            {
                vectorOptions = new VectorOptions()
                {
                    DestinationEmbeddingType = vectorOptions.DestinationEmbeddingType,
                    Dimensions = vectorOptions.Dimensions,
                    SourceEmbeddingType = VectorEmbeddingType.Text,
                    NumberOfCandidatesForIndexing = vectorOptions.NumberOfCandidatesForIndexing,
                    NumberOfEdges = vectorOptions.NumberOfEdges
                };
            }

            var vector = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueTokenType, methodParameter, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);

            if (vector.SingleVector != null)
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, exact);

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, exact);
        }

        // Direct value (not a method call) — use pre-resolved value
        var value = methodParameter;
        var valueType = valueTokenType;

        (VectorValue? SingleVector, VectorValue[] MultiVector) transformedEmbeddings = (null, null);
        int numberOfDimensions;
        if (VectorHelpers.TryRetrieveEmbeddingsGenerationTaskIdentifier(builderParameters, fieldName, out embeddingsGenerationTaskIdentifier))
        {
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            transformedEmbeddings = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueType, value, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);
        }
        else
        {
            VectorOptions vectorOptions = VectorHelpers.GetOptions(builderParameters, fieldName, out indexField);

            if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
                return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Text)
            {
                transformedEmbeddings = VectorHelpers.GetVectorValueForTextualInput(builderParameters, vectorOptions, valueType, value);
            }
            else
            {
                switch (value)
                {
                    case string s:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, s);
                        break;
                    case StringSegment stringSegment:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, stringSegment.ToString());
                        break;
                    case BlittableJsonReaderObject bjro:
                        transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, bjro, vectorOptions);
                        break;
                    case BlittableJsonReaderArray { Length: > 0 } bjra:
                    {
                        var isRavenVector = bjra[0] is BlittableJsonReaderObject;
                        var isStringArray = bjra[0] is string or StringSegment or LazyStringValue;
                        var isArray = bjra[0] is BlittableJsonReaderArray;

                        if (isRavenVector == false && isStringArray == false && isArray == false)
                        {
                            transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, bjra, vectorOptions);
                        }
                        else
                        {
                            var embeddings = new VectorValue[bjra.Length];
                            for (int i = 0; i < bjra.Length; ++i)
                            {
                                if (isRavenVector)
                                    embeddings[i] = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, (BlittableJsonReaderObject)bjra[i], vectorOptions);
                                else if (isStringArray)
                                    embeddings[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, bjra[i].ToString());
                                else
                                    embeddings[i] = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, (BlittableJsonReaderArray)bjra[i],
                                        vectorOptions);
                            }

                            transformedEmbeddings.MultiVector = embeddings;
                        }

                        break;
                    }
                    default:
                        PortableExceptions.Throw<InvalidDataException>("We expected to get vector(s), however got: " + value.GetType().Name);
                        break;
                }
            }
        }

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
            return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed

        if (transformedEmbeddings.SingleVector != null)
        {
            var singleVector = transformedEmbeddings.SingleVector.Value;

            if (indexField != null)
                AssertDimensions(singleVector);
            return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, singleVector, numberOfCandidates, minimumMatch, exact);
        }

        if (transformedEmbeddings.MultiVector != null)
        {
            var multiVector = transformedEmbeddings.MultiVector;

            if (indexField != null)
            {
                foreach (var vector in multiVector)
                    AssertDimensions(vector);
            }

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, multiVector, numberOfCandidates, minimumMatch, exact);
        }

        throw new InvalidDataException("Expected to get single or multiple embeddings of VectorValue type but none was provided");

        void AssertDimensions(in VectorValue vector)
        {
            if (numberOfDimensions != vector.Length)
            {
                using (vector)
                    VectorHelpers.ThrowDifferentNumberOfDimensions(indexField, fieldName, vector, numberOfDimensions);
            }
        }
    }

    private static class VectorHelpers
    {
        public enum MethodVectorValue
        {
            ForDocument,
            ForRaw,
            EmbeddingText
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRetrieveEmbeddingsGenerationTaskIdentifier(QueryBuilderParameters builderParameters, in string fieldName, out string embeddingsGenerationTaskIdentifier)
        {
            var existsInPersistence =
                builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out embeddingsGenerationTaskIdentifier);

            if (builderParameters.Metadata.IsDynamic == false)
                return existsInPersistence;

            if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField))) &&
                indexField.Vector is AutoVectorOptions avo)
            {
                embeddingsGenerationTaskIdentifier = avo.EmbeddingsGenerationTaskIdentifier;
                return string.IsNullOrEmpty(avo.EmbeddingsGenerationTaskIdentifier) == false;
            }

            embeddingsGenerationTaskIdentifier = null;
            return false;
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetVectorValueForTextualInput(QueryBuilderParameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
        {
            if (valueType is ValueTokenType.String)
                return (GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, value.ToString()), null);

            if (valueType is not ValueTokenType.Parameter)
                PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value. Got {valueType}");

            if (value is BlittableJsonReaderArray valueAsList)
            {
                var embeddings = new VectorValue[valueAsList.Length];
                for (var i = 0; i < valueAsList.Length; ++i)
                    embeddings[i] = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, valueAsList[i].ToString());

                return (null, embeddings);
            }

            PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value(s). Got {valueType}");
            return (null, null);
        }

        internal static VectorValue GetVectorValueFromRavenVector(QueryBuilderParameters parameters, BlittableJsonReaderObject json, VectorOptions vectorOptions)
        {
            var vectorObjectFound = json.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vectorObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(vectorObjectFound, "Cannot find vector property in the object.");

            var vectorReader = (BlittableJsonReaderVector)vectorObject;
            return QueryBuilderHelper.GetVectorValueFromBlittableJsonVectorReader(parameters.Allocator, vectorOptions, vectorReader);
        }

        internal static VectorValue GetVectorValueFromNumericalBlittableArray(QueryBuilderParameters parameters, BlittableJsonReaderArray array, VectorOptions vectorOptions)
        {
            var bytesUsed = array.Length * (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : 1);
            var memScope = parameters.Allocator.Allocate(bytesUsed, out Memory<byte> mem);

            // Hoist the per-element type switch out of the loop: SourceEmbeddingType is
            // constant for the whole array, so dispatching once and then running a tight
            // typed copy loop avoids repeating the branch (and the three ref captures) per
            // element.
            switch (vectorOptions.SourceEmbeddingType)
            {
                case VectorEmbeddingType.Single:
                    CopyFloats(array, MemoryMarshal.Cast<byte, float>(mem.Span));
                    break;
                case VectorEmbeddingType.Int8:
                    CopyInt8(array, MemoryMarshal.Cast<byte, sbyte>(mem.Span));
                    break;
                default:
                    CopyBytes(array, mem.Span);
                    break;
            }

            return GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);

            static void CopyFloats(BlittableJsonReaderArray src, Span<float> dst)
            {
                ref var dstRef = ref MemoryMarshal.GetReference(dst);
                for (int i = 0; i < src.Length; ++i)
                    Unsafe.Add(ref dstRef, i) = src.GetByIndex<float>(i);
            }

            static void CopyInt8(BlittableJsonReaderArray src, Span<sbyte> dst)
            {
                ref var dstRef = ref MemoryMarshal.GetReference(dst);
                for (int i = 0; i < src.Length; ++i)
                    Unsafe.Add(ref dstRef, i) = src.GetByIndex<sbyte>(i);
            }

            static void CopyBytes(BlittableJsonReaderArray src, Span<byte> dst)
            {
                ref var dstRef = ref MemoryMarshal.GetReference(dst);
                for (int i = 0; i < src.Length; ++i)
                    Unsafe.AddByteOffset(ref dstRef, i) = src.GetByIndex<byte>(i);
            }
        }

        internal static VectorOptions GetExplicitVectorOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            return indexField.Vector;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VectorOptions GetOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            // VectorOptions can be null when a user does not specify the configuration.
            // In such cases, we will choose the input depending on the value type (similar to how we handle it during indexing).
            if (indexField.Vector != null)
                return indexField.Vector;

            builderParameters.Index.IndexFieldsPersistence.TryReadVectorSourceEmbeddingType(fieldName, out var vectorSourceEmbeddingType);

            var defaultVectorOptions = vectorSourceEmbeddingType switch
            {
                VectorEmbeddingType.Single => VectorOptions.Default,
                VectorEmbeddingType.Text => VectorOptions.DefaultText,
                _ => throw new InvalidDataException(
                    $"Unknown vector source embedding type: {vectorSourceEmbeddingType}. Implicit configuration support only single and text vector source embedding types.")
            };

            indexField.Vector = defaultVectorOptions;

            return defaultVectorOptions;
        }

        internal static void ThrowDifferentNumberOfDimensions(in IndexField indexField, in string fieldName, in VectorValue transformedEmbedding,
            in int numberOfDimensions)
        {
            var (storedDimensions, inputDimensions) = indexField.Vector.DestinationEmbeddingType switch
            {
                VectorEmbeddingType.Single => (numberOfDimensions / sizeof(float), transformedEmbedding.Length / sizeof(float)),
                VectorEmbeddingType.Int8 => (numberOfDimensions - sizeof(float), transformedEmbedding.Length - sizeof(float)),
                VectorEmbeddingType.Binary => (numberOfDimensions, transformedEmbedding.Length),
                _ => throw new InvalidDataException($"Unexpected embedding type - {numberOfDimensions}.")
            };

            PortableExceptions.Throw<InvalidDataException>(
                $"Vector field `{fieldName}` has {storedDimensions} dimensions, but the vector passed to vector.search() has {inputDimensions} dimensions.");
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetEmbeddingsForQueryParameter(QueryBuilderParameters builderParameters, ValueTokenType valueType,
            object value,
            string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions, string fieldName)
        {
            var database = builderParameters.Index.DocumentDatabase;

            var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);

            var embeddingsGenerator = database.EmbeddingsGeneratorQueries;

            var sourceEmbeddingType = embeddingsGenerator.GetQuantizationOf(embeddingsTaskId);

            // Quantized dynamic field indicates that the task generated embeddings with different quantization than requested in the index
            // In this case we want to use quantization defined in dynamic field (which was set in CurrentIndexingScope.GetLoadVectorField)
            VectorEmbeddingType destinationEmbeddingType;
            if (builderParameters.Metadata.IsDynamic)
            {
                destinationEmbeddingType = sourceEmbeddingType is not VectorEmbeddingType.Single ? sourceEmbeddingType : vectorOptions!.DestinationEmbeddingType;
            }
            else
            {
                destinationEmbeddingType = vectorOptions?.DestinationEmbeddingType ?? sourceEmbeddingType;
            }

            ReadOnlyMemory<ReadOnlyMemory<byte>> embeddingValues;

            switch (valueType)
            {
                case ValueTokenType.String:
                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, value.ToString());
                    break;
                case ValueTokenType.Parameter:
                {
                    if (value is not BlittableJsonReaderArray bjra)
                        throw new InvalidQueryException($"Expected array as parameter of vector.search({fieldName}) method, got '{value.GetType().FullName}' type instead.");

                    var values = new string[bjra.Length];

                    for (var i = 0; i < values.Length; i++)
                        values[i] = bjra[i].ToString();

                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, values);
                    break;
                }
                default:
                    throw new NotSupportedException($"Unexpected value type provided as parameter to vector.search({fieldName}) method. Got '{value.GetType().FullName}' type.");
            }

            var queryingVectorOption = new VectorOptions
            {
                SourceEmbeddingType = sourceEmbeddingType,
                DestinationEmbeddingType = destinationEmbeddingType
            };

            if (embeddingValues.Length == 1)
            {
                var embeddingValue = embeddingValues.Span[0];

                return (GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption), null);
            }
            else
            {
                var vectorValues = new VectorValue[embeddingValues.Length];

                for (int i = 0; i < embeddingValues.Length; i++)
                {
                    var embeddingValue = embeddingValues.Span[i];

                    vectorValues[i] = GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption);
                }

                return (null, vectorValues);
            }
        }
    }
}
