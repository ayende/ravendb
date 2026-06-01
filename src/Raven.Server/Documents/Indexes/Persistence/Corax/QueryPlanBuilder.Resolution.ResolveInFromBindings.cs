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
    private static void ResolveInFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer,
        Span<ParameterBinding> bindings, QueryBuilderParameters builderParameters)
    {
        var resolvedValues = new List<object>(bindings.Length);
        var termTypes = new List<ParamValueType>(bindings.Length);
        bool hasNullTerm = false;

        foreach (var it in bindings)
        {
            if (it.Source == BindingSource.QueryParameter // handle array-valued query parameters
                && queryParameters.TryGet(it.ParameterName, out object raw)
                && raw is BlittableJsonReaderArray arr)
            {
                foreach (var elem in arr)
                {
                    var (elemVal, elemType) = ResolveParameterValue(elem);
                    AddInValue(elemVal, ToParamValueType(elemType));
                }

                continue;
            }

            var (val, type) = ResolveBindingScalar(it, queryParameters, builderParameters); // normal parameter
            AddInValue(val, type);
        }

        ParamValueType dominantType = resolvedValues.Count > 0 ? termTypes[0] : ParamValueType.String;
        EmitInTerms(exec, writer, dominantType, resolvedValues, hasNullTerm);

        void AddInValue(object val, ParamValueType type)
        {
            if (val == null)
            {
                hasNullTerm = true;
                return;
            }

            resolvedValues.Add(val);
            termTypes.Add(type);
        }
    }
}
