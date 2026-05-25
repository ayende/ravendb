using System;
using System.Collections.Generic;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>
    /// Accumulates typed parameter values during query plan building.
    /// Each Add call stores the native value in the appropriate-typed list
    /// and returns a <see cref="PackedParam"/> encoding (type + index) for the clause.
    /// Values are stored as their native types — no string round-trips.
    /// </summary>
    private sealed class ValueWriter
    {
        private readonly List<long> _longs = [];
        private readonly List<double> _doubles = [];
        private readonly List<string> _strings = [];

        private PackedParam AddLong(long value)
        {
            _longs.Add(value);
            return new PackedParam(PackedParam.TypeLong, _longs.Count - 1);
        }

        private PackedParam AddDouble(double value)
        {
            _doubles.Add(value);
            return new PackedParam(PackedParam.TypeDouble, _doubles.Count - 1);
        }

        private PackedParam AddString(string value)
        {
            _strings.Add(value);
            return new PackedParam(PackedParam.TypeString, _strings.Count - 1);
        }

        /// <summary>Add a resolved value by its detected type. Used by Parse* methods
        /// after <see cref="ResolveTermValue"/> determines the native type.</summary>
        public PackedParam Add(object value, ValueTokenType type)
        {
            return type switch
            {
                ValueTokenType.Long => AddLong(value is long l ? l : Convert.ToInt64(value)),
                ValueTokenType.Double => AddDouble(value is double d ? d : Convert.ToDouble(value)),
                _ => AddString(value?.ToString())
            };
        }

        /// <summary>Add a pair of resolved values (for BETWEEN).</summary>
        public PackedParam AddPair(object value1, object value2, ValueTokenType type)
        {
            return type switch
            {
                ValueTokenType.Long => AddLongPair(
                    value1 is long l1 ? l1 : Convert.ToInt64(value1),
                    value2 is long l2 ? l2 : Convert.ToInt64(value2)),
                ValueTokenType.Double => AddDoublePair(
                    value1 is double d1 ? d1 : Convert.ToDouble(value1),
                    value2 is double d2 ? d2 : Convert.ToDouble(value2)),
                _ => AddStringPair(value1?.ToString(), value2?.ToString())
            };

            PackedParam AddLongPair(long low, long high)
            {
                _longs.Add(low);
                _longs.Add(high);
                return new PackedParam(PackedParam.TypeLong, _longs.Count - 2, _longs.Count - 1);
            }

            PackedParam AddDoublePair(double low, double high)
            {
                _doubles.Add(low);
                _doubles.Add(high);
                return new PackedParam(PackedParam.TypeDouble, _doubles.Count - 2, _doubles.Count - 1);
            }

            PackedParam AddStringPair(string low, string high)
            {
                _strings.Add(low);
                _strings.Add(high);
                return new PackedParam(PackedParam.TypeString, _strings.Count - 2, _strings.Count - 1);
            }
        }

        public int LongCount => _longs.Count;
        public int DoubleCount => _doubles.Count;
        public int StringCount => _strings.Count;

        public long GetLong(int index) => _longs[index];
        public double GetDouble(int index) => _doubles[index];
        public string GetString(int index) => _strings[index];

        public void SetValues(QueryExecution exec)
        {
            exec.LongValues = _longs.Count > 0 ? _longs.ToArray() : [];
            exec.DoubleValues = _doubles.Count > 0 ? _doubles.ToArray() : [];
            exec.StringValues = _strings.Count > 0 ? _strings.ToArray() : [];
        }

        public (int PackedType, int StartIdx) ResolveInSlot(ParamValueType dominantType)
        {
            int packedType = dominantType switch
            {
                ParamValueType.Long => PackedParam.TypeLong,
                ParamValueType.Double => PackedParam.TypeDouble,
                _ => PackedParam.TypeString
            };
            int startIdx = packedType switch
            {
                PackedParam.TypeLong => LongCount,
                PackedParam.TypeDouble => DoubleCount,
                _ => StringCount
            };
            return (packedType, startIdx);
        }
    }
}
