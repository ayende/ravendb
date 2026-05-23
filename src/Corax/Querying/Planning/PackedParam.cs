namespace Corax.Querying.Planning;

/// <summary>
/// Packed parameter reference — a 32-bit value encoding the type and index(es)
/// of a clause's resolved value within the plan's typed arrays
/// (QueryExecution.LongValues / DoubleValues / StringValues).
///
///   bits [31:30] = value type (Long=0, Double=1, String=2, None=3)
///   bits [29:15] = first parameter index (0..32767)
///   bits [14:0]  = second parameter index (0..32767, 0x7FFF = no second param)
///
/// For simple predicates (Equals, GT, LT, etc.): Param1 = value index, Param2 = NoParam.
/// For BETWEEN: Param1 = low-bound index, Param2 = high-bound index (same-typed array).
/// For IN/AllIn: Param1 = start index into the typed array. Term count is stored separately
///   in ClauseInfo.InTermCount (not packed) because IN can exceed 32K terms.
/// For parameterless clauses (Exists): None sentinel.
/// </summary>
public readonly struct PackedParam
{
    public const int NoParamValue = 0x7FFF;
    private const int MaxIndex = 0x7FFF; // 32767

    public const int TypeLong = 0;
    public const int TypeDouble = 1;
    public const int TypeString = 2;
    private const int TypeNone = 3;

    /// <summary>Sentinel: no parameter ("exists(Field)" clauses). Spatial and vector clauses
    /// store their numeric parameters directly on ClauseInfo fields instead.</summary>
    public static readonly PackedParam None = new((TypeNone << 30) | (NoParamValue << 15) | NoParamValue);

    private readonly int _value;

    private PackedParam(int raw) => _value = raw;

    public PackedParam(int type, int param1, int param2 = NoParamValue)
    {
        if (param1 > MaxIndex)
            ThrowLimitExceeded(param1);
        if (param2 != NoParamValue && param2 > MaxIndex)
            ThrowLimitExceeded(param2);
        _value = (type << 30) | ((param1 & 0x7FFF) << 15) | (param2 & 0x7FFF);
    }

    public int ValueType => (_value >>> 30) & 0x3;
    public int Param1 => (_value >>> 15) & 0x7FFF;
    public int Param2 => _value & 0x7FFF;
    public bool IsNone => _value == None._value;

    /// <summary>For IN/AllIn clauses: build a PackedParam pointing at the n-th IN term.
    /// IN terms are stored contiguously starting at Param1; offset n addresses Param1 + n.</summary>
    public PackedParam WithTermOffset(int termIndex) => new(ValueType, Param1 + termIndex);

    private static void ThrowLimitExceeded(int index)
    {
        throw new System.InvalidOperationException(
            $"Query parameter index {index} exceeds maximum ({MaxIndex}). " +
            "Simplify the query or reduce the number of IN terms.");
    }
}