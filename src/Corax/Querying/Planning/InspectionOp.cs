namespace Corax.Querying.Planning;

public sealed class InspectionOp
{
    public string Name;
    public string Dispatch;
    public string FieldName;
    public string ClauseType;
    public bool IsNegated;
    public long EstimatedCardinality;

    public int FlatClauseIndex = -1;

    /// <summary>Index of this op in the FULL PlanOp[] stream (before control-flow ops are filtered out of the
    /// inspection template). Runtime per-op telemetry (timings / result counts) is recorded against the full
    /// stream index, so the timing overlay must join on this — not on the op's position in the compacted
    /// template, which drifts once any op is dropped.</summary>
    public int OpIndex = -1;

    /// <summary>Bitmap slot this op writes its result into (<see cref="PlanOp.BitmapLocal"/>). Every op has a
    /// destination, so this is always &gt;= 0 for a real op — it is what lets a consumer reconstruct the
    /// physical dataflow (slot = node, op = edge into the slot) and emit a graph.</summary>
    public int DestSlot = -1;

    /// <summary>For the slot-to-slot merge ops (AND-Bitmaps / ANDNOT-Bitmaps / OR-Bitmaps) the SOURCE slot
    /// that is merged into <see cref="DestSlot"/> (<see cref="PlanOp.ParamIndex2"/>). -1 for leaf and
    /// control-flow ops, which take their operand from the leaf cursor rather than another slot.</summary>
    public int SourceSlot = -1;

    /// <summary>True for the <see cref="PlanOp.Kind"/> == MaybeEntryScan op: a read-only cost gate on the
    /// slot-0 accumulator that may divert execution to the shared entry-scan tail. Carried as a typed flag so
    /// consumers do not have to match on the display <see cref="Name"/>.</summary>
    public bool IsEntryScanGate;

    /// <summary>For the IN/range-expansion ops (OR-Range / AND-Range) the index into
    /// <see cref="Matches.CompiledQueryMatch.InRangeCounts"/> that holds the number of expanded term slots
    /// this op unions/intersects. The expansion width is only known at runtime, so the template cannot bake it
    /// into the display name; the timing overlay reads <c>InRangeCounts[RangeCountIndex]</c> to surface the
    /// actual term count. -1 for every non-range op.</summary>
    public int RangeCountIndex = -1;
}
