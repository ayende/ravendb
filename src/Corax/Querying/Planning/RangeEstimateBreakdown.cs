namespace Corax.Querying.Planning;

/// <summary>Raw inputs and intermediate values behind a single <see cref="IndexSearcher.EstimateMatchesInRange{TValue}"/>
/// run, captured per-execution so the inspection / <c>include timings()</c> view can explain WHY a range clause's
/// estimate came out the way it did (not just the final number). Plans are cached and reused across parameter sets,
/// so these values must be carried on the per-execution <see cref="ClauseExecution"/>, never on the cached plan.</summary>
public struct RangeEstimateBreakdown
{
    /// <summary>The final estimate this run produced (already capped at NumberOfEntries) = <see cref="RawEstimate"/>
    /// times the learned calibration multiplier. This is what cost gates consume.</summary>
    public long Estimate;

    /// <summary>The pre-calibration estimate (the cold-start beta=1 shrinkage blend, capped). This is the
    /// quantity fed to the calibration EWMA as "predicted" — the multiplier is EWMA(actual / RawEstimate), so
    /// the next run's Estimate = RawEstimate * multiplier converges to the observed actual.</summary>
    public long RawEstimate;

    /// <summary>Total in-range term count from the term-count descent (the population the sampling extrapolates over).</summary>
    public long RangeTerms;

    /// <summary>Terms actually walked across the bottom + top edge samples.</summary>
    public long SampledTerms;

    /// <summary>Postings summed over the sampled edge terms.</summary>
    public long SampledPostings;

    /// <summary>Unscanned middle terms (RangeTerms - SampledTerms) the shrinkage blend had to guess at.</summary>
    public long MiddleTerms;

    /// <summary>Per-term posting density measured on the sampled edges (SampledPostings / SampledTerms).</summary>
    public double SampledAvg;

    /// <summary>Field-wide density (NumberOfEntries / total term count) the middle is shrunk toward.</summary>
    public double GlobalAvg;

    /// <summary>The per-clause calibration multiplier fed in (EWMA of matched/estimated; 0 = no history).</summary>
    public double CalibrationFactor;

    /// <summary>The clamped shrinkage strength actually used (CalibrationFactor folded into [0.25, 4.0], 1.0 cold-start).</summary>
    public double Beta;

    /// <summary>Pseudo-observation count Beta * MiddleTerms used in the shrinkage blend.</summary>
    public double K;

    /// <summary>The blended density assigned to the unscanned middle.</summary>
    public double MiddleAvg;

    /// <summary>True when the range was small enough to be counted exactly (no extrapolation): the edge sample
    /// covered every in-range term, so Estimate is an exact posting count rather than a shrinkage blend.</summary>
    public bool IsExact;
}
