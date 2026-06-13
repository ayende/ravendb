using System;
using System.Threading;

namespace Corax.Querying.Planning;

/// <summary>
/// Thread-safe exponentially-weighted moving average of an <em>inflation factor</em>: the ratio of
/// what actually happened to what a heuristic predicted (<c>actual / predicted</c>). It is a smoothed,
/// self-correcting multiplier that lets a cheap a-priori estimate learn from the outcomes it produced,
/// without any per-run state beyond a single double.
/// </summary>
/// <remarks>
/// <para>One implementation, two consumers:</para>
/// <list type="bullet">
///   <item><b>Streaming-sort scan inflation</b> (per <see cref="CompiledPlan"/>): (index entries actually
///   scanned) / (the cost gate's uniform estimate). Lifts the gate's estimate when the sort axis is
///   clustered so a streaming walk really reads more than uniform.</item>
///   <item><b>Range-estimate calibration</b> (per <see cref="ClauseInfo"/>): (documents the range clause
///   actually matched) / (the range cardinality estimate). Feeds the shrinkage strength used by the
///   two-ended range probe in <c>IndexSearcher.EstimateMatchesInRange</c>, nudging the unscanned-middle
///   extrapolation toward the global density when this clause has historically been under-estimated.</item>
/// </list>
/// <para>Each instance is shared by every concurrent query that reaches it, so each run's observation is
/// weighted equally regardless of its size — this is a smoothed multiplier, not a volume-weighted ratio.</para>
/// <para>Writes are rare (once per observed run) and use a compare-exchange loop; reads are on hot paths and
/// are plain. The race between a reader and a writer is benign: on 64-bit the read sees either the old or new
/// value (never torn), and on 32-bit a torn read at worst nudges one query's estimate — acceptable for a
/// heuristic that self-corrects on the next observation.</para>
/// </remarks>
public sealed class InflationEwma
{
    private const double Alpha = 0.05;

    /// <summary>Skip the (contended) write when the blended value would move the factor by less than
    /// this fraction of its current magnitude. The blend moves the factor by Alpha*(sample-current),
    /// so a 1% relative move corresponds to a sample within ~20% of the current factor — the
    /// steady state once converged. A sub-1% nudge cannot change a consumer's decision, so suppressing
    /// it spares the shared cache line a lock cmpxchg on every one of many concurrent queries hitting
    /// the same instance.</summary>
    private const double ConvergenceTolerance = 0.01;

    /// <summary>The smoothed factor, or 0 before the first observation ("no history").
    /// Written only via <see cref="Interlocked.CompareExchange(ref double, double, double)"/>.</summary>
    private double _factor;

    /// <summary>The learned inflation factor, or 0 when nothing has been observed yet
    /// (callers treat 0 as "trust the raw estimate / use the neutral default").</summary>
    public double Factor => _factor;

    /// <summary>
    /// Fold one run into the average. <paramref name="actual"/> is what really happened (entries scanned,
    /// documents matched, ...); <paramref name="predicted"/> is what the heuristic estimated. Their ratio
    /// is the inflation this run exhibited — the first observation seeds the average, later ones blend at Alpha.
    /// </summary>
    public void Observe(long actual, long predicted)
    {
        if (predicted <= 0)
            return;

        double sample = (double)actual / predicted;

        while (true)
        {
            double current = _factor;
            double updated = current == 0
                ? sample
                : current + Alpha * (sample - current);

            // Converged: the update would move the factor by less than ConvergenceTolerance of its
            // magnitude, so don't pay for the write. (current == 0 is "no history" — always seed.)
            if (current != 0 && Math.Abs(updated - current) <= current * ConvergenceTolerance)
                return;

            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (Interlocked.CompareExchange(ref _factor, updated, current) == current)
                return;
        }
    }
}
