using System.Threading;

namespace Corax.Querying.Planning;

/// <summary>
/// Thread-safe exponentially-weighted moving average of a streaming sort's <em>scan-inflation
/// factor</em>: (index entries actually scanned) / (the cost gate's uniform estimate). One instance
/// lives on each <see cref="CompiledPlan"/> and is shared by every concurrent query that hits the
/// plan, so each run's observation is weighted equally regardless of its size — this is a smoothed
/// multiplier, not a volume-weighted ratio.
/// </summary>
/// <remarks>
/// Writes are rare (once per streaming run) and use a compare-exchange loop; reads are on the hot
/// cost-gate path and are plain. The race between a reader and a writer is benign: on 64-bit the
/// read sees either the old or new value (never torn), and on 32-bit a torn read at worst nudges one
/// query's estimate — acceptable for a heuristic that self-corrects on the next observation.
/// </remarks>
public sealed class ScanInflationEwma
{
    private const double Alpha = 0.05;

    /// <summary>The smoothed factor, or 0 before the first observation ("no history").
    /// Written only via <see cref="Interlocked.CompareExchange(ref double, double, double)"/>.</summary>
    private double _factor;

    /// <summary>The learned inflation factor, or 0 when no streaming run has been observed yet
    /// (callers treat 0 as "trust the raw estimate").</summary>
    public double Factor => _factor;

    /// <summary>
    /// Fold one streaming run into the average. <paramref name="entriesScanned"/> is what the walk
    /// actually read; <paramref name="estimatedScan"/> is what the gate predicted. Their ratio is the
    /// inflation this run exhibited — the first observation seeds the average, later ones blend at Alpha.
    /// </summary>
    public void Observe(long entriesScanned, long estimatedScan)
    {
        if (estimatedScan <= 0)
            return;

        double sample = (double)entriesScanned / estimatedScan;

        while (true)
        {
            double current = _factor;
            double updated = current == 0
                ? sample
                : current * (1 - Alpha) + sample * Alpha;

            if (Interlocked.CompareExchange(ref _factor, updated, current) == current)
                return;
        }
    }
}
