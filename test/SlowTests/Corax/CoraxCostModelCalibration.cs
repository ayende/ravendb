using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Corax.Querying.Matches.SortingMatches;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

/// <summary>
/// Calibration harness for the Corax cost models / cost gates (RavenDB-25281). These are NOT pass/fail
/// correctness tests — they force each branch of a cost gate via the reserved <c>$rvn_corax_sort</c> /
/// <c>$rvn_corax_strategy</c> pins, read the real server-side <c>include timings()</c> numbers (uninstrumented,
/// the same numbers an operator sees), and emit a report from which the true cost constants are derived.
///
/// They are gated behind the RAVEN_CALIBRATE env var so they are a fast no-op in CI; set RAVEN_CALIBRATE=1
/// to run the sweep. Read the report from the test output.
///
/// Gate inventory (see also the constants they feed):
///   1. ShouldUseIndexOrderStreaming  -> IndexStreamingVsInMemorySortCostRatio (this file calibrates it)
///   2. StreamInIndexOrder bailout     -> maxScanCandidateMultiplier (this file probes the crossover)
///   3. IsDirectScanCostEffective      -> EntryScanCostMultiplier / EntryScanCountThreshold (TODO method below)
///   4. EstimateMatchesInRange         -> CalibrationBeta clamps / sample sizes (TODO method below)
/// </summary>
public class CoraxCostModelCalibration : RavenTestBase
{
    private readonly ITestOutputHelper _output;

    public CoraxCostModelCalibration(ITestOutputHelper output) : base(output) => _output = output;

    private static bool Enabled => Environment.GetEnvironmentVariable("RAVEN_CALIBRATE") == "1";

    // How many timed repetitions per measurement; we report the MIN (least noisy, closest to the real cost
    // with no GC/scheduling interference) alongside the median.
    private const int Repetitions = 7;

    /// <summary>
    /// Sort-strategy gate: for a range of (candidate fraction, limit, value cardinality) over a multi-valued
    /// sort field, force InMemorySort and IndexOrderStreaming separately and measure the SortingMatch node's
    /// own Ms. The derived ratio  (sortMs/candidates) / (streamMs/entriesStreamed)  is the real value the
    /// IndexStreamingVsInMemorySortCostRatio constant should reflect; the EntriesStreamed/candidates column
    /// shows where the maxScanCandidateMultiplier bailout should sit.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_SortStrategy_StreamVsInMemory(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the sort-strategy cost calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# Sort-strategy calibration  (docs={totalDocs:N0}, repetitions={Repetitions}, MIN ms)");
        _output.WriteLine("tagFreq% | candidates | take | InMemMs | StreamMs | EntriesStreamed | scan/cand | speedup | K(per-unit)");
        _output.WriteLine(new string('-', 110));

        // Pick tags whose posting list spans a range of selectivities; for each, sweep the LIMIT.
        foreach (var tag in new[] { "tag_dense", "tag_mid", "tag_rare" })
        {
            foreach (var take in new[] { 10, 25, 100, 1000 })
            {
                var inMem = await MeasureSort(store, tag, take, CoraxSortingStrategy.InMemorySort);
                var stream = await MeasureSort(store, tag, take, CoraxSortingStrategy.IndexOrderStreaming);

                double scanPerCand = stream.EntriesStreamed / (double)Math.Max(1, inMem.Candidates);
                double speedup = inMem.SortMs / Math.Max(0.0001, stream.SortMs);
                // Per-unit cost ratio: cost(sorted candidate) / cost(streamed entry).
                double perUnitK = (inMem.SortMs / Math.Max(1, inMem.Candidates))
                                  / (stream.SortMs / Math.Max(1, stream.EntriesStreamed));

                double freqPct = 100.0 * inMem.Candidates / totalDocs;
                _output.WriteLine(
                    $"{freqPct,7:F2} | {inMem.Candidates,10:N0} | {take,4} | {inMem.SortMs,7:F2} | {stream.SortMs,8:F3} | " +
                    $"{stream.EntriesStreamed,15:N0} | {scanPerCand,8:F2} | {speedup,7:F1}x | {perUnitK,8:F1}");
            }
        }

        _output.WriteLine("");
        _output.WriteLine("Interpretation: K(per-unit) is what IndexStreamingVsInMemorySortCostRatio approximates.");
        _output.WriteLine("The gate should choose streaming while  scan/cand < K(per-unit)  (i.e. speedup > 1).");
    }

    private sealed record SortMeasurement(double SortMs, long Candidates, long EntriesStreamed);

    private async Task<SortMeasurement> MeasureSort(DocumentStore store, string tag, int take, CoraxSortingStrategy strategy)
    {
        var rql = $"from index '{new ItemsIndex().IndexName}' where Tags = $t order by Tags include timings() limit {take}";

        double best = double.MaxValue;
        long candidates = 0, entriesStreamed = 0;
        for (int i = 0; i < Repetitions; i++)
        {
            using var session = store.OpenAsyncSession();
            // NoCaching is essential: without it the repeated identical queries are served from the client
            // cache, which returns the cached rows WITHOUT re-populating timings/QueryPlan (DurationInMs=0).
            var q = session.Advanced.AsyncRawQuery<ItemDoc>(rql)
                .AddParameter("t", tag)
                .AddParameter("rvn_corax_sort", strategy.ToString())
                .NoCaching();
            await q.Timings(out var timings).ToListAsync();

            var sort = FindSortingMatch((QueryInspectionNode)timings.QueryPlan);
            Assert.NotNull(sort);
            // Confirm the pin actually took (a pin that can't apply is silently ignored).
            Assert.Equal(strategy.ToString(), sort.Parameters["Strategy"]);

            double ms = sort.Parameters.TryGetValue("Ms", out var msStr)
                ? double.Parse(msStr, CultureInfo.InvariantCulture)
                : 0;
            best = Math.Min(best, ms);
            candidates = long.Parse(sort.Parameters["Incoming"], NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            if (sort.Parameters.TryGetValue("EntriesStreamed", out var es))
                entriesStreamed = long.Parse(es, NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
        }

        if (entriesStreamed == 0)
            entriesStreamed = candidates; // InMemorySort doesn't stream; use candidates so the per-unit math stays defined.

        return new SortMeasurement(best, candidates, entriesStreamed);
    }

    private static QueryInspectionNode FindSortingMatch(QueryInspectionNode node)
    {
        if (node.Operation == "SortingMatch")
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindSortingMatch(child);
            if (found != null)
                return found;
        }

        return null;
    }

    private async Task<DocumentStore> BuildItemsStore(Options options, int totalDocs, int distinctTags, int tagsPerDoc, bool clustered)
    {
        var store = GetDocumentStore(options);
        await new ItemsIndex().ExecuteAsync(store);

        // Three "probe" tags with controlled frequency so the sweep covers dense/mid/rare candidate sets,
        // plus filler tags. When clustered=true a doc's probe tag correlates with its insertion order, which
        // is the pathological case for ascending IndexOrderStreaming (the over-scan the inflation EWMA learns).
        var rng = new Random(20250618);
        var fillerTags = Enumerable.Range(0, distinctTags).Select(i => $"g{i:D2}").ToArray();

        var bulk = store.BulkInsert();
        await using (bulk)
        {
            for (int i = 0; i < totalDocs; i++)
            {
                var tags = new List<string>(tagsPerDoc + 1);

                // probe tag: dense ~40%, mid ~8%, rare ~0.5%
                double r = clustered ? (double)i / totalDocs : rng.NextDouble();
                if (r < 0.40) tags.Add("tag_dense");
                else if (r < 0.48) tags.Add("tag_mid");
                else if (r < 0.485) tags.Add("tag_rare");

                for (int t = 0; t < tagsPerDoc; t++)
                    tags.Add(fillerTags[rng.Next(fillerTags.Length)]);

                await bulk.StoreAsync(new ItemDoc { Tags = tags.Distinct().ToArray() });
            }
        }

        await Indexes.WaitForIndexingAsync(store);
        return store;
    }

    private class ItemDoc
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<ItemDoc>
    {
        public ItemsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Tags };
        }
    }
}
