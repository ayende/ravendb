using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    // RavenDB-25281: inside the BitmapPipeline strategy, a per-execution cost gate decides whether to
    // switch from bitmap AND to a per-entry residual scan. The reserved query parameter
    // $rvn_corax_entry_scan overrides that gate: a non-negative value forces the scan at the gate whose
    // op-index equals it (the index the plan reports as EntryScanAt), and -1 disables every gate.
    //
    // These tests prove the override is honored independently of the cost gate's own verdict: forcing the
    // gate's op-index runs the residual scan there, -1 suppresses every gate, an unknown op-index is a safe
    // no-op, and in every case the documents returned match the disabled (pure bitmap) baseline — so the
    // override only changes the execution path, never the result set.
    public class RavenDB_25281_ForcedEntryScan : RavenTestBase
    {
        public RavenDB_25281_ForcedEntryScan(ITestOutputHelper output) : base(output)
        {
        }

        private const int DocCount = 5_000;

        // Only the first RareCategoryDocs documents carry RareCategory, so it is a small slot-0 clause; a
        // common category is a large one. The override is exercised against both candidate-set sizes.
        private const string RareCategory = "rare";
        private const int RareCategoryDocs = 10;
        private const string CommonCategory = "common-0";

        private sealed class Item
        {
            public string Id { get; set; }
            public string Category { get; set; }
            public string Status { get; set; }
        }

        private IDocumentStore GetSeededStore()
        {
            IDocumentStore store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < DocCount; i++)
                {
                    bulk.Store(new Item
                    {
                        Category = i < RareCategoryDocs ? RareCategory : $"common-{i % 50}",
                        Status = i % 2 == 0 ? "active" : "inactive"
                    });
                }
            }

            Indexes.WaitForIndexing(store);
            return store;
        }

        // Category = $cat AND Status = $st: Category fills the accumulator, Status is AND'd behind a
        // MaybeEntryScan gate (Status = becomes the residual predicate when the scan runs). Two equalities
        // with no ORDER BY keep this on the bitmap pipeline. Parameterized so different category values
        // reuse the same cached plan — the gate then sits at the same op-index for both.
        private const string Rql = "from Items where Category = $cat and Status = $st";

        // Runs the query, optionally pinning $rvn_corax_entry_scan, and returns the result ids (sorted, so
        // membership compares regardless of emission order) and the op-index the plan says the entry scan
        // ran at (-1 when no scan was taken).
        private static (List<string> Ids, int EntryScanAt) Run(IDocumentStore store, string category, long? force)
        {
            using IDocumentSession session = store.OpenSession();
            // NoCaching: the same RQL+parameters is intentionally run more than once (sweep, then re-force the
            // discovered gate). A client-cache hit would skip server execution, so the plan would never run and
            // the timings would report Taken=False — defeating the whole point. Force a real execution every call.
            IRawDocumentQuery<Item> q = session.Advanced.RawQuery<Item>(Rql + " include timings()")
                .NoCaching()
                .Timings(out QueryTimings timings)
                .AddParameter("cat", category)
                .AddParameter("st", "active");
            if (force.HasValue)
                q.AddParameter("rvn_corax_entry_scan", force.Value);

            List<Item> results = q.ToList();

            var plan = (QueryInspectionNode)timings.QueryPlan;
            QueryInspectionNode entryScan = FindNode(plan, "EntryScan");
            int entryScanAt = -1;
            if (entryScan?.Parameters != null
                && entryScan.Parameters.TryGetValue("Taken", out string taken)
                && string.Equals(taken, "True", StringComparison.OrdinalIgnoreCase)
                && entryScan.Parameters.TryGetValue("SwitchedAfterClauses", out string s))
            {
                int.TryParse(s, out entryScanAt);
            }

            return (results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToList(), entryScanAt);
        }

        // Forcing a real gate op-index makes the plan report EntryScanAt == that index; forcing a non-gate
        // index is a no-op (EntryScanAt stays -1). Sweeping low indices finds the gate either way, and every
        // run must return the supplied baseline documents.
        private static int DiscoverGate(IDocumentStore store, string category, List<string> baselineIds)
        {
            for (int f = 0; f <= 15; f++)
            {
                (List<string> Ids, int EntryScanAt) forced = Run(store, category, force: f);
                AssertSameIds(baselineIds, forced.Ids, forced.EntryScanAt);
                if (forced.EntryScanAt == f)
                    return f;
                Assert.Equal(-1, forced.EntryScanAt);
            }

            return -1;
        }

        private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
        {
            if (node == null)
                return null;
            if (node.Operation == operation)
                return node;
            if (node.Children != null)
            {
                foreach (QueryInspectionNode child in node.Children)
                {
                    QueryInspectionNode found = FindNode(child, operation);
                    if (found != null)
                        return found;
                }
            }

            return null;
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void ForceRunsScan_DisableSuppressesIt_AllMatchBaseline()
        {
            using IDocumentStore store = GetSeededStore();

            // -1 disables every gate: pure bitmap pipeline, no scan reported. This is the correctness baseline.
            (List<string> Ids, int EntryScanAt) rareBaseline = Run(store, RareCategory, force: -1);
            Assert.Equal(-1, rareBaseline.EntryScanAt);
            Assert.NotEmpty(rareBaseline.Ids);

            int gate = DiscoverGate(store, RareCategory, rareBaseline.Ids);
            Assert.True(gate >= 0, "expected to find an entry-scan gate by sweeping op-indices 0..15");

            // Forcing the gate op-index runs the scan there over the small (rare) candidate set; same documents.
            (List<string> Ids, int EntryScanAt) rareForced = Run(store, RareCategory, force: gate);
            Assert.Equal(gate, rareForced.EntryScanAt);
            AssertSameIds(rareBaseline.Ids, rareForced.Ids, rareForced.EntryScanAt);

            // Same RQL text with a common (large) category reuses the cached plan, so the gate is at the same
            // op-index. Forcing it runs the scan over the larger candidate set; still the baseline documents.
            (List<string> Ids, int EntryScanAt) commonBaseline = Run(store, CommonCategory, force: -1);
            Assert.Equal(-1, commonBaseline.EntryScanAt);
            Assert.NotEmpty(commonBaseline.Ids);

            (List<string> Ids, int EntryScanAt) commonForced = Run(store, CommonCategory, force: gate);
            Assert.Equal(gate, commonForced.EntryScanAt);
            AssertSameIds(commonBaseline.Ids, commonForced.Ids, commonForced.EntryScanAt);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void ForcingNonGateIndex_IsGracefulNoOp()
        {
            using IDocumentStore store = GetSeededStore();

            (List<string> Ids, int EntryScanAt) baseline = Run(store, RareCategory, force: -1);
            Assert.Equal(-1, baseline.EntryScanAt);
            Assert.NotEmpty(baseline.Ids);

            // An op-index that no gate occupies matches no gate, so no scan is taken and the result is
            // unchanged — the override fails safe rather than corrupting the query.
            (List<string> Ids, int EntryScanAt) forced = Run(store, RareCategory, force: 99_999);
            Assert.Equal(-1, forced.EntryScanAt);
            AssertSameIds(baseline.Ids, forced.Ids, forced.EntryScanAt);
        }

        private static void AssertSameIds(List<string> expected, List<string> actual, int entryScanAt)
        {
            List<string> missing = expected.Except(actual).ToList();
            List<string> extra = actual.Except(expected).ToList();
            Assert.True(missing.Count == 0 && extra.Count == 0,
                $"EntryScanAt={entryScanAt} expected={expected.Count} actual={actual.Count}; " +
                $"missing(in baseline, not actual)=[{string.Join(",", missing.Take(15))}] (total {missing.Count}); " +
                $"extra(in actual, not baseline)=[{string.Join(",", extra.Take(15))}] (total {extra.Count})");
        }
    }
}
