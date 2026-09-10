using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Config;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12725_Raven : RavenTestBase
    {
        public RavenDB_12725_Raven(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public async Task CanLoadDatabaseAndIgnoreMissingJournals()
        {
            UseNewLocalServer(new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors), "true"},
                // a small environment is otherwise flushed at most once per 30s, which would gate the wait below
                {RavenConfiguration.GetKey(x => x.Storage.TimeToSyncAfterFlush), "1"}
            });

            var path = NewDataPath();

            DocumentDatabase db;

            using (var store = GetDocumentStore(new Options()
            {
                Path = path
            }))
            {
                db = await GetDatabase(store.Database);
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                // Deleting a journal is only safe once its transactions are in the data file and synced:
                // journal entries are page DIFFS, so replaying the later journals over a data file that
                // lacks the deleted journal's pages produces checksum failures on load. Dispose does not
                // flush, and the background flusher may legitimately defer a small environment (throttle,
                // yielding to journal writes on a slow disk), so the premise has to be made explicit.
                await WaitForJournalsToBeSyncedAsync(store, db, path);
            }

            db.Dispose();

            var journalPath = Path.Combine(path, "Journals");

            var firstJournal = new DirectoryInfo(journalPath).GetFiles("*.journal").OrderBy(x => x.Name).First();

            File.Delete(firstJournal.FullName);

            using (GetDocumentStore(new Options()
            {
                Path = path
            }))
            {
            }
        }

        private static async Task WaitForJournalsToBeSyncedAsync(DocumentStore store, DocumentDatabase db, string path)
        {
            var env = db.DocumentsStorage.Environment;
            var journalPath = Path.Combine(path, "Journals");
            var sw = Stopwatch.StartNew();
            long lastSynced = -1, highest = -1;
            for (var i = 0; sw.Elapsed < TimeSpan.FromSeconds(60); i++)
            {
                lastSynced = env.Journal.GetCurrentJournalInfo().LastSyncedJournal;
                highest = new DirectoryInfo(journalPath).GetFiles("*.journal")
                    .Select(f => long.Parse(Path.GetFileNameWithoutExtension(f.Name)))
                    .DefaultIfEmpty(-1)
                    .Max();
                if (lastSynced >= highest)
                    return;

                // a commit is what asks the flusher to apply the journals; the sync follows the flush
                await store.Commands().PutAsync("sync-markers/" + i, null, new { Marker = i });
                env.ForceSyncDataFile();
                await Task.Delay(250);
            }

            throw new TimeoutException($"journals not synced within 60s: LastSyncedJournal={lastSynced}, highest journal on disk={highest}");
        }
    }
}
