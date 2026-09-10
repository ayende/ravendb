using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Voron.Impl.Journal;
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
                {RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors), "true"}
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
                // lacks the deleted journal's pages fails the recovery checksum validation. Dispose does
                // not flush, and the background flusher may legitimately defer a small environment, so
                // flush and sync explicitly instead of relying on its timing.
                var env = db.DocumentsStorage.Environment;
                env.Journal.Applicator.ApplyLogsToDataFile(CancellationToken.None, TimeSpan.FromSeconds(30));
                using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                    sync.SyncDataFile();
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
    }
}
