using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

/// <summary>
/// Pipelined commits for indexes: the root's merged batches are submitted rather than written inline, and an
/// index hands a batch's journal write off to run in the background while the next batch executes. Both make
/// a batch's results become visible later than the code that produced them, which is what these cover.
/// </summary>
public class RavenDB_27407 : RavenTestBase
{
    public RavenDB_27407(ITestOutputHelper output) : base(output)
    {
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from user in users select new { user.Name };
        }
    }

    private class Users_ByAge : AbstractIndexCreationTask<User>
    {
        public Users_ByAge()
        {
            Map = users => from user in users select new { user.Age };
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }

    private const int DocumentCount = 2_000;

    private static void Seed(IDocumentStore store)
    {
        using var bulk = store.BulkInsert();
        for (int i = 0; i < DocumentCount; i++)
            bulk.Store(new User { Name = $"user/{i}", Age = i % 97 });
    }

    /// <summary>
    /// A small map batch size forces many batches, so most of them commit with the chain open and are only
    /// published when the following batch starts. If the batch-completed signal were still raised before the
    /// transaction is public, waiting for non-stale results here would return early and the count would be short.
    /// </summary>
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task PipelinedIndexCommitsProduceCompleteResults()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128"
        });

        await new Users_ByName().ExecuteAsync(store);
        Seed(store);

        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            var count = await session.Query<User, Users_ByName>().CountAsync();
            Assert.Equal(DocumentCount, count);
        }

        Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));
    }

    /// <summary>
    /// Several indexes commit into the same merged batch through the shared journal. With the root submitting
    /// batches instead of writing them inline, and branches released once their entry has been taken rather
    /// than once it is durable, a single batch can now carry more than one entry from the same environment.
    /// </summary>
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task SeveralIndexesShareMergedBatches()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128"
        });

        await new Users_ByName().ExecuteAsync(store);
        await new Users_ByAge().ExecuteAsync(store);
        Seed(store);

        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(DocumentCount, await session.Query<User, Users_ByName>().CountAsync());
            Assert.Equal(DocumentCount, await session.Query<User, Users_ByAge>().CountAsync());
        }
    }

    /// <summary>
    /// Restarting replays the journals the pipelined path produced. A batch that was async-committed but whose
    /// write never landed must not leave the index claiming work it cannot show.
    /// </summary>
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task IndexSurvivesRestartAfterPipelinedCommits()
    {
        var path = NewDataPath();

        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            Path = path,
            ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128"
        });

        await new Users_ByName().ExecuteAsync(store);
        Seed(store);
        await Indexes.WaitForIndexingAsync(store);

        Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            var names = await session.Query<User, Users_ByName>()
                .Select(u => u.Name)
                .Take(DocumentCount)
                .ToListAsync();

            Assert.Equal(DocumentCount, names.Count);
            Assert.Equal(DocumentCount, new HashSet<string>(names).Count);
        }
    }

    /// <summary>
    /// The chain leaves a write transaction open across batches, and the environment refuses to dispose while
    /// one is live. Deleting the database mid-indexing has to drain it rather than time out.
    /// </summary>
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task CanDisposeWhileTheCommitChainIsOpen()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128"
        });

        await new Users_ByName().ExecuteAsync(store);
        Seed(store);

        // deliberately not waiting for indexing - we want the chain open when the database goes away
        var database = await GetDatabase(store.Database);
        Assert.NotNull(database);

        Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

        // reloads and finishes on its own
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
            Assert.Equal(DocumentCount, await session.Query<User, Users_ByName>().CountAsync());
    }
}
