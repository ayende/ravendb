﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_14303 : ClusterTestBase
    {

        [Fact]
        public async Task IgnoreIdleIndexOnPromotable()
        {
            var clusterSize = 3;
            var (_, leader) = await CreateRaftCluster(clusterSize);
            var replicationFactor = 2;
            var databaseName = GetDatabaseName();
            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var createRes = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, replicationFactor));

                var dbServer = Servers.First(s => createRes.NodesAddedTo.Contains(s.WebUrl));
                await dbServer.ServerStore.Cluster.WaitForIndexNotification(createRes.RaftCommandIndex);
                Random rnd = new Random();
                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" , Count = i, Age = rnd.Next(1,100)}, "users/" + i);
                        await session.SaveChangesAsync();
                    }
                }

                var count = new AutoIndexField
                {
                    Name = "Count",
                };

                var age = new AutoIndexField
                {
                    Name = "Age",
                };

                var sum = new AutoIndexField
                {
                    Name = "Sum",
                };

                var db = await dbServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                var index = await db.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition("Users", new[] { count, sum }, new[] { age }), Guid.NewGuid().ToString());

                await leader.ServerStore.Engine.PutAsync(new SetIndexStateCommand(index.Name, IndexState.Idle, db.Name, Guid.NewGuid().ToString()));
                dbServer = Servers.First(s => !createRes.NodesAddedTo.Contains(s.WebUrl));
                dbServer.ServerStore.ForTestingPurposes = new ServerStore.TestingStuff {stopIndex = true};

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                WaitForUserToContinueTheTest(store);
                var res = await WaitForValueAsync(async () =>
                {
                    var membersCount = await GetMembersCount(store, db.Name);
                    return membersCount == 3;
                }, true, 5000);
                Assert.True(res);
            }
        }
        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }


        public RavenDB_14303(ITestOutputHelper output) : base(output)
        {
        }
    }
}
