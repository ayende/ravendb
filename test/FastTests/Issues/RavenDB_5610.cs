using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using System.Linq;
using System.Threading;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5610 : RavenTestBase
    {
        [Fact]
        public void UpdateType()
        {
            using (var database = CreateDocumentDatabase())
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                Assert.Equal(0, database.IndexStore.CreateIndex(new IndexLocalizedData(indexDefinition, 0, database)));

                var index = database.IndexStore.GetIndexes().First();

                var options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Noop, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";
                
                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Update, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Update, options);
            }
        }

        [Fact]
        public void WillUpdate()
        {
            var path = NewDataPath();
            using (var server = GetNewServer(runInMemory: false, partialPath: "CanPersist"))
            using (var store = GetDocumentStore(modifyName: x => "CanPersistDB", defaultServer: server, deleteDatabaseWhenDisposed: false, modifyDatabaseRecord: x => x.Settings["Raven/RunInMemory"] = "False"))
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                store.Admin.Send(new PutIndexesOperation(indexDefinition));                

                var index = store.Admin.Send(new GetIndexOperation(indexDefinition.Name));
                Assert.Equal("33", index.Configuration["Raven/Indexing/MapTimeoutInSec"]);

                indexDefinition = CreateIndexDefinition();
                
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                                      
                index = store.Admin.Send(new GetIndexesOperation(0,10)).Last();

                Assert.Equal("30", index.Configuration["Raven/Indexing/MapTimeoutInSec"]);
                
            }

            using (var server = GetNewServer(runInMemory: false, deletePrevious: false, partialPath: "CanPersist"))
            {
                var database = AsyncHelpers.RunSync(() => server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("CanPersistDB"));
                var index = database.IndexStore.GetIndexes().First();
                Assert.Equal(30, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);
            }
        }

        private static IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Name = "Users_ByName",
                Maps = { "from user in docs.Users select new { user.Name }" },
                Type = IndexType.Map
            };
        }
    }
}