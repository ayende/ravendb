//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using Raven.Abstractions.Data;
//using Raven.Abstractions.Replication;
//using Raven.Bundles.Replication.Tasks;
//using Raven.Client;
//using Raven.Client.Document;
//using Raven.Database;
//using Raven.Database.Config;
//using Raven.Json.Linq;
//using Raven.Server;
//using Raven.Tests.Core.Replication;
//using Raven.Tests.Helpers;
//using Xunit;

//namespace Raven.Tests.Issues
//{
//	public class RavenDB_4750 : RavenTestBase
//	{
//		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
//		{
//			configuration.Settings["Raven/ActiveBundles"] = "Replication";
//		}
//NOT Finished, WIP
//		[Fact]
//		public async Task Three_node_cluster_should_not_go_to_infinite_loop()
//		{
//			using (var r1Server = GetNewServer(8077))
//			using (var r1 = NewRemoteDocumentStore(ravenDbServer: r1Server))
//			using (var r2Server = GetNewServer(8078))
//			using (var r2 = NewRemoteDocumentStore(ravenDbServer: r2Server))
//			using (var r3Server = GetNewServer())
//			using (var r3 = NewRemoteDocumentStore(ravenDbServer: r3Server))
//			{
//				CreateDatabaseWithReplication(r1, "testDB");
//				CreateDatabaseWithReplication(r2, "testDB");
//				CreateDatabaseWithReplication(r3, "testDB");

//				var replTask1 = await GetReplicationTask(r1Server, "testDB");
//				var replTask2 = await GetReplicationTask(r2Server, "testDB");
//				var replTask3 = await GetReplicationTask(r3Server, "testDB");

                
//			}
//		}

//		private static async Task<ReplicationTask> GetReplicationTask(RavenDbServer server, string dbName)
//		{
//			var db = await server.Server.GetDatabaseInternal(dbName);
//			return db.StartupTasks.OfType<ReplicationTask>().First();
//		}

//		private static void SetupReplication(IDocumentStore source, string databaseName, params IDocumentStore[] destinations)
//		{
//			source
//				.DatabaseCommands
//				.ForDatabase(databaseName)
//				.Put(
//					Constants.RavenReplicationDestinations,
//					null,
//					RavenJObject.FromObject(new ReplicationDocument
//					{
//						Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
//							new ReplicationDestination
//							{
//								Database = databaseName,
//								Url = destination.Url
//							}))

//					}),
//					new RavenJObject());
//		}

//		private static List<ReplicationDestination> SetupReplication(IDocumentStore source, string databaseName, Func<IDocumentStore, bool> shouldSkipIndexReplication, params IDocumentStore[] destinations)
//		{
//			var replicationDocument = new ReplicationDocument
//			{
//				Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
//					new ReplicationDestination
//					{
//						Database = databaseName,
//						Url = destination.Url,
//						SkipIndexReplication = shouldSkipIndexReplication(destination)
//					}))
//			};

//			using (var session = source.OpenSession(databaseName))
//			{
//				session.Store(replicationDocument, Constants.RavenReplicationDestinations);
//				session.SaveChanges();
//			}

//			return replicationDocument.Destinations;
//		}


//		private static void CreateDatabaseWithReplication(DocumentStore store, string databaseName)
//		{
//			store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
//			{
//				Id = databaseName,
//				Settings =
//				{
//					{"Raven/DataDir", "~/Tenants/" + databaseName},
//					{"Raven/ActiveBundles", "Replication"}
//				}
//			});
//		}
//	}
//}
