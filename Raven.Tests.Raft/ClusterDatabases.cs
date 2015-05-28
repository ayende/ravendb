﻿// -----------------------------------------------------------------------
//  <copyright file="ClusterDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Http;
using System.Threading.Tasks;

using Rachis.Transport;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft
{
	public class ClusterDatabases : RaftTestBase
	{
		[Fact]
		public async Task CanCreateClusterWhenThereAreNoDatabasesOnServer()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var databases = store.DatabaseCommands.ForSystemDatabase().StartsWith(Constants.Database.Prefix, null, 0, 1024);
				foreach (var database in databases)
					store.DatabaseCommands.GlobalAdmin.DeleteDatabase(database.Key.Substring(Constants.Database.Prefix.Length));

				var request = store.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/create", HttpMethod.Post);
				await request.WriteAsync(RavenJObject.FromObject(new NodeConnectionInfo()));
			}
		}

		[Fact]
		public async Task CannotCreateClusterWhenThereAreAnyDatabasesOnServer()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var request = store.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/create", HttpMethod.Post);
				var e = await AssertAsync.Throws<ErrorResponseException>(() => request.WriteAsync(RavenJObject.FromObject(new NodeConnectionInfo())));

				Assert.Equal("To create a cluster server must not contain any databases.", e.Message);
			}
		}

		[Theory]
		[PropertyData("Nodes")]
		public void DatabaseShouldBeCreatedOnAllNodes(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes);

			using (var store1 = clusterStores[0])
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
																   {
																	   Id = "Northwind",
																	   Settings =
					                                                   {
						                                                   {"Raven/DataDir", "~/Databases/Northwind"}
					                                                   }
																   });

				var key = Constants.Database.Prefix + "Northwind";

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));
			}
		}

		[Theory]
		[PropertyData("Nodes")]
		public void DatabaseShouldBeDeletedOnAllNodes(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes);

			using (var store1 = clusterStores[0])
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Northwind",
					Settings =
					{
						{"Raven/DataDir", "~/Databases/Northwind"},
						{Constants.Cluster.NonClusterDatabaseMarker, "false"}
					}
				});

				var key = Constants.Database.Prefix + "Northwind";

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));

				store1.DatabaseCommands.GlobalAdmin.DeleteDatabase(key);

				clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands.ForSystemDatabase(), key));
			}
		}

		[Fact]
		public void NonClusterDatabasesShouldNotBeCreatedOnAllNodes()
		{
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Northwind",
					Settings =
					{
						{"Raven/DataDir", "~/Databases/Northwind"},
						{Constants.Cluster.NonClusterDatabaseMarker, "true"}
					}
				});

				var key = Constants.Database.Prefix + "Northwind";

				Assert.NotNull(store1.DatabaseCommands.ForSystemDatabase().Get(key));

				var e = Assert.Throws<Exception>(() => WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), key, TimeSpan.FromSeconds(10)));
				Assert.Equal("WaitForDocument failed", e.Message);

				e = Assert.Throws<Exception>(() => WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), key, TimeSpan.FromSeconds(10)));
				Assert.Equal("WaitForDocument failed", e.Message);
			}
		}
	}
}