﻿// -----------------------------------------------------------------------
//  <copyright file="Documents.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Cluster;
using Raven.Bundles.Replication.Tasks;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
	public class Documents : RaftTestBase
	{
		[Theory]
		[PropertyData("Nodes")]
		public void CanReadFromMultipleServers1(int numberOfNodes)
		{
			CanReadFromMultipleServersInternal(numberOfNodes, ClusterBehavior.ReadFromAllWriteToLeader);
		}

		[Theory]
		[PropertyData("Nodes")]
		public void CanReadFromMultipleServers2(int numberOfNodes)
		{
			CanReadFromMultipleServersInternal(numberOfNodes, ClusterBehavior.ReadFromAllWriteToLeaderWithFailovers);
		}

		private void CanReadFromMultipleServersInternal(int numberOfNodes, ClusterBehavior clusterBehavior)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = clusterBehavior);

			SetupClusterConfiguration(clusterStores);

			clusterStores[0].DatabaseCommands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
			clusterStores[0].DatabaseCommands.Put("keys/2", null, new RavenJObject(), new RavenJObject());
			clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/2"));

			var tasks = new List<ReplicationTask>();
			foreach (var server in servers)
			{
				server.Options.DatabaseLandlord.ForAllDatabases(database => tasks.Add(database.StartupTasks.OfType<ReplicationTask>().First()));
				server.Options.ClusterManager.Value.Engine.Dispose();
			}

			foreach (var task in tasks)
			{
				task.Pause();
				SpinWait.SpinUntil(() => task.IsRunning == false, TimeSpan.FromSeconds(3));
			}

			servers.ForEach(server => server.Options.RequestManager.ResetNumberOfRequests());

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Get("keys/1");
				store.DatabaseCommands.Get("keys/2");
			}

			servers.ForEach(server => Assert.True(server.Options.RequestManager.NumberOfRequests > 0));
		}

		[Theory]
		[PropertyData("Nodes")]
		public void PutShouldBePropagated(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			SetupClusterConfiguration(clusterStores);

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/" + i));
			}
		}

		[Theory]
		[PropertyData("Nodes")]
		public void DeleteShouldBePropagated(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			SetupClusterConfiguration(clusterStores);

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/" + i));
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Delete("keys/" + i, null);
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/" + i));
			}
		}
	}
}