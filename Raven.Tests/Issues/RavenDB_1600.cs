﻿// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1600.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.Collections.Specialized;
	using System.Linq;
	using Client.Indexes;
	using Client.Linq;
	using Database;
	using Database.Config;
	using Database.Extensions;
	using Database.Queries;
	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Json.Linq;
	using Xunit;

	public class RavenDB_1600 : RavenTest
	{
		private readonly string DataDir;
		private readonly string BackupDir;

		public RavenDB_1600()
		{
			DataDir = NewDataPath("RavenDB_1600_Data");
			BackupDir = NewDataPath("RavenDB_1600_Backup");
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			base.ModifyConfiguration(configuration);
			configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
		}

		[Fact]
		public void ShouldNotSetAutoIndexesToAbandonedPriorityAfterDatabaseRecovery()
		{
			using (var db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
			}))
			{
				db.SpinBackgroundWorkers();
				db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());

				db.Put("users/1", null, RavenJObject.Parse("{'Name':'Arek'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
				db.Put("users/2", null, RavenJObject.Parse("{'Name':'David'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

				var results = db.ExecuteDynamicQuery("Users", new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Name:Arek"
				});

				WaitForIndexing(db);

				var autoIdexes = db.Statistics.Indexes.Where(x => x.Name.StartsWith("Auto")).ToList();

				Assert.True(autoIdexes.Count > 0);

				autoIdexes.ForEach(x => db.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexPriority(x.Name, IndexingPriority.Idle)));
				
				db.StartBackup(BackupDir, false, new DatabaseDocument());
				WaitForBackup(db, true);
			}
			IOExtensions.DeleteDirectory(DataDir);

			DocumentDatabase.Restore(new RavenConfiguration(), BackupDir, DataDir, s => { }, defrag: true);

			using (var db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
			}))
			{
				db.SpinBackgroundWorkers();
				db.RunIdleOperations();

				var autoIndexes = db.Statistics.Indexes.Where(x => x.Name.StartsWith("Auto")).ToList();

				Assert.True(autoIndexes.Count > 0);

				foreach (var indexStats in autoIndexes)
				{
					Assert.NotEqual(indexStats.Priority, IndexingPriority.Abandoned);
				}
			}
		}
	}
}