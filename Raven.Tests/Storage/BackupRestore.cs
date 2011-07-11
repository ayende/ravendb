//-----------------------------------------------------------------------
// <copyright file="BackupRestore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Json;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class BackupRestore : AbstractDocumentStorageTest
	{
		private DocumentDatabase db;

		public BackupRestore()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
			});
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		private void DeleteIfExists(string DirectoryName)
		{
			string directoryFullName = null;

			if (Path.IsPathRooted(DirectoryName) == false)
				directoryFullName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, DirectoryName);
			else
				directoryFullName = DirectoryName;

			IOExtensions.DeleteDirectory(directoryFullName);
		}
		
		[Fact]
		public void AfterBackupRestoreCanReadDocument()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), new RavenJObject(), null);

			db.StartBackup("raven.db.test.backup");
			WaitForBackup(true);

			db.Dispose();

			DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration(), "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent"});

			var jObject = db.Get("ayende", null).ToJson();
			Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
		}

		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedAfterRestore()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);

			db.StartBackup("raven.db.test.backup");
			WaitForBackup(true);

			db.Dispose();

			DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration(), "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent" });
			db.SpinBackgroundWorkers();
			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Users]]",
					PageSize = 10
				});
			} while (queryResult.IsStale);
			Assert.Equal(1, queryResult.Results.Count);
		}

		[Fact]
		public void AfterBackupRestoreCanQueryIndex_CreatedBeforeRestore()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
			db.SpinBackgroundWorkers();
			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Users]]",
					PageSize = 10
				});
			} while (queryResult.IsStale);
			Assert.Equal(1, queryResult.Results.Count);

			db.StartBackup("raven.db.test.backup");
			WaitForBackup(true);

			db.Dispose();

			DeleteIfExists("raven.db.test.esent");

			DocumentDatabase.Restore(new RavenConfiguration(), "raven.db.test.backup", "raven.db.test.esent");

			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent" });

			queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				Query = "Tag:[[Users]]",
				PageSize = 10
			});
			Assert.Equal(1, queryResult.Results.Count);
		}

		[Fact]
		public void AfterFailedBackupRestoreCanDetectError()
		{
			db.Put("ayende", null, RavenJObject.Parse("{'email':'ayende@ayende.com'}"), RavenJObject.Parse("{'Raven-Entity-Name':'Users'}"), null);
			db.SpinBackgroundWorkers();
			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Users]]",
					PageSize = 10
				});
			} while (queryResult.IsStale);
			Assert.Equal(1, queryResult.Results.Count);


			File.WriteAllText("raven.db.test.backup.txt", "Sabotage!");
			db.StartBackup("raven.db.test.backup.txt");
			WaitForBackup(false);

			Assert.True(GetStateOfLastStatusMessage().Severity == BackupStatus.BackupMessageSeverity.Error);
		}

		private BackupStatus.BackupMessage GetStateOfLastStatusMessage()
		{
			JsonDocument jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
			var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
			return backupStatus.Messages.OrderByDescending(m => m.Timestamp).First();
		}

		private void WaitForBackup(bool checkError)
		{
			while (true)
			{
				var jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					break;
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
				{
					if (checkError)
					{
						Assert.False(backupStatus.Messages.Any(x => x.Severity == BackupStatus.BackupMessageSeverity.Error));
					}

					return;
				}
				Thread.Sleep(50);
			}
		}
	}
}
