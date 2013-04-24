//-----------------------------------------------------------------------
// <copyright file="BackupOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Munin;

namespace Raven.Storage.Managed.Backup
{
	public class BackupOperation
	{
		private readonly DocumentDatabase database;
		private readonly IPersistentSource persistentSource;
		private string to;
		private readonly DatabaseDocument databaseDocument;
		private string src;

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public BackupOperation(DocumentDatabase database, IPersistentSource persistentSource, string src, string to, DatabaseDocument databaseDocument)
		{
			this.database = database;
			this.persistentSource = persistentSource;
			this.src = src;
			this.to = to;
			this.databaseDocument = databaseDocument;
		}

		public void Execute()
		{
			try
			{
				to = to.ToFullPath();
				src = src.ToFullPath();
				logger.Info("Starting backup of '{0}' to '{1}'", src, to);
				var directoryBackups = new List<DirectoryBackup>
				{
					new DirectoryBackup(src, to, Path.Combine("TempData" + Guid.NewGuid().ToString("N")), false),
					new DirectoryBackup(Path.Combine(src, "IndexDefinitions"), Path.Combine(to, "IndexDefinitions"),
										Path.Combine(src, "Temp" + Guid.NewGuid().ToString("N")), false)
				};

				database.IndexStorage.Backup(to);

				persistentSource.Read(log =>
				{
					persistentSource.FlushLog();

					foreach (var directoryBackup in directoryBackups)
					{
						directoryBackup.Notify += UpdateBackupStatus;
						directoryBackup.Prepare();
					}

					foreach (var directoryBackup in directoryBackups)
					{
						directoryBackup.Execute();
					}

					return 0;// ignored
				});
			}
			catch (Exception e)
			{
				logger.ErrorException("Failed to complete backup", e);
				UpdateBackupStatus("Failed to complete backup because: " + e.Message, BackupStatus.BackupMessageSeverity.Error);
			}
			finally
			{
				CompleteBackup();
			}
		}

		private void CompleteBackup()
		{
			try
			{
				if (databaseDocument != null)
					File.WriteAllText(Path.Combine(to, "Database.Document"), RavenJObject.FromObject(databaseDocument).ToString());

				logger.Info("Backup completed");
				var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					return;

				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				backupStatus.IsRunning = false;
				backupStatus.Completed = SystemTime.UtcNow;
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
							 jsonDocument.Metadata,
							 null);
			}
			catch (Exception e)
			{
				logger.WarnException("Failed to update completed backup status, will try deleting document", e);
				try
				{
					database.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
				}
				catch (Exception ex)
				{
					logger.WarnException("Failed to remove out of date backup status", ex);
				}
			}
		}

		private void UpdateBackupStatus(string newMsg, BackupStatus.BackupMessageSeverity severity)
		{
			try
			{
				logger.Info(newMsg);
				var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if(jsonDocument==null)
					return;
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				backupStatus.Messages.Add(new BackupStatus.BackupMessage
				{
					Message = newMsg,
					Timestamp = SystemTime.UtcNow,
					Severity = severity
				});
				database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus), jsonDocument.Metadata,
							 null);
			}
			catch (Exception e)
			{
				logger.WarnException("Failed to update backup status", e);
			}
		}
	}
}