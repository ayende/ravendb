// -----------------------------------------------------------------------
//  <copyright file="MaintenanceActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

using Voron.Impl.Backup;

namespace Raven.Database.Actions
{
    public class MaintenanceActions : ActionsBase
    {
        public MaintenanceActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        internal static string FindDatabaseDocument(string rootBackupPath)
        {
            // try to find newest database document in incremental backups first - to have the most recent version (if available)

            var backupPath = Directory.GetDirectories(rootBackupPath, "Inc*")
                                       .OrderByDescending(dir => dir)
                                       .Select(dir => Path.Combine(dir, Constants.DatabaseDocumentFilename))
                                       .FirstOrDefault();

            return backupPath ?? Path.Combine(rootBackupPath, Constants.DatabaseDocumentFilename);
        }

        public static void Restore(RavenConfiguration configuration, DatabaseRestoreRequest restoreRequest, Action<string> output)
        {
            var databaseDocumentPath = FindDatabaseDocument(restoreRequest.BackupLocation);
            if (File.Exists(databaseDocumentPath) == false)
            {
                throw new InvalidOperationException("Cannot restore when the Database.Document file is missing in the backup folder: " + restoreRequest.BackupLocation);
            }

            var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
            var databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();

            string storage;
            if (databaseDocument.Settings.TryGetValue("Raven/StorageTypeName", out storage) == false)
            {
                if (File.Exists(Path.Combine(restoreRequest.BackupLocation, BackupMethods.Filename))) 
                    storage = InMemoryRavenConfiguration.VoronTypeName;
                else if (Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")))
                    storage = InMemoryRavenConfiguration.EsentTypeName;
                else
                    storage = InMemoryRavenConfiguration.EsentTypeName;
            }

            if (!string.IsNullOrWhiteSpace(restoreRequest.DatabaseLocation))
            {
                configuration.DataDirectory = restoreRequest.DatabaseLocation;
            }

            using (var transactionalStorage = configuration.CreateTransactionalStorage(storage, () => { }, () => { }))
            {
                transactionalStorage.Restore(restoreRequest, output);
            }
        }

        public void StartBackup(string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument databaseDocument)
        {
            if (databaseDocument == null) throw new ArgumentNullException("databaseDocument");
            var document = Database.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
            if (document != null)
            {
                var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning)
                {
                    throw new InvalidOperationException("Backup is already running");
                }
            }

            bool enableIncrementalBackup;
            if (incrementalBackup &&
                TransactionalStorage is Raven.Storage.Esent.TransactionalStorage &&
                (bool.TryParse(Database.Configuration.Settings[Constants.Esent.CircularLog], out enableIncrementalBackup) == false || enableIncrementalBackup))
            {
                throw new InvalidOperationException("In order to run incremental backups using Esent you must have circular logging disabled");
            }

            if (incrementalBackup &&
                TransactionalStorage is Raven.Storage.Voron.TransactionalStorage &&
                Database.Configuration.Storage.Voron.AllowIncrementalBackups == false)
            {
                throw new InvalidOperationException("In order to run incremental backups using Voron you must have the appropriate setting key (Raven/Voron/AllowIncrementalBackups) set to true");
            }

            Database.Documents.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(new BackupStatus
            {
                Started = SystemTime.UtcNow,
                IsRunning = true,
            }), new RavenJObject(), null);
            Database.IndexStorage.FlushMapIndexes();
            Database.IndexStorage.FlushReduceIndexes();

            if (databaseDocument.Settings.ContainsKey("Raven/StorageTypeName") == false)
                databaseDocument.Settings["Raven/StorageTypeName"] = TransactionalStorage.FriendlyName ?? TransactionalStorage.GetType().AssemblyQualifiedName;

            TransactionalStorage.StartBackupOperation(Database, backupDestinationDirectory, incrementalBackup, databaseDocument);
        }

        public void RemoveAllBefore(string listName,Etag etag)
        {
            TransactionalStorage.Batch(accessor => accessor.Lists.RemoveAllBefore(listName,etag));
        }

        public void PurgeOutdatedTombstones()
        {
            var tomstoneLists = new[]
            {
                Constants.RavenPeriodicExportsAttachmentsTombstones,
                Constants.RavenPeriodicExportsDocsTombstones,
                Constants.RavenReplicationAttachmentsTombstones,
                Constants.RavenReplicationDocsTombstones
            };

            var olderThan = SystemTime.UtcNow.Subtract(Database.Configuration.TombstoneRetentionTime);

            foreach (var listName in tomstoneLists)
            {
                string name = listName;
                TransactionalStorage.Batch(accessor => accessor.Lists.RemoveAllOlderThan(name, olderThan));
            }
        }
        public void DeleteRemovedIndexes(Dictionary<int, DocumentDatabase.IndexFailDetails> reason)
        {
            TransactionalStorage.Batch(actions =>
            {
                foreach (var result in actions.Lists.Read("Raven/Indexes/PendingDeletion", Etag.Empty, null, 100))
                {
                    Database.Indexes.StartDeletingIndexDataAsync(result.Data.Value<int>("IndexId"), result.Data.Value<string>("IndexName"));
                }

                List<int> indexIds = actions.Indexing.GetIndexesStats().Select(x => x.Id).ToList();
                foreach (int id in indexIds)
                {
                    var index = IndexDefinitionStorage.GetIndexDefinition(id);
                    if (index != null)
                        continue;

                    // index is not found on disk, better kill for good
                    // Even though technically we are running into a situation that is considered to be corrupt data
                    // we can safely recover from it by removing the other parts of the index.
                    Database.IndexStorage.DeleteIndex(id);
                    actions.Indexing.DeleteIndex(id, WorkContext.CancellationToken);

                    string indexName;
                    string msg;
                    string ex;

                    DocumentDatabase.IndexFailDetails failDetails;
                    if (reason == null || reason.TryGetValue(id, out failDetails) == false)
                    {
                        indexName = "Unknown Name";
                        msg = string.Format("Index '{0}-({1})' couldn't be found or invalid", id, indexName);
                        ex = "";
                    }
                    else
                    {
                        indexName = failDetails.IndexName;
                        msg = failDetails.Reason;
                        ex = failDetails.Ex.ToString();
                    }

                    Database.AddAlert(new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = SystemTime.UtcNow,
                        Message = msg,
                        Title = string.Format("Index '{0}-({1})' removed because it is not found or invalid", id, indexName),
                        Exception = ex,
                        UniqueKey = msg
                    });
                }
            });
        }
    }
}
