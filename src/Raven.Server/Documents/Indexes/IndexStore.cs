using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStore : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _documentDatabase;
        private readonly ServerStore _serverStore;

        private readonly CollectionOfIndexes _indexes = new CollectionOfIndexes();


        private bool _initialized;

        private bool _run = true;

        public readonly IndexIdentities Identities = new IndexIdentities();
        private readonly object _indexAndTransformerLocker;
        private readonly string _databaseIndexesLocalKey;

        public Logger Logger => _logger;

        public IndexStore(DocumentDatabase documentDatabase, ServerStore serverStore, object indexAndTransformerLocker)
        {
            _documentDatabase = documentDatabase;
            _serverStore = serverStore;
            _indexAndTransformerLocker = indexAndTransformerLocker;
            _logger = LoggingSource.Instance.GetLogger<IndexStore>(_documentDatabase.Name);
            if (serverStore != null)
                serverStore.Cluster.DatabaseChanged += HandleDatabaseRecordChange;

            _databaseIndexesLocalKey = $"db/{_documentDatabase.Name}/indexes";
        }

        private void HandleDatabaseRecordChange(object sender, string changedDatabase)
        {
            var errors = new List<Exception>();
            if (_serverStore == null)
                return;
            if (string.Equals(changedDatabase, _documentDatabase.Name, StringComparison.OrdinalIgnoreCase) == false)
                return;
            DatabaseLocalNodeIndexes localIndexes;
            DatabaseRecord databaseRecord;
            TransactionOperationContext context;
            
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            {
                using (context.OpenReadTransaction())
                {
                    databaseRecord = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name);
                    if (databaseRecord == null)
                        return;

                    var localIndexesJson = _serverStore.Cluster.ReadLocal(context, _databaseIndexesLocalKey);
                    localIndexes = localIndexesJson != null
                        ? JsonDeserializationCluster.DatabaseLocalNodeIndexes(localIndexesJson)
                        : new DatabaseLocalNodeIndexes {Indexes = new Dictionary<string, IndexHolder>()};
                }
                
                lock (_indexAndTransformerLocker)
                {
                    var updatedIndexes = new Dictionary<string, IndexHolder>();

                    // first, delete all indexes that does not show in the database record
                    foreach (var existingIndex in localIndexes.Indexes)
                    {
                        if (databaseRecord.Indexes.ContainsKey(existingIndex.Value.Current.Definition.Name) == false)
                        {
                            DeleteIndex(existingIndex.Value.Current.Definition.Etag);
                            localIndexes.Indexes.Remove(existingIndex.Key);
                        }
                    }

                    // then, go thourhg all index records and figure out which were changed
                    foreach (var definition in databaseRecord.Indexes.Values)
                    {
                        IndexHolder existingDefs;

                        // add new index, if did not exist before
                        //todo: see how we deal here with auto indexes...
                        if (localIndexes.Indexes.TryGetValue(definition.Name, out existingDefs) == false)
                        {
                            var newIndex = new IndexHolder()
                            {
                                Current = new IndexLocalizedData(
                                    definition,0,_documentDatabase)
                                
                            };
                            localIndexes.Indexes[definition.Name] = newIndex;
                            updatedIndexes[definition.Name] = newIndex;
                            continue;
                        }

                        Debug.Assert(existingDefs.Current != null);
                        var indexCreationOptionsForCurrent = 
                            GetIndexCreationOptions(definition, existingDefs.Current.Definition, _documentDatabase);

                        var nextId = existingDefs.GetNextId();
                        // treat case when we have a side-by-side index in progress
                        if (existingDefs.SideBySide != null)
                        {
                            // if received index equals to current, or does not demand recompiling current,
                            // that means we can remove the "side-by-side" one
                            if (indexCreationOptionsForCurrent == IndexCreationOptions.Noop ||
                                indexCreationOptionsForCurrent == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                            {
                                DeleteIndex(existingDefs.SideBySide.Definition.Etag);

                                CollectionOfIndexes.IndexPair indexPair;
                                Debug.Assert(_indexes.TryGetByName(definition.Name, out indexPair));

                                
                                existingDefs.Current = new IndexLocalizedData(definition, nextId, _documentDatabase);
                                existingDefs.SideBySide = null;
                                updatedIndexes[definition.Name] = new IndexHolder
                                {
                                    Current = existingDefs.Current,
                                    SideBySide = null
                                };
                                
                                continue;
                            }

                            // if side by side index equals to received, means there is a change in progress,
                            // and we're fine with that
                            var indexCreationOptionsForSideBySide = 
                                GetIndexCreationOptions(definition, existingDefs.SideBySide.Definition,_documentDatabase);

                            if (indexCreationOptionsForSideBySide == IndexCreationOptions.Noop)
                                continue;

                            existingDefs.SideBySide = new IndexLocalizedData(definition, nextId, _documentDatabase);
                                
                            updatedIndexes[definition.Name] = new IndexHolder()
                            {
                                SideBySide = existingDefs.SideBySide
                            };

                            if (indexCreationOptionsForSideBySide == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                                continue;

                            DeleteIndexDirectory(existingDefs.SideBySide, errors);
                        }
                        else if (indexCreationOptionsForCurrent == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                        {
                            existingDefs.Current.Definition = definition;
                            updatedIndexes[definition.Name] = new IndexHolder
                            {
                                Current = existingDefs.Current
                            };
                        }
                        else if (indexCreationOptionsForCurrent == IndexCreationOptions.Update)
                        {
                            CollectionOfIndexes.IndexPair indexPair;
                            Debug.Assert(_indexes.TryGetByName(definition.Name, out indexPair));
                            
                            existingDefs.SideBySide = 
                                new IndexLocalizedData(definition, nextId, _documentDatabase);
                            updatedIndexes[definition.Name] = new IndexHolder()
                            {
                                SideBySide = existingDefs.SideBySide
                            };

                        }
                    }

                    if (Logger.IsInfoEnabled)
                        Logger.Info("Transformers  configuration changed");

                    // actually store the changes locally
                    foreach (var indexHolder in updatedIndexes.Values)
                    {
                        if (indexHolder.Current!= null)
                            AddIndexInstance(indexHolder.Current, errors);

                        if (indexHolder.SideBySide != null)
                            AddIndexInstance(indexHolder.SideBySide, errors, true);
                    }
                }                

                using (context.OpenWriteTransaction())
                {
                    var afterUpdateLocalState = EntityToBlittable.ConvertEntityToBlittable(localIndexes, DocumentConventions.Default, context);
                    _serverStore.Cluster.WriteLocal(context, _databaseIndexesLocalKey, afterUpdateLocalState);
                    context.Transaction.Commit();
                }

                if (errors.Count == 0)
                    return;

                if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                    throw new AggregateException(errors);

                _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Indexes store initialization error",
                    "Failed to update indexes status",
                    AlertType.IndexStore_IndexCouldNotBeOpened,
                    NotificationSeverity.Error,
                    key: "UpdateLocalStateBasedOnDatabaseRecord",
                    details: new ExceptionDetails(new AggregateException(errors))));
            }

        }

        //private void RenameIndexDirectory(Index existingIndex, IndexDefinition newDefinition)
        //{
        //    if (_documentDatabase.Configuration.Core.RunInMemory)
        //        return;
        //    var newDefinitionConfiguration = new SingleIndexConfiguration(newDefinition.Configuration, _documentDatabase.Configuration);
            
        //    var pathEndOriginal = IndexDefinitionBase.GetIndexPathEndSafeForFileSystem(name: existingIndex.Name, etag: existingIndex.Etag);
        //    var pathEndDestination = IndexDefinitionBase.GetIndexPathEndSafeForFileSystem(name: existingIndex.Name, etag: newDefinition.Etag);
            

        //    using (existingIndex.DrainRunningQueries())
        //    {
        //        existingIndex.Dispose();
        //    }

        //    IOExtensions.MoveDirectory(
        //        src: existingIndex.Configuration.StoragePath.Combine(path: pathEndOriginal).ToFullPath(),
        //        dst: newDefinitionConfiguration.StoragePath.Combine(path: pathEndDestination).ToFullPath()
        //    );
                

        //    if (existingIndex.Configuration.JournalsStoragePath!= null)
        //    {
        //        IOExtensions.MoveDirectory(
        //            src: existingIndex.Configuration.JournalsStoragePath.Combine(path: pathEndOriginal).ToFullPath(),
        //            dst: newDefinitionConfiguration.JournalsStoragePath.Combine(path: pathEndDestination).ToFullPath()
        //        );
        //    }

        //    if (existingIndex.Configuration.TempPath != null)
        //    {
        //        IOExtensions.MoveDirectory(
        //            src: existingIndex.Configuration.TempPath.Combine(path: pathEndOriginal).ToFullPath(),
        //            dst: newDefinitionConfiguration.TempPath.Combine(path: pathEndDestination).ToFullPath()
        //        );
        //    }
        //}

        public Task InitializeAsync()
        {
            lock (_indexAndTransformerLocker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(IndexStore)} was already initialized.");

                TransactionOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    BlittableJsonReaderObject localIndexesJson;
                    DatabaseRecord databaseRecord;
                    DatabaseLocalNodeIndexes localIndexes;
                    using (context.OpenReadTransaction())
                    {
                        localIndexesJson = _serverStore.Cluster.ReadLocal(context, _databaseIndexesLocalKey);
                        localIndexes = localIndexesJson != null
                            ? JsonDeserializationCluster.DatabaseLocalNodeIndexes(localIndexesJson)
                            : new DatabaseLocalNodeIndexes { Indexes = new Dictionary<string, IndexHolder>() };

                        databaseRecord = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name);
                    }
                    if (databaseRecord == null)
                        return Task.CompletedTask;
                    UpdateLocalStateBasedOnDatabaseRecord(localIndexes.Indexes, databaseRecord.Indexes);

                    var afterUpdateLocalState = EntityToBlittable.ConvertEntityToBlittable(localIndexes, DocumentConventions.Default, context);

                    if (localIndexesJson == null || afterUpdateLocalState.Equals(localIndexesJson) == false)
                    {
                        using (context.OpenWriteTransaction())
                        {
                            _serverStore.Cluster.WriteLocal(context, _databaseIndexesLocalKey, afterUpdateLocalState);

                            context.Transaction.Commit();
                        }
                    }
                }

                _initialized = true;
            }

            return Task.Factory.StartNew(OpenIndexes);
        }

        private void UpdateLocalStateBasedOnDatabaseRecord(Dictionary<string, IndexHolder> existingIndexes, Dictionary<string, IndexDefinition> expectedIndexes)
        {
            var errors = new List<Exception>();

            foreach (var name in existingIndexes.Keys.ToArray())
            {
                IndexDefinition value;
                if (expectedIndexes.TryGetValue(name, out value))
                    continue;

                var index = existingIndexes[name];
                existingIndexes.Remove(name);

                // need to delete
                DeleteIndexDirectory(index.Current, errors);
            }

            foreach (var definition in expectedIndexes.Values)
            {
                IndexHolder existingDefs;
                if (existingIndexes.TryGetValue(definition.Name, out existingDefs) == false)
                {
                    existingIndexes[definition.Name] = new IndexHolder
                    {
                        Current = new IndexLocalizedData(definition,0, _documentDatabase)
                    };
                    continue;
                }
                Debug.Assert(existingDefs.Current != null);
                var indexCreationOptionsForCurrent = 
                    IndexStore.GetIndexCreationOptions(definition, existingDefs.Current.Definition, _documentDatabase);
                if (indexCreationOptionsForCurrent == IndexCreationOptions.Noop || indexCreationOptionsForCurrent == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                {
                    DeleteIndexDirectory(existingDefs.SideBySide, errors);
                    existingDefs.SideBySide = null;
                    continue;
                }
                
                if (existingDefs.SideBySide != null)
                {
                    var indexCreationOptionsForSideBySide = 
                        GetIndexCreationOptions(definition, existingDefs.SideBySide.Definition,_documentDatabase);

                    if (indexCreationOptionsForSideBySide == IndexCreationOptions.Noop)
                    {
                        continue;
                    }
                    var nextId = existingDefs.GetNextId();

                    existingDefs.SideBySide = new IndexLocalizedData(definition,nextId, _documentDatabase);

                    if (indexCreationOptionsForSideBySide == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                        continue;

                    DeleteIndexDirectory(existingDefs.SideBySide, errors);
                }
            }

            if (errors.Count == 0)
                return;

            if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                throw new AggregateException(errors);

            _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Indexes store initialization error",
                "Failed to update indexes status",
                AlertType.IndexStore_IndexCouldNotBeOpened,
                NotificationSeverity.Error,
                key: "UpdateLocalStateBasedOnDatabaseRecord",
                details: new ExceptionDetails(new AggregateException(errors))));
        }

        private void DeleteIndexDirectory(IndexLocalizedData localizedData, List<Exception> errors)
        {
            if (localizedData == null)
                return;
            try
            {
                if (localizedData.JournalFinalPath != null)
                {
                    IOExtensions.DeleteDirectory(localizedData.JournalFinalPath);
                }
                if (localizedData.TempFinalPath != null)
                {
                    IOExtensions.DeleteDirectory(localizedData.TempFinalPath);
                }
                if (localizedData.StorageFinalPath != null)
                {
                    IOExtensions.DeleteDirectory(localizedData.StorageFinalPath);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    errors?.Add(e);
                    _logger.Operations($"Unable to delete index {localizedData.Definition.Name} when found that our local state includes it and the database record does not", e);
                }
            }
        }

        public Index GetIndex(string name, bool getCurrent = true)
        {
            CollectionOfIndexes.IndexPair pair;
            if (_indexes.TryGetByName(name, out pair) == false)
                return null;

            return getCurrent?pair.Current:pair.SideBySide;
        }

        public Index GetIndex(long id)
        {
            Index index;
            if (_indexes.TryGetById(id, out index) == false)
                return null;

            return index;
        }

        public long CreateIndex(IndexLocalizedData indexLocalizedData, bool isSideBySide=false, Index existingIndex=null)
        {
            if (indexLocalizedData == null)
                throw new ArgumentNullException(nameof(indexLocalizedData));

            lock (_indexAndTransformerLocker)
            {
                var transformer = _documentDatabase.TransformerStore.GetTransformer(indexLocalizedData.Definition.Name);
                if (transformer != null)
                    throw new IndexOrTransformerAlreadyExistException($"Tried to create an index with a name of {indexLocalizedData.Definition.Name}, but a transformer under the same name exist");

                ValidateIndexName(indexLocalizedData.Definition.Name);
                indexLocalizedData.Definition.RemoveDefaultValues();
                ValidateAnalyzers(indexLocalizedData.Definition);

                var lockMode = IndexLockMode.Unlock;
                var creationOptions = IndexCreationOptions.Create;
                
                if (existingIndex != null)
                {
                    lockMode = existingIndex.Definition.LockMode;
                    creationOptions = GetIndexCreationOptions(indexLocalizedData.Definition, existingIndex);
                }

                if (creationOptions == IndexCreationOptions.Noop)
                {
                    Debug.Assert(existingIndex != null);
                    return existingIndex.Etag;
                }

                if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                {
                    Debug.Assert(existingIndex != null);

                    if (lockMode == IndexLockMode.LockedIgnore)
                        return existingIndex.Etag;

                    if (lockMode == IndexLockMode.LockedError)
                        throw new InvalidOperationException($"Can not overwrite locked index: {existingIndex.Name } should not happen here");

                    UpdateIndex(indexLocalizedData, existingIndex);
                    return existingIndex.Etag;
                }

                if (creationOptions == IndexCreationOptions.Update)
                {
                    Debug.Assert(existingIndex != null);

                    if (lockMode == IndexLockMode.LockedIgnore)
                        return existingIndex.Etag;

                    if (lockMode == IndexLockMode.LockedError)
                        throw new InvalidOperationException($"Can not overwrite locked index: {existingIndex.Name}");

                    var replacementIndexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexLocalizedData.Definition.Name;

                    indexLocalizedData.Definition.Name = replacementIndexName;

                    existingIndex = GetIndex(replacementIndexName);
                    if (existingIndex != null)
                    {
                        creationOptions = GetIndexCreationOptions(indexLocalizedData, existingIndex);
                        if (creationOptions == IndexCreationOptions.Noop)
                            return existingIndex.Etag;

                        if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                        {
                            UpdateIndex(indexLocalizedData, existingIndex);
                            return existingIndex.Etag;
                        }
                    }

                    TryDeleteIndexIfExists(replacementIndexName);
                }

                Index index;

                switch (indexLocalizedData.Definition.Type)
                {
                    case IndexType.Map:
                        index = MapIndex.CreateNew(indexLocalizedData, _documentDatabase);
                        break;
                    case IndexType.MapReduce:
                        index = MapReduceIndex.CreateNew(indexLocalizedData, _documentDatabase);
                        break;
                    default:
                        throw new NotSupportedException($"Cannot create {indexLocalizedData.Definition.Type} index from IndexDefinition");
                }

                index.IsSideBySide = isSideBySide;
                return CreateIndexInternal(index, isSideBySide);
            }
        }

        public long CreateIndex(IndexDefinitionBase definition, bool isSideBySide = false)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            if (definition is MapIndexDefinition)
                return CreateIndex(new IndexLocalizedData(((MapIndexDefinition)definition).IndexDefinition,0,_documentDatabase)
                    , isSideBySide, null);

            lock (_indexAndTransformerLocker)
            {
                var transformer = _documentDatabase.TransformerStore.GetTransformer(definition.Name);
                if (transformer != null)
                    throw new IndexOrTransformerAlreadyExistException($"Tried to create an index with a name of {definition.Name}, but a transformer under the same name exist");

                ValidateIndexName(definition.Name);

                var lockMode = IndexLockMode.Unlock;
                var creationOptions = IndexCreationOptions.Create;
                var existingIndex = GetIndex(definition.Name);
                if (existingIndex != null)
                {
                    lockMode = existingIndex.Definition.LockMode;
                    creationOptions = GetIndexCreationOptions(definition, existingIndex);
                }

                if (creationOptions == IndexCreationOptions.Noop)
                {
                    Debug.Assert(existingIndex != null);

                    return existingIndex.Etag;
                }

                if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex || creationOptions == IndexCreationOptions.Update)
                {
                    Debug.Assert(existingIndex != null);

                    if (lockMode == IndexLockMode.LockedIgnore)
                        return existingIndex.Etag;

                    if (lockMode == IndexLockMode.LockedError)
                        throw new InvalidOperationException($"Can not overwrite locked index: {existingIndex.Name}");

                    throw new NotSupportedException($"Can not update auto-index: {existingIndex.Name}");
                }

                Index index;

                if (definition is AutoMapIndexDefinition)
                    index = AutoMapIndex.CreateNew((AutoMapIndexDefinition)definition, _documentDatabase);
                else if (definition is AutoMapReduceIndexDefinition)
                    index = AutoMapReduceIndex.CreateNew((AutoMapReduceIndexDefinition)definition, _documentDatabase);
                else
                    throw new NotImplementedException($"Unknown index definition type: {definition.GetType().FullName}");

                return CreateIndexInternal(index, isSideBySide);
            }
        }

        public bool HasChanged(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            ValidateIndexName(definition.Name);

            var existingIndex = GetIndex(definition.Name);
            if (existingIndex == null)
                return true;

            var creationOptions = GetIndexCreationOptions(definition, existingIndex);
            return creationOptions != IndexCreationOptions.Noop;
        }

        private long CreateIndexInternal(Index index, bool isSideBySide)
        {
            Debug.Assert(index != null);

            if (_documentDatabase.Configuration.Indexing.Disabled == false && _run)
                index.Start();
                        

            if (isSideBySide)
            {
                _indexes.SetSideBySideIndex(index);
            }
            else
            {
                _indexes.AddNewIndex(index);
            }
            

            _documentDatabase.Changes.RaiseNotifications(
                new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded,
                    Etag = index.Etag
                });
            return index.Etag;
        }

        private void UpdateIndex(IndexLocalizedData localizedData, Index existingIndex)
        {
            switch (localizedData.Definition.Type)
            {
                case IndexType.Map:
                    MapIndex.Update(existingIndex, localizedData, _documentDatabase);
                    break;
                case IndexType.MapReduce:
                    MapReduceIndex.Update(existingIndex, localizedData, _documentDatabase);
                    break;
                default:
                    throw new NotSupportedException($"Cannot update {localizedData.Definition.Type} index from IndexDefinition");
            }
        }

        internal IndexCreationOptions GetIndexCreationOptions(object indexDefinition, Index existingIndex)
        {
            if (existingIndex == null)
                return IndexCreationOptions.Create;

            //if (existingIndex.Definition.IsTestIndex) // TODO [ppekrol]
            //    return IndexCreationOptions.Update;

            var result = IndexDefinitionCompareDifferences.None;

            var indexDef = indexDefinition as IndexDefinition;
            if (indexDef != null)
                result = existingIndex.Definition.Compare(indexDef);

            var indexDefBase = indexDefinition as IndexDefinitionBase;
            if (indexDefBase != null)
                result = existingIndex.Definition.Compare(indexDefBase);

            if (result == IndexDefinitionCompareDifferences.All)
                return IndexCreationOptions.Update;

            result &= ~IndexDefinitionCompareDifferences.Etag; // we do not care about IndexId

            if (result == IndexDefinitionCompareDifferences.None)
                return IndexCreationOptions.Noop;

            if ((result & IndexDefinitionCompareDifferences.Maps) == IndexDefinitionCompareDifferences.Maps ||
                (result & IndexDefinitionCompareDifferences.Reduce) == IndexDefinitionCompareDifferences.Reduce)
                return IndexCreationOptions.Update;

            if ((result & IndexDefinitionCompareDifferences.Fields) == IndexDefinitionCompareDifferences.Fields)
                return IndexCreationOptions.Update;

            if ((result & IndexDefinitionCompareDifferences.Configuration) == IndexDefinitionCompareDifferences.Configuration)
            {
                var currentConfiguration = existingIndex.Configuration as SingleIndexConfiguration;
                if (currentConfiguration == null) // should not happen
                    return IndexCreationOptions.Update;

                var newIndexLocalizedData = new IndexLocalizedData(indexDef, 0, this._documentDatabase);
                
                var configurationResult = currentConfiguration.CalculateUpdateType(newIndexLocalizedData.LocalizedConfig);
                switch (configurationResult)
                {
                    case IndexUpdateType.None:
                        break;
                    case IndexUpdateType.Refresh:
                        return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;
                    case IndexUpdateType.Reset:
                        return IndexCreationOptions.Update;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if ((result & IndexDefinitionCompareDifferences.MapsFormatting) == IndexDefinitionCompareDifferences.MapsFormatting ||
                (result & IndexDefinitionCompareDifferences.ReduceFormatting) == IndexDefinitionCompareDifferences.ReduceFormatting)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            if ((result & IndexDefinitionCompareDifferences.Priority) == IndexDefinitionCompareDifferences.Priority)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            if ((result & IndexDefinitionCompareDifferences.LockMode) == IndexDefinitionCompareDifferences.LockMode)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            return IndexCreationOptions.Update;
        }
        
        internal static IndexCreationOptions GetIndexCreationOptions(IndexDefinition indexADefinition, IndexDefinition indexBDefinition, DocumentDatabase docDB)
        {
            if (indexBDefinition == null)
                return IndexCreationOptions.Create;

            //if (existingIndex.Definition.IsTestIndex) // TODO [ppekrol]
            //    return IndexCreationOptions.Update;

            var result = indexBDefinition.Compare(indexADefinition);

            if (result == IndexDefinitionCompareDifferences.All)
                return IndexCreationOptions.Update;

            result &= ~IndexDefinitionCompareDifferences.Etag; // we do not care about IndexId

            if (result == IndexDefinitionCompareDifferences.None)
                return IndexCreationOptions.Noop;

            if ((result & IndexDefinitionCompareDifferences.Maps) == IndexDefinitionCompareDifferences.Maps ||
                (result & IndexDefinitionCompareDifferences.Reduce) == IndexDefinitionCompareDifferences.Reduce)
                return IndexCreationOptions.Update;

            if ((result & IndexDefinitionCompareDifferences.Fields) == IndexDefinitionCompareDifferences.Fields)
                return IndexCreationOptions.Update;

            if ((result & IndexDefinitionCompareDifferences.Configuration) == IndexDefinitionCompareDifferences.Configuration)
            {
                var indexALocalizedData = new IndexLocalizedData(indexADefinition, 0, docDB);

                var indexBLocalizedData = new IndexLocalizedData(indexBDefinition, 0, docDB);

                if (indexBLocalizedData.LocalizedConfig == null) // should not happen
                    return IndexCreationOptions.Update;

                var configurationResult = indexBLocalizedData.LocalizedConfig.CalculateUpdateType(indexALocalizedData.LocalizedConfig);
                                
                switch (configurationResult)
                {
                    case IndexUpdateType.None:
                        break;
                    case IndexUpdateType.Refresh:
                        return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;
                    case IndexUpdateType.Reset:
                        return IndexCreationOptions.Update;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if ((result & IndexDefinitionCompareDifferences.MapsFormatting) == IndexDefinitionCompareDifferences.MapsFormatting ||
                (result & IndexDefinitionCompareDifferences.ReduceFormatting) == IndexDefinitionCompareDifferences.ReduceFormatting)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            if ((result & IndexDefinitionCompareDifferences.Priority) == IndexDefinitionCompareDifferences.Priority)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            if ((result & IndexDefinitionCompareDifferences.LockMode) == IndexDefinitionCompareDifferences.LockMode)
                return IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex;

            return IndexCreationOptions.Update;
        }

        public static void ValidateIndexName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Index name cannot be empty!");

            if (name.StartsWith(DynamicQueryRunner.DynamicIndexPrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index names starting with dynamic_ or dynamic/ are reserved!", nameof(name));
            }

            if (name.Equals(DynamicQueryRunner.DynamicIndex, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index name dynamic is reserved!", nameof(name));
            }

            if (name.Contains("//"))
            {
                throw new ArgumentException($"Index name '{name.Replace("//", "__")}' not permitted. Index name cannot contain // (double slashes)", nameof(name));
            }
        }

        public long ResetIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            return ResetIndexInternal(index);
        }

        public long ResetIndex(long id)
        {
            var index = GetIndex(id);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(id);

            return ResetIndexInternal(index);
        }

        public bool TryDeleteIndexIfExists(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                return false;

            DeleteIndexInternal(index);
            return true;
        }

        public void DeleteIndex(long id)
        {
            var index = GetIndex(id);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(id);

            DeleteIndexInternal(index);
        }

        private void DeleteIndexInternal(Index index)
        {
            lock (_indexAndTransformerLocker)
            {
                Index _;
                _indexes.TryRemoveById(index.Etag, out _);

                try
                {
                    index.Dispose();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not dispose index '{index.Name}' ({index.Etag}).", e);
                }
                                
                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved,
                    Etag = index.Etag
                });

                if (index.Configuration.RunInMemory)
                    return;

                var indexPath = index.Configuration.StoragePath;

                var indexTempPath = index.Configuration.TempPath;

                var journalPath = index.Configuration.JournalsStoragePath;

                IOExtensions.DeleteDirectory(indexPath.FullPath);

                if (indexTempPath != null)
                    IOExtensions.DeleteDirectory(indexTempPath.FullPath);

                if (journalPath != null)
                    IOExtensions.DeleteDirectory(journalPath.FullPath);
            }
        }

        public IndexRunningStatus Status
        {
            get
            {
                if (_documentDatabase.Configuration.Indexing.Disabled)
                    return IndexRunningStatus.Disabled;

                if (_run)
                    return IndexRunningStatus.Running;

                return IndexRunningStatus.Paused;
            }
        }

        public void StartIndexing()
        {
            _run = true;

            StartIndexing(_indexes.SelectMany(x=>new []{x.SideBySide, x.Current}).Where(x => x != null));
        }

        public void StartMapIndexes()
        {
            StartIndexing(_indexes.SelectMany(x => new[] { x.SideBySide, x.Current }).Where(x => x!= null && x.Type.IsMap()));
        }

        public void StartMapReduceIndexes()
        {
            StartIndexing(_indexes.SelectMany(x => new[] { x.SideBySide, x.Current }).Where(x => x!= null && x.Type.IsMapReduce()));
        }

        private void StartIndexing(IEnumerable<Index> indexes)
        {
            if (_documentDatabase.Configuration.Indexing.Disabled)
                return;

            Parallel.ForEach(indexes, index => index.Start());
        }

        public void StartIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Start();
        }

        public void StopIndex(string name)
        {
            var index = GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Stop();

            _documentDatabase.Changes.RaiseNotifications(new IndexChange
            {
                Name = name,
                Type = IndexChangeTypes.IndexPaused
            });
        }

        public void StopIndexing()
        {
            _run = false;

            StopIndexing(_indexes.SelectMany(x => new[] { x.SideBySide, x.Current }).Where(x => x != null));
        }

        public void StopMapIndexes()
        {
            StopIndexing(_indexes.SelectMany(x => new[] { x.SideBySide, x.Current }).Where(x => x!= null && x.Type.IsMap()));
        }

        public void StopMapReduceIndexes()
        {
            StopIndexing(_indexes.SelectMany(x => new[] { x.SideBySide, x.Current }).Where(x => x!= null && x.Type.IsMapReduce()));
        }

        private void StopIndexing(IEnumerable<Index> indexes)
        {
            if (_documentDatabase.Configuration.Indexing.Disabled)
                return;

            Parallel.ForEach(indexes, index => index.Stop());

            foreach (var index in indexes)
            {
                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexPaused
                });
            }
        }

        public void Dispose()
        {
            //FlushMapIndexes();
            //FlushReduceIndexes();

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(IndexStore)}");

            Parallel.ForEach(_indexes.SelectMany(x => new[] { x.SideBySide, x.Current }).Where(x => x != null), index =>
            {
                if (index is FaultyInMemoryIndex)
                    return;

                exceptionAggregator.Execute(index.Dispose);
            });

            if (_serverStore != null)
                exceptionAggregator.Execute(() =>
                {
                    _serverStore.Cluster.DatabaseChanged -= HandleDatabaseRecordChange;
                });


            exceptionAggregator.ThrowIfNeeded();
        }

        private long ResetIndexInternal(Index index)
        {
            lock (_indexAndTransformerLocker)
            {
                CollectionOfIndexes.IndexPair pair;
                if (_indexes.TryGetByName(index.Name, out pair))
                {
                    var isSideBySide = pair.Current.Etag != index.Etag;
                    DeleteIndex(index.Etag);
                    return CreateIndex(index.Definition, isSideBySide);
                }
                throw new InvalidOperationException($"Could not find index named {index.Name} to reset");
            }
        }

        private void OpenIndexes()
        {
            lock (_indexAndTransformerLocker)
            {
                TransactionOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    context.OpenReadTransaction();

                    var localIndexesJson = _serverStore.Cluster.ReadLocal(context, _databaseIndexesLocalKey);
                    if (localIndexesJson == null)
                        return; // shouldn't happen

                    var localIndexes = JsonDeserializationCluster.DatabaseLocalNodeIndexes(localIndexesJson);

                    if (localIndexes.Indexes == null)
                        return; // shouldn't happen

                    List<Exception> exceptions = null;
                    if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                        exceptions = new List<Exception>();

                    foreach (var index in localIndexes.Indexes)
                    {
                        AddIndexInstance(index.Value.Current, exceptions);

                        if(index.Value.SideBySide != null)
                            AddIndexInstance(index.Value.SideBySide, exceptions,true);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the indexes", exceptions);
                }
            }
        }

        public void AddIndexInstance(IndexLocalizedData indexLocalizedData, List<Exception> exceptions, bool isSideBySide = false)
        {
            Index indexInstance = null;
            
            try
            {
                if (_indexes.TryGetById(indexLocalizedData.Definition.Etag, out indexInstance) == false)
                {
                    CollectionOfIndexes.IndexPair indexPair;
                    if (_indexes.TryGetByName(indexLocalizedData.Definition.Name, out indexPair))
                    {
                        if (isSideBySide)
                            indexInstance = indexPair.SideBySide;
                        else
                            indexInstance = indexPair.Current;

                        
                    }

                    if (indexInstance == null)
                    {
                        // check if the path exists, it not, create it
                        var indexStoreDirectory = new DirectoryInfo(indexLocalizedData.StorageFinalPath);

                        if (indexStoreDirectory.Exists)
                        {
                            indexInstance = Index.Open(indexLocalizedData, _documentDatabase, isSideBySide);

                            CreateIndexInternal(indexInstance, isSideBySide);
                            return;
                        }
                        
                    }
                    CreateIndex(indexLocalizedData, isSideBySide, indexInstance);
                }
            }
            catch (Exception e)
            {
                indexInstance?.Dispose();
                exceptions?.Add(e);

                var configuration = new FaultyInMemoryIndexConfiguration(new PathSetting(indexLocalizedData.StorageFinalPath), _documentDatabase.Configuration);
                var fakeIndex = new FaultyInMemoryIndex(e, indexLocalizedData.Definition.Etag, indexLocalizedData.Definition.Name, configuration);

                var message =
                    $"Could not open index with id {indexLocalizedData.Definition.Name} at '{indexLocalizedData.StorageFinalPath}'. Created in-memory, fake instance: {fakeIndex.Name}";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Indexes store initialization error",
                    message,
                    AlertType.IndexStore_IndexCouldNotBeOpened,
                    NotificationSeverity.Error,
                    key: fakeIndex.Name,
                    details: new ExceptionDetails(e)));

                if (isSideBySide)
                    _indexes.SetSideBySideIndex(fakeIndex);
                else
                    _indexes.AddNewIndex(fakeIndex);
            }
        }

        public IEnumerable<Index> GetIndexesForCollection(string collection)
        {
            return _indexes.GetForCollection(collection).SelectMany(x=>new[]{x.Current, x.SideBySide}).Where(x => x != null);
        }

        public IEnumerable<Index> GetIndexes()
        {
            return _indexes.SelectMany(x => new[] { x.Current, x.SideBySide }).Where(x=>x!=null); 
        }

        public void RunIdleOperations()
        {
            HandleUnusedAutoIndexes();
            //DeleteSurpassedAutoIndexes(); // TODO [ppekrol]
        }

        private void HandleUnusedAutoIndexes()
        {
            var timeToWaitBeforeMarkingAutoIndexAsIdle = _documentDatabase.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle;
            var timeToWaitBeforeDeletingAutoIndexMarkedAsIdle = _documentDatabase.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle;
            var ageThreshold = timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan.Add(timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan); // idle * 2

            var indexesSortedByLastQueryTime = (from index in _indexes.SelectMany(x => new[] { x.Current, x.SideBySide }).Where(x => x != null)
                                                where index.State != IndexState.Disabled && index.State != IndexState.Error
                                                let stats = index.GetStats()
                                                let lastQueryingTime = stats.LastQueryingTime ?? DateTime.MinValue
                                                orderby lastQueryingTime
                                                select new UnusedIndexState
                                                {
                                                    LastQueryingTime = lastQueryingTime,
                                                    Index = index,
                                                    State = stats.State,
                                                    CreationDate = stats.CreatedTimestamp
                                                }).ToList();

            for (var i = 0; i < indexesSortedByLastQueryTime.Count; i++)
            {
                var item = indexesSortedByLastQueryTime[i];

                if (item.Index.Type != IndexType.AutoMap && item.Index.Type != IndexType.AutoMapReduce)
                    continue;

                var now = _documentDatabase.Time.GetUtcNow();
                var age = now - item.CreationDate;
                var lastQuery = now - item.LastQueryingTime;

                if (item.State == IndexState.Normal)
                {
                    TimeSpan differenceBetweenNewestAndCurrentQueryingTime;
                    if (i < indexesSortedByLastQueryTime.Count - 1)
                    {
                        var lastItem = indexesSortedByLastQueryTime[indexesSortedByLastQueryTime.Count - 1];
                        differenceBetweenNewestAndCurrentQueryingTime = lastItem.LastQueryingTime - item.LastQueryingTime;
                    }
                    else
                        differenceBetweenNewestAndCurrentQueryingTime = TimeSpan.Zero;

                    if (differenceBetweenNewestAndCurrentQueryingTime >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                    {
                        if (lastQuery >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                        {
                            item.Index.SetState(IndexState.Idle);
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Changed index '{item.Index.Name} ({item.Index.Etag})' priority to idle. Age: {age}. Last query: {lastQuery}. Query difference: {differenceBetweenNewestAndCurrentQueryingTime}.");
                        }
                    }

                    continue;
                }

                if (item.State == IndexState.Idle)
                {
                    if (age <= ageThreshold || lastQuery >= timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan)
                    {
                        DeleteIndex(item.Index.Etag);
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Deleted index '{item.Index.Name} ({item.Index.Etag})' due to idleness. Age: {age}. Last query: {lastQuery}.");
                    }
                }
            }
        }


        private static void ValidateAnalyzers(IndexDefinition definition)
        {
            if (definition.Fields == null)
                return;

            foreach (var kvp in definition.Fields)
            {
                if (string.IsNullOrWhiteSpace(kvp.Value.Analyzer))
                    continue;

                try
                {
                    IndexingExtensions.GetAnalyzerType(kvp.Key, kvp.Value.Analyzer);
                }
                catch (Exception e)
                {
                    throw new IndexCompilationException(e.Message, e);
                }
            }
        }

        private class UnusedIndexState
        {
            public DateTime LastQueryingTime { get; set; }
            public Index Index { get; set; }
            public IndexState State { get; set; }
            public DateTime CreationDate { get; set; }
        }

        public bool SwitchSideBySideIndexWithCurrent(string indexName, bool immediately = false)
        {
            bool lockTaken = false;
            Index oldCurrent = null;
            try
            {
                Monitor.TryEnter(_indexAndTransformerLocker, 16, ref lockTaken);
                if (lockTaken == false)
                    return false;


                CollectionOfIndexes.IndexPair indexPair;
                
                if (_indexes.TryGetByName(indexName, out indexPair) && indexPair.SideBySide != null)
                {
                    oldCurrent = indexPair.Current;
                    if (oldCurrent.Type.IsStatic())
                    {
                        // todo: not sure we need this code inside the "if"
                        var currentIndexDefinition = oldCurrent.GetIndexDefinition();
                        var sideBySideIndexDefinition = indexPair.SideBySide.Definition.GetOrCreateIndexDefinitionInternal();

                        if (indexPair.SideBySide.Definition.LockMode == IndexLockMode.Unlock && sideBySideIndexDefinition.LockMode.HasValue == false &&
                            currentIndexDefinition.LockMode.HasValue)
                            indexPair.SideBySide.SetLock(currentIndexDefinition.LockMode.Value);

                        if (indexPair.SideBySide.Definition.Priority == IndexPriority.Normal && sideBySideIndexDefinition.Priority.HasValue == false &&
                            currentIndexDefinition.Priority.HasValue)
                            indexPair.SideBySide.SetPriority(currentIndexDefinition.Priority.Value);
                    }
                }
                using (oldCurrent.DrainRunningQueries())
                {                    
                    DeleteIndexInternal(oldCurrent);
                }

                DatabaseLocalNodeIndexes localIndexes;
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var localIndexesJson = _serverStore.Cluster.ReadLocal(context, _databaseIndexesLocalKey);
                    localIndexes = localIndexesJson != null
                        ? JsonDeserializationCluster.DatabaseLocalNodeIndexes(localIndexesJson)
                        : new DatabaseLocalNodeIndexes {Indexes = new Dictionary<string, IndexHolder>()};
                    var localIndex = localIndexes.Indexes[indexName];
                    localIndex.Current = localIndex.SideBySide;
                    localIndex.SideBySide = null;
                    var afterUpdateLocalState = EntityToBlittable.ConvertEntityToBlittable(localIndexes, DocumentConventions.Default, context);
                    _serverStore.Cluster.WriteLocal(context, _databaseIndexesLocalKey, afterUpdateLocalState);
                    context.Transaction.Commit();
                }

                return true;
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(_indexAndTransformerLocker);
            }
        }

        public void RenameIndex(string oldIndexName, string newIndexName)
        {
            // todo: not sure how to implement that yet
            throw new NotImplementedException();
            //Index index;
            //if (_indexes.TryGetByName(oldIndexName, out index) == false)
            //    throw new InvalidOperationException($"Index {oldIndexName} does not exist");

            //lock (_indexAndTransformerLocker)
            //{
            //    var transformer = _documentDatabase.TransformerStore.GetTransformer(newIndexName);
            //    if (transformer != null)
            //    {
            //        throw new IndexOrTransformerAlreadyExistException(
            //            $"Cannot rename index to {newIndexName} because a transformer having the same name already exists");
            //    }

            //    Index _;
            //    if (_indexes.TryGetByName(newIndexName, out _))
            //    {
            //        throw new IndexOrTransformerAlreadyExistException(
            //            $"Cannot rename index to {newIndexName} because an index having the same name already exists");
            //    }

            //    index.Rename(newIndexName); // store new index name in 'metadata' file, actual dir rename will happen on next db load
            //    _indexes.RenameIndex(index, oldIndexName, newIndexName);
            //}

            //_documentDatabase.Changes.RaiseNotifications(new IndexRenameChange
            //{
            //    Name = newIndexName,
            //    OldIndexName = oldIndexName,
            //    Type = IndexChangeTypes.Renamed
            //});
        }

        public void SwapIndexInstances(string indexName, long oldEtag, long newEtag)
        {
            throw new NotImplementedException("TBI");
        }
    }

    public class IndexLocalizedData
    {
        public IndexLocalizedData(){}

        public IndexLocalizedData(IndexDefinition definition, int newId, IResourceStore docDB)
        {
            LocalizedConfig = new SingleIndexConfiguration(definition.Configuration, docDB.Configuration);
            var pathEnd = IndexDefinitionBase.GetIndexPathEndSafeForFileSystem(definition.Name, newId);

            Definition = definition;
                        
            StorageFinalPath = LocalizedConfig.StoragePath.Combine(pathEnd).ToFullPath();
            TempFinalPath = LocalizedConfig.TempPath?.Combine(pathEnd).ToFullPath();
            JournalFinalPath = LocalizedConfig.JournalsStoragePath?.Combine(pathEnd).ToFullPath();
            LocalizedConfig = new SingleIndexConfiguration(this, docDB.Configuration);
        }

        [JsonIgnoreAttribute]
        public SingleIndexConfiguration LocalizedConfig;
        public IndexDefinition Definition;
        public string StorageFinalPath;
        public string JournalFinalPath;
        public string TempFinalPath;
    }

    public class IndexHolder
    {
        public IndexLocalizedData Current { get; set; }
        public IndexLocalizedData SideBySide { get; set; }

        public int GetNextId()
        {
            if (Current == null)
                return 0;

            if (SideBySide == null)
                return ExtractId(Current.StorageFinalPath) + 1;


            return Math.Max(ExtractId(Current.StorageFinalPath), ExtractId(SideBySide.StorageFinalPath)) + 1;
        }

        private static int ExtractId(string storageFinalPath)
        {
            var lastSlashPosition = storageFinalPath.LastIndexOf('\\');
            var suffix = Int32.Parse(storageFinalPath.Substring(lastSlashPosition+1));
            return suffix;
        }
    }

    public class DatabaseLocalNodeIndexes
    {
        

        

        public Dictionary<string, IndexHolder> Indexes;
    }
}