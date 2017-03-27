using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
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
                serverStore.Cluster.DatabaseChanged += HandleDatabaseRecordChanged;

            _databaseIndexesLocalKey = $"db/{_documentDatabase.Name}/indexes";
        }

        private void HandleDatabaseRecordChanged(object o, string s)
        {

        }

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
                            : new DatabaseLocalNodeIndexes { Indexes = new Dictionary<string, DatabaseLocalNodeIndexes.IndexHolder>() };

                        databaseRecord = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name);
                    }

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

        private void UpdateLocalStateBasedOnDatabaseRecord(Dictionary<string, DatabaseLocalNodeIndexes.IndexHolder> existingIndexes, Dictionary<string, IndexDefinition> expectedIndexes)
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
                DeleteIndexDirectory(index.Current, errors, name, etag: null);
            }

            foreach (var definition in expectedIndexes.Values)
            {
                DatabaseLocalNodeIndexes.IndexHolder existingDefs;
                if (existingIndexes.TryGetValue(definition.Name, out existingDefs) == false)
                {
                    existingIndexes[definition.Name] = new DatabaseLocalNodeIndexes.IndexHolder
                    {
                        Current = definition
                    };
                    continue;
                }
                Debug.Assert(existingDefs.Current != null);
                if (existingDefs.Current.Equals(definition))
                {
                    DeleteIndexDirectory(existingDefs.SideBySide, errors, definition.Name, definition.Etag);
                    existingDefs.SideBySide = null;
                    continue;
                }
                if (existingDefs.SideBySide?.Equals(definition) == true)
                {
                    continue;
                }
                DeleteIndexDirectory(existingDefs.SideBySide, errors, definition.Name, definition.Etag);
                existingDefs.SideBySide = definition;
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

        private void DeleteIndexDirectory(IndexDefinition definition, List<Exception> errors, string name, long? etag)
        {
            if (definition == null)
                return;
            try
            {
                var configuration = new SingleIndexConfiguration(definition.Configuration, _documentDatabase.Configuration);
                if (configuration.JournalsStoragePath != null)
                {
                    IOExtensions.DeleteDirectory(etag != null
                        ? Path.Combine(configuration.JournalsStoragePath.FullPath, name, etag.Value.ToString())
                        : Path.Combine(configuration.JournalsStoragePath.FullPath, name));
                }
                if (configuration.TempPath != null)
                {
                    IOExtensions.DeleteDirectory(etag != null
                        ? Path.Combine(configuration.TempPath.FullPath, name, etag.Value.ToString())
                        : Path.Combine(configuration.TempPath.FullPath, name));
                }
                if (configuration.StoragePath != null)
                {
                    IOExtensions.DeleteDirectory(etag != null
                        ? Path.Combine(configuration.StoragePath.FullPath, name, etag.Value.ToString())
                        : Path.Combine(configuration.StoragePath.FullPath, name));
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    errors.Add(e);
                    _logger.Operations($"Unable to delete index {name} when found that our local state includes it and the database record does not", e);
                }
            }
        }

        public Index GetIndex(string name)
        {
            Index index;
            if (_indexes.TryGetByName(name, out index) == false)
                return null;

            return index;
        }

        public Index GetIndex(long id)
        {
            Index index;
            if (_indexes.TryGetById(id, out index) == false)
                return null;

            return index;
        }

        public long CreateIndex(IndexDefinition definition, string path = null)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            lock (_indexAndTransformerLocker)
            {
                var transformer = _documentDatabase.TransformerStore.GetTransformer(definition.Name);
                if (transformer != null)
                    throw new IndexOrTransformerAlreadyExistException($"Tried to create an index with a name of {definition.Name}, but a transformer under the same name exist");

                ValidateIndexName(definition.Name);
                definition.RemoveDefaultValues();
                ValidateAnalyzers(definition);

                var lockMode = IndexLockMode.Unlock;
                var creationOptions = IndexCreationOptions.Create;
                var existingIndex = GetIndex(definition.Name);
                if (existingIndex != null)
                {
                    lockMode = existingIndex.Definition.LockMode;
                    creationOptions = GetIndexCreationOptions(definition, existingIndex);
                }

                var replacementIndexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + definition.Name;

                if (creationOptions == IndexCreationOptions.Noop)
                {
                    Debug.Assert(existingIndex != null);

                    TryDeleteIndexIfExists(replacementIndexName);

                    return existingIndex.Etag;
                }

                if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                {
                    Debug.Assert(existingIndex != null);

                    if (lockMode == IndexLockMode.LockedIgnore)
                        return existingIndex.Etag;

                    if (lockMode == IndexLockMode.LockedError)
                        throw new InvalidOperationException("Can not overwrite locked index: " + existingIndex.Name);

                    TryDeleteIndexIfExists(replacementIndexName);

                    UpdateIndex(definition, existingIndex);
                    return existingIndex.Etag;
                }

                if (creationOptions == IndexCreationOptions.Update)
                {
                    Debug.Assert(existingIndex != null);

                    if (lockMode == IndexLockMode.LockedIgnore)
                        return existingIndex.Etag;

                    if (lockMode == IndexLockMode.LockedError)
                        throw new InvalidOperationException($"Can not overwrite locked index: {existingIndex.Name}");

                    definition.Name = replacementIndexName;

                    existingIndex = GetIndex(replacementIndexName);
                    if (existingIndex != null)
                    {
                        creationOptions = GetIndexCreationOptions(definition, existingIndex);
                        if (creationOptions == IndexCreationOptions.Noop)
                            return existingIndex.Etag;

                        if (creationOptions == IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex)
                        {
                            UpdateIndex(definition, existingIndex);
                            return existingIndex.Etag;
                        }
                    }

                    TryDeleteIndexIfExists(replacementIndexName);
                }

                Index index;

                switch (definition.Type)
                {
                    case IndexType.Map:
                        index = MapIndex.CreateNew(definition.Etag, definition, _documentDatabase);
                        break;
                    case IndexType.MapReduce:
                        index = MapReduceIndex.CreateNew(definition, _documentDatabase);
                        break;
                    default:
                        throw new NotSupportedException($"Cannot create {definition.Type} index from IndexDefinition");
                }

                return CreateIndexInternal(index);
            }
        }

        public long CreateIndex(IndexDefinitionBase definition, string path = null)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            if (definition is MapIndexDefinition)
                return CreateIndex(((MapIndexDefinition)definition).IndexDefinition, path);

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

                return CreateIndexInternal(index);
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

        private long CreateIndexInternal(Index index)
        {
            Debug.Assert(index != null);

            if (_documentDatabase.Configuration.Indexing.Disabled == false && _run)
                index.Start();

            var etag = _documentDatabase.IndexMetadataPersistence.OnIndexCreated(index);

            _indexes.Add(index);

            _documentDatabase.Changes.RaiseNotifications(
                new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexAdded,
                    Etag = etag
                });
            return index.Etag;
        }

        private void UpdateIndex(IndexDefinition definition, Index existingIndex)
        {
            switch (definition.Type)
            {
                case IndexType.Map:
                    MapIndex.Update(existingIndex, definition, _documentDatabase);
                    break;
                case IndexType.MapReduce:
                    MapReduceIndex.Update(existingIndex, definition, _documentDatabase);
                    break;
                default:
                    throw new NotSupportedException($"Cannot update {definition.Type} index from IndexDefinition");
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

                var newConfiguration = new SingleIndexConfiguration(indexDef.Configuration, _documentDatabase.Configuration);
                var configurationResult = currentConfiguration.CalculateUpdateType(newConfiguration);
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

        private void ValidateIndexName(string name)
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

                var tombstoneEtag = _documentDatabase.IndexMetadataPersistence.OnIndexDeleted(index);
                _documentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = index.Name,
                    Type = IndexChangeTypes.IndexRemoved,
                    Etag = tombstoneEtag
                });

                if (index.Configuration.RunInMemory)
                    return;

                var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name);

                var indexPath = index.Configuration.StoragePath.Combine(name);

                var indexTempPath = index.Configuration.TempPath?.Combine(name);

                var journalPath = index.Configuration.JournalsStoragePath?.Combine(name);

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

            StartIndexing(_indexes);
        }

        public void StartMapIndexes()
        {
            StartIndexing(_indexes.Where(x => x.Type.IsMap()));
        }

        public void StartMapReduceIndexes()
        {
            StartIndexing(_indexes.Where(x => x.Type.IsMapReduce()));
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

            StopIndexing(_indexes);
        }

        public void StopMapIndexes()
        {
            StopIndexing(_indexes.Where(x => x.Type.IsMap()));
        }

        public void StopMapReduceIndexes()
        {
            StopIndexing(_indexes.Where(x => x.Type.IsMapReduce()));
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

            Parallel.ForEach(_indexes, index =>
            {
                if (index is FaultyInMemoryIndex)
                    return;

                exceptionAggregator.Execute(index.Dispose);
            });

            if (_serverStore != null)
                exceptionAggregator.Execute(() =>
                {
                    _serverStore.Cluster.DatabaseChanged -= HandleDatabaseRecordChanged;
                });


            exceptionAggregator.ThrowIfNeeded();
        }

        private long ResetIndexInternal(Index index)
        {
            lock (_indexAndTransformerLocker)
            {
                DeleteIndex(index.Etag);
                return CreateIndex(index.Definition, index.Configuration.StoragePath.ToString());
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
                        AddIndexInstance(index.Value.Current,  exceptions);

                        if(index.Value.SideBySide != null)
                            AddIndexInstance(index.Value.SideBySide, exceptions);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the indexes", exceptions);
                }
            }
        }

        private void AddIndexInstance(IndexDefinition indexDefinition, List<Exception> exceptions)
        {
            Index indexInstance = null;
            var singleIndexConfiguration = new SingleIndexConfiguration(indexDefinition.Configuration, _documentDatabase.Configuration);
            try
            {
                // check if the path exists, it not, create it
                indexInstance = Index.Open(singleIndexConfiguration, indexDefinition, _documentDatabase);

                _indexes.Add(indexInstance);
            }
            catch (Exception e)
            {
                indexInstance?.Dispose();
                exceptions?.Add(e);

                var configuration = new FaultyInMemoryIndexConfiguration(singleIndexConfiguration.StoragePath, _documentDatabase.Configuration);
                var fakeIndex = new FaultyInMemoryIndex(e, indexDefinition.Etag, indexDefinition.Name, configuration);

                var message =
                    $"Could not open index with id {indexDefinition.Name} at '{singleIndexConfiguration.StoragePath}'. Created in-memory, fake instance: {fakeIndex.Name}";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Indexes store initialization error",
                    message,
                    AlertType.IndexStore_IndexCouldNotBeOpened,
                    NotificationSeverity.Error,
                    key: fakeIndex.Name,
                    details: new ExceptionDetails(e)));

                _indexes.Add(fakeIndex);
            }
        }

        public IEnumerable<Index> GetIndexesForCollection(string collection)
        {
            return _indexes.GetForCollection(collection);
        }

        public IEnumerable<Index> GetIndexes()
        {
            return _indexes;
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

            var indexesSortedByLastQueryTime = (from index in _indexes
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

        public bool TryReplaceIndexes(string oldIndexName, string newIndexName, bool immediately = false)
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(_indexAndTransformerLocker, 16, ref lockTaken);
                if (lockTaken == false)
                    return false;

                Index newIndex;
                if (_indexes.TryGetByName(newIndexName, out newIndex) == false)
                    return true;

                Index oldIndex;
                if (_indexes.TryGetByName(oldIndexName, out oldIndex))
                {
                    oldIndexName = oldIndex.Name;

                    if (oldIndex.Type.IsStatic() && newIndex.Type.IsStatic())
                    {
                        var oldIndexDefinition = oldIndex.GetIndexDefinition();
                        var newIndexDefinition = newIndex.Definition.GetOrCreateIndexDefinitionInternal();

                        if (newIndex.Definition.LockMode == IndexLockMode.Unlock && newIndexDefinition.LockMode.HasValue == false && oldIndexDefinition.LockMode.HasValue)
                            newIndex.SetLock(oldIndexDefinition.LockMode.Value);

                        if (newIndex.Definition.Priority == IndexPriority.Normal && newIndexDefinition.Priority.HasValue == false && oldIndexDefinition.Priority.HasValue)
                            newIndex.SetPriority(oldIndexDefinition.Priority.Value);
                    }
                }

                _documentDatabase.IndexMetadataPersistence.OnIndexDeleted(newIndex);

                _indexes.ReplaceIndex(oldIndexName, oldIndex, newIndex);
                newIndex.Rename(oldIndexName);

                if (oldIndex != null)
                {
                    using (oldIndex.DrainRunningQueries())
                        DeleteIndexInternal(oldIndex);
                }

                _documentDatabase.IndexMetadataPersistence.OnIndexCreated(newIndex);

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
            Index index;
            if (_indexes.TryGetByName(oldIndexName, out index) == false)
                throw new InvalidOperationException($"Index {oldIndexName} does not exist");

            lock (_indexAndTransformerLocker)
            {
                var transformer = _documentDatabase.TransformerStore.GetTransformer(newIndexName);
                if (transformer != null)
                {
                    throw new IndexOrTransformerAlreadyExistException(
                        $"Cannot rename index to {newIndexName} because a transformer having the same name already exists");
                }

                Index _;
                if (_indexes.TryGetByName(newIndexName, out _))
                {
                    throw new IndexOrTransformerAlreadyExistException(
                        $"Cannot rename index to {newIndexName} because an index having the same name already exists");
                }

                index.Rename(newIndexName); // store new index name in 'metadata' file, actual dir rename will happen on next db load
                _indexes.RenameIndex(index, oldIndexName, newIndexName);
            }

            _documentDatabase.Changes.RaiseNotifications(new IndexRenameChange
            {
                Name = newIndexName,
                OldIndexName = oldIndexName,
                Type = IndexChangeTypes.Renamed
            });
        }

        public void SwapIndexInstances(string indexName, long oldEtag, long newEtag)
        {
            throw new NotImplementedException("TBI");
        }
    }

    public class DatabaseLocalNodeIndexes
    {
        public class IndexHolder
        {
            public IndexDefinition Current;
            public IndexDefinition SideBySide;
        }

        public Dictionary<string, IndexHolder> Indexes;
    }
}