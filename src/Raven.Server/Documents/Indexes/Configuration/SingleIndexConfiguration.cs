using System;
using System.Reflection;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Configuration
{
    public class SingleIndexConfiguration : IndexingConfiguration
    {
        private bool? _runInMemory;
        private PathSetting _indexStoragePath;

        private readonly RavenConfiguration _databaseConfiguration;


        public SingleIndexConfiguration(IndexLocalizedData localizedData, RavenConfiguration databaseConfiguration)
            : base(databaseConfiguration)
        {
            _databaseConfiguration = databaseConfiguration;

            Initialize(
                key =>
                    new SettingValue(localizedData.Definition.Configuration.GetValue(key) ?? databaseConfiguration?.GetSetting(key),
                        databaseConfiguration?.GetServerWideSetting(key)),
                databaseConfiguration.GetServerWideSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory)),
                databaseConfiguration.ResourceType,
                databaseConfiguration.ResourceName,
                throwIfThereIsNoSetMethod: false);

            StoragePath = new PathSetting(localizedData.StorageFinalPath);
            JournalsStoragePath = localizedData.JournalFinalPath!= null? new PathSetting(localizedData.JournalFinalPath):null;
            TempPath = localizedData.TempFinalPath != null? new PathSetting(localizedData.TempFinalPath):null;

            Validate();
        }

        public SingleIndexConfiguration(IndexConfiguration clientConfiguration, RavenConfiguration databaseConfiguration)
            : base(databaseConfiguration)
        {
            _databaseConfiguration = databaseConfiguration;

            Initialize(
                key =>
                    new SettingValue(clientConfiguration.GetValue(key) ?? databaseConfiguration?.GetSetting(key),
                        databaseConfiguration?.GetServerWideSetting(key)),
                databaseConfiguration.GetServerWideSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory)),
                databaseConfiguration.ResourceType, 
                databaseConfiguration.ResourceName, 
                throwIfThereIsNoSetMethod: false);

            Validate();
        }

        public SingleIndexConfiguration(IndexConfiguration clientConfiguration, ResourceType resourceType, string resourceName)
            : base(null)
        {
            _databaseConfiguration = null;

            Initialize(
                key =>
                    new SettingValue(clientConfiguration.GetValue(key),
                        null),
                null,
                resourceType,
                resourceName,
                throwIfThereIsNoSetMethod: false);
        }

        private void Validate()
        {
            // todo: think if we need to perform any validations here
            return;
            if (string.Equals(StoragePath.FullPath, _databaseConfiguration.Indexing.StoragePath.FullPath, StringComparison.OrdinalIgnoreCase))
                return;

            if (_databaseConfiguration.Indexing.AdditionalStoragePaths != null)
            {
                foreach (var path in _databaseConfiguration.Indexing.AdditionalStoragePaths)
                {
                    if (string.Equals(StoragePath.FullPath, path.FullPath, StringComparison.OrdinalIgnoreCase))
                        return;
                }
            }

            throw new InvalidOperationException($"Given index path ('{StoragePath}') is not defined in '{RavenConfiguration.GetKey(x => x.Indexing.StoragePath)}' or '{RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)}'");
        }

        public override bool Disabled => _databaseConfiguration.Indexing.Disabled;

        public override bool RunInMemory
        {
            get
            {
                if (_runInMemory == null)
                    _runInMemory = _databaseConfiguration.Indexing.RunInMemory;

                return _runInMemory.Value;
            }

            protected set { _runInMemory = value; }
        }
        
        public override PathSetting StoragePath
        {
            get
            {
                if (_indexStoragePath == null)
                    _indexStoragePath = _databaseConfiguration.Indexing.StoragePath;
                return _indexStoragePath;
            }

            protected set
            {
                _indexStoragePath = value;
            }
        }

        public override PathSetting TempPath => _databaseConfiguration.Indexing.TempPath;

        public override PathSetting JournalsStoragePath => _databaseConfiguration.Indexing.JournalsStoragePath;

        public IndexUpdateType CalculateUpdateType(SingleIndexConfiguration newConfiguration)
        {
            var result = IndexUpdateType.None;
            foreach (var property in GetConfigurationProperties())
            {
                var currentValue = property.GetValue(this);
                var newValue = property.GetValue(newConfiguration);

                if (Equals(currentValue, newValue))
                    continue;

                var updateTypeAttribute = property.GetCustomAttribute<IndexUpdateTypeAttribute>();

                if (updateTypeAttribute.UpdateType == IndexUpdateType.Reset)
                    return IndexUpdateType.Reset; // worst case, we do not need to check further

                result = updateTypeAttribute.UpdateType;
            }

            return result;
        }
    }
}