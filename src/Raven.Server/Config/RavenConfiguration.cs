﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Extensions;
using Raven.Server.Utils;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
        public bool Initialized { get; private set; }
        private bool allowChanges = false;

        private readonly IConfigurationBuilder _configBuilder;

        public CoreConfiguration Core { get; }

        public ReplicationConfiguration Replication { get; }

        public StorageConfiguration Storage { get; }

        public EncryptionConfiguration Encryption { get; }

        public MonitoringConfiguration Monitoring { get; }

        public WebSocketsConfiguration WebSockets { get; set; }

        public QueryConfiguration Queries { get; }

        public PatchingConfiguration Patching { get; }

        public BulkInsertConfiguration BulkInsert { get; }

        public ServerConfiguration Server { get; }

        public MemoryConfiguration Memory { get; }

        public ExpirationBundleConfiguration Expiration { get; }

        public VersioningBundleConfiguration Versioning { get; }

        public StudioConfiguration Studio { get; }

        public DatabaseConfiguration Databases { get; }

        public LicenseConfiguration Licensing { get; }

        public QuotasBundleConfiguration Quotas { get; }

        protected NameValueCollection Settings { get; set; }

        public RavenConfiguration()
        {
            _configBuilder = new ConfigurationBuilder()
                .AddJsonFile("settings.json", optional: true)
                .AddEnvironmentVariables(prefix: "RAVEN_");

            Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

            Core = new CoreConfiguration();

            Replication = new ReplicationConfiguration();
            Storage = new StorageConfiguration();
            Encryption = new EncryptionConfiguration();
            WebSockets = new WebSocketsConfiguration();
            Monitoring = new MonitoringConfiguration();
            Queries = new QueryConfiguration();
            Patching = new PatchingConfiguration();
            BulkInsert = new BulkInsertConfiguration();
            Server = new ServerConfiguration();
            Memory = new MemoryConfiguration();
            Expiration = new ExpirationBundleConfiguration();
            Versioning = new VersioningBundleConfiguration();
            Studio = new StudioConfiguration();
            Databases = new DatabaseConfiguration();
            Licensing = new LicenseConfiguration();
            Quotas = new QuotasBundleConfiguration();
        }

        public string DatabaseName { get; set; }
        public RavenWebHostConfiguration WebHostConfig { get; private set; }

        public RavenConfiguration Initialize()
        {
            LoadConfiguration(_configBuilder.Build());
            WebHostConfig = new RavenWebHostConfiguration(this);

            Core.Initialize(Settings);
            Replication.Initialize(Settings);
            Queries.Initialize(Settings);
            Patching.Initialize(Settings);
            BulkInsert.Initialize(Settings);
            Server.Initialize(Settings);
            Memory.Initialize(Settings);
            Storage.Initialize(Settings);
            Encryption.Initialize(Settings);
            Monitoring.Initialize(Settings);
            Expiration.Initialize(Settings);
            Versioning.Initialize(Settings);
            Studio.Initialize(Settings);
            Databases.Initialize(Settings);
            Licensing.Initialize(Settings);
            Quotas.Initialize(Settings);

            PostInit();

            Initialized = true;

            return this;
        }

        private void LoadConfiguration(IConfigurationRoot configurationRoot)
        {
            foreach (var section in configurationRoot.GetChildren())
            {
                Settings[section.Key] = section.Value;
            }
        }

        public void PostInit()
        {
            
        }

        public void CopyParentSettings(RavenConfiguration serverConfiguration)
        {
            Encryption.UseSsl = serverConfiguration.Encryption.UseSsl;
            Encryption.UseFips = serverConfiguration.Encryption.UseFips;

            Storage.AllowOn32Bits = serverConfiguration.Storage.AllowOn32Bits;
        }

        public void SetSetting(string key, string value)
        {
            if (Initialized && allowChanges == false)
                throw new InvalidOperationException("Configuration already initialized. You cannot specify an already initialized setting.");

            Settings[key] = value;
        }

        public string GetSetting(string key)
        {
            return Settings[key];
        }

        public static string GetKey<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = getKey.ToProperty();
            return prop.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).First().Key;
        }

        public static RavenConfiguration CreateFrom(RavenConfiguration parent)
        {
            var result = new RavenConfiguration
            {
                Settings = new NameValueCollection(parent.Settings)
            };

            result.Settings[GetKey(x => x.Core.RunInMemory)] = parent.Core.RunInMemory.ToString();

            return result;
        }

        public void AddCommandLine(string[] args)
        {
            _configBuilder.AddCommandLine(args);
        }
    }

    public class RavenWebHostConfiguration : IConfiguration
    {
        private readonly RavenConfiguration _configuration;

        public RavenWebHostConfiguration(RavenConfiguration configuration)
        {
            _configuration = configuration;
        }


        public IConfigurationSection GetSection(string key)
        {
            switch (key)
            {
                case "server.urls":
                    return new RavenConfigurationSection(key, "", _configuration.Core.ServerUrl);
                default:
                    throw new NotImplementedException($"{key} should be supported");
            }
        }

        public IEnumerable<IConfigurationSection> GetChildren()
        {
            throw new NotImplementedException();
        }

        public IChangeToken GetReloadToken()
        {
            throw new NotImplementedException();
        }

        public string this[string key]
        {
            get
            {
                switch (key)
                {
                    case "webroot":
                        return "webroot";
                    case "Hosting:Environment":
                        return "Production";
                    case "Hosting:Server":
                        return "Microsoft.AspNet.Server.Kestrel";
                    case "Hosting:Application":
                        return "";
                    case "server.urls":
                        return _configuration.Core.ServerUrl;
                    case "HTTP_PLATFORM_PORT":
                        return "";
                    /*var url = _configuration.Core.ServerUrls.First();
                    return url.Substring(url.IndexOf(':', "http://".Length) + 1).TrimEnd('/');*/
                    default:
                        throw new NotImplementedException($"{key} should be supported");
                }
            }
            set { throw new NotImplementedException(); }
        }
    }

    public class RavenConfigurationSection : IConfigurationSection
    {
        public RavenConfigurationSection(string key, string path, string value)
        {
            Key = key;
            Path = path;
            Value = value;
        }

        public IConfigurationSection GetSection(string key)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IConfigurationSection> GetChildren()
        {
            throw new NotImplementedException();
        }

        public IChangeToken GetReloadToken()
        {
            throw new NotImplementedException();
        }

        public string this[string key]
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public string Key { get; }
        public string Path { get; }
        public string Value { get; set; }
    }
}