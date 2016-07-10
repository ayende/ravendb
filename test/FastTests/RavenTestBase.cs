﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

using Sparrow.Collections;
using Sparrow.Logging;

namespace FastTests
{
    public class RavenTestBase : LinuxRaceConditionWorkAround, IDisposable
    {
        public const string ServerName = "Raven.Tests.Core.Server";

        protected readonly List<DocumentStore> CreatedStores = new List<DocumentStore>();
        protected static readonly ConcurrentSet<string> PathsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private static RavenServer _globalServer;
        private static readonly object ServerLocker = new object();
        private static readonly object AvailableServerPortsLocker = new object();
        private RavenServer _localServer;

        public void DoNotReuseServer() => _doNotReuseServer = true;
        private bool _doNotReuseServer;
        private int NonReusedServerPort { get; set; }
        private int NonReusedTcpServerPort { get; set; }
        private const int MaxParallelServer = 79;
        private static readonly List<int> _usedServerPorts = new List<int>();
        private static List<int> _availableServerPorts;

        public async Task<DocumentDatabase> GetDatabase(string databaseName)
        {
            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            return database;
        }

        public RavenServer Server
        {
            get
            {
                if (_localServer != null)
                    return _localServer;

                if (_doNotReuseServer)
                {
                    if (_availableServerPorts == null)
                    {
                        lock (AvailableServerPortsLocker)
                        {
                            if (_availableServerPorts == null)
                                _availableServerPorts =
                                    Enumerable.Range(8079 - MaxParallelServer, MaxParallelServer).ToList();
                        }
                    }

                    NonReusedServerPort = GetAvailablePort();
                    NonReusedTcpServerPort = GetAvailablePort();
                    _localServer = CreateServer(NonReusedServerPort, NonReusedTcpServerPort);
                    return _localServer;
                }

                if (_globalServer != null)
                {
                    _localServer = _globalServer;
                    return _localServer;
                }
                lock (ServerLocker)
                {
                    if (_globalServer == null)
                    {
                        var globalServer = CreateServer(8080, 9090);
                        AssemblyLoadContext.Default.Unloading += context =>
                        {
                            globalServer.Dispose();

                            GC.Collect(2);
                            GC.WaitForPendingFinalizers();

                            var exceptionAggregator = new ExceptionAggregator("Failed to cleanup test databases");

                            RavenTestHelper.DeletePaths(PathsToDelete, exceptionAggregator);

                            exceptionAggregator.ThrowIfNeeded();
                        };
                        _globalServer = globalServer;
                    }
                    _localServer = _globalServer;
                }
                return _globalServer;
            }
        }

        private static int GetAvailablePort()
        {
            int available;
            lock (AvailableServerPortsLocker)
            {
                if (_availableServerPorts.Count != 0)
                {
                    available = _availableServerPorts[0];
                    _usedServerPorts.Add(available);
                    _availableServerPorts.RemoveAt(0);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Maximum allowed parallel servers pool in test is exhausted (max={MaxParallelServer}");
                }
            }
            return available;
        }

        private static void RemoveUsedPort(int port)
        {
            lock (AvailableServerPortsLocker)
            {
                _availableServerPorts.Add(port);
                _usedServerPorts.Remove(port);
            }
        }

        private RavenServer CreateServer(int port, int tcpPort)
        {
            var configuration = new RavenConfiguration();
            configuration.Initialize();
            configuration.DebugLog.LogMode = LogMode.None;
            configuration.Core.ServerUrl = $"http://localhost:{port}";
            configuration.Core.TcpServerUrl = $"tcp://localhost:{tcpPort}";
            configuration.Server.Name = ServerName;
            configuration.Core.RunInMemory = true;
            string postfix = port == 8080 ? "" : "_" + port;
            configuration.Core.DataDirectory = Path.Combine(configuration.Core.DataDirectory, $"Tests{postfix}");
            configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);
            configuration.Storage.AllowOn32Bits = true;

			IOExtensions.DeleteDirectory(configuration.Core.DataDirectory);
			ModifyConfiguration(configuration);

			var server = new RavenServer(configuration);
            server.Initialize();

            // TODO: Make sure to properly handle this when this is resolved:
            // TODO: https://github.com/dotnet/corefx/issues/5205
            // TODO: AssemblyLoadContext.GetLoadContext(typeof(RavenTestBase).GetTypeInfo().Assembly).Unloading +=

            return server;
        }

	    protected virtual void ModifyConfiguration(RavenConfiguration configuration)
	    {		    
	    }

        protected Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(DocumentStore store)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
        }

        protected virtual async Task<DocumentStore> GetDocumentStore([CallerMemberName] string caller = null, string dbSuffixIdentifier = null,
           Action<DatabaseDocument> modifyDatabaseDocument = null, string apiKey = null)
        {
            var name = caller ?? Guid.NewGuid().ToString("N");

            if (dbSuffixIdentifier != null)
                name = string.Format("{0}_{1}", name, dbSuffixIdentifier);

            var path = NewDataPath(name);

            var doc = MultiDatabase.CreateDatabaseDocument(name);
            doc.Settings["Raven/DataDir"] = path;
            doc.Settings["Raven/Indexing/ThrowIfAnyIndexCouldNotBeOpened"] = "true";
            modifyDatabaseDocument?.Invoke(doc);

            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                if (Server.ServerStore.Read(context, Constants.Database.Prefix + name) != null)
                    throw new InvalidOperationException($"Database '{name}' already exists");
            }

            var store = new DocumentStore
            {
                Url = UseFiddler(Server.Configuration.Core.ServerUrl),
                DefaultDatabase = name,
                ApiKey = apiKey
            };
            ModifyStore(store);
            store.Initialize();

            await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc).ConfigureAwait(false);
            store.AfterDispose += (sender, args) =>
            {
                store.DatabaseCommands.GlobalAdmin.DeleteDatabase(name, hardDelete: true);
                CreatedStores.Remove(store);
            };
            CreatedStores.Add(store);
            return store;
        }

        protected virtual void ModifyStore(DocumentStore store)
        {

        }

        private static string UseFiddler(string url)
        {
            if (Debugger.IsAttached && Process.GetProcessesByName("fiddler").Any())
                return url.Replace("localhost", "localhost.fiddler");

            return url;
        }

        public static void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
        {
            var databaseCommands = store.DatabaseCommands;
            if (database != null)
            {
                databaseCommands = databaseCommands.ForDatabase(database);
            }

            timeout = timeout ?? (Debugger.IsAttached
                ? TimeSpan.FromMinutes(5)
                : TimeSpan.FromSeconds(20));

            var spinUntil = SpinWait.SpinUntil(() =>
                databaseCommands.GetStatistics().StaleIndexes.Length == 0,
                timeout.Value);

            if (spinUntil)
                return;

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value);
        }

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            string url = documentStore.Url;

            var databaseNameEncoded = Uri.EscapeDataString(documentStore.DefaultDatabase);
            var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            Process.Start(documentsPage); // start the server

            do
            {
                Thread.Sleep(100);
            } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }

        protected string NewDataPath([CallerMemberName]string prefix = null, bool forceCreateDir = false)
        {
            var path = RavenTestHelper.NewDataPath(prefix, NonReusedServerPort, forceCreateDir);

            PathsToDelete.Add(path);
            return path;
        }

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);

            SystemTime.UtcDateTime = null;

            var exceptionAggregator = new ExceptionAggregator("Could not dispose test");
            foreach (var store in CreatedStores)
                exceptionAggregator.Execute(store.Dispose);
            CreatedStores.Clear();

            if (_localServer != null)
            {
                if (_doNotReuseServer)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        _localServer.Dispose();
                        _localServer = null;
                        RemoveUsedPort(NonReusedServerPort);
                        RemoveUsedPort(NonReusedTcpServerPort);
                    });

                    exceptionAggregator.ThrowIfNeeded();
                    return;
                }

                 exceptionAggregator.ThrowIfNeeded();
            }
        }
    }
}