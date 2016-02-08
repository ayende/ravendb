using System;
using System.Diagnostics;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Internal;
using Microsoft.Extensions.DependencyInjection;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using System;
using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel;
using Microsoft.Extensions.DependencyInjection;

namespace Raven.Server
{
    public class RavenServer : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RavenServer));

        public readonly RavenConfiguration Configuration;

        public readonly ServerStore ServerStore;
        private IWebHost _webHost;

        public RavenServer(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            Configuration = configuration;
            if (Configuration.Initialized == false)
                throw new InvalidOperationException("Configuration must be initialized");

            ServerStore = new ServerStore(Configuration);
        }

        public void Initialize()
        {
            var sp = Stopwatch.StartNew();
            try
            {
                ServerStore.Initialize();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not open the server store", e);
                throw;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Server store started took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }
            sp.Restart();

            try
            {
                _webHost = new WebHostBuilder()
                    .UseServer("Microsoft.AspNet.Server.Kestrel")
                    .UseUrls(Configuration.Core.ServerUrl)
                    .UseEnvironment(EnvironmentName.Production)
                    .UseCaptureStartupErrors(true)
                    .UseStartup<RavenServerStartup>()
                    .Build();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not configure server", e);
                throw;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Configuring HTTP server took {0:#,#;;0} ms", sp.ElapsedMilliseconds);
            }

            try
            {
                _webHost.Start();
            }
            catch (Exception e)
            {
                Log.FatalException("Could not start server", e);
                throw;
            }
        }

        public void Dispose()
        {
            _webHost?.Dispose();
            ServerStore?.Dispose();
        }
    }
}