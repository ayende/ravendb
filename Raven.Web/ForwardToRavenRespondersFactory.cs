//-----------------------------------------------------------------------
// <copyright file="ForwardToRavenRespondersFactory.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Web;
using System.Web.Hosting;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using NLog;

namespace Raven.Web
{
	public class ForwardToRavenRespondersFactory : IHttpHandlerFactory
	{
		private static Logger log = LogManager.GetCurrentClassLogger();
		internal static DocumentDatabase database;
		internal static HttpServer server;
		private static readonly object locker = new object();

		public class ReleaseRavenDBWhenAppDomainIsTornDown : IRegisteredObject
		{
			public void Stop(bool immediate)
			{
				Shutdown();
				HostingEnvironment.UnregisterObject(this);
			}
		}

		public IHttpHandler GetHandler(HttpContext context, string requestType, string url, string pathTranslated)
		{
			if (database == null)
				throw new InvalidOperationException("Database has not been initialized properly");
			return new ForwardToRavenResponders(server);
		}

		public void ReleaseHandler(IHttpHandler handler)
		{
		}

		public static void Init()
		{
			if (database != null)
				return;

			lock (locker)
			{
				if (database != null)
					return;
					
				log.Debug("Initializing Raven forwarders factory on first request"); 

				try
				{
					var ravenConfiguration = new RavenConfiguration();
					HttpEndpointRegistration.RegisterHttpEndpointTarget();
					database = new DocumentDatabase(ravenConfiguration);
					database.SpinBackgroundWorkers();
					server = new HttpServer(ravenConfiguration, database);
					server.Init();
				}
				catch
				{
					if (database != null)
					{
						database.Dispose();
						database = null;
					}
					if (server != null)
					{
						server.Dispose();
						server = null;
					}
					throw;
				}

				HostingEnvironment.RegisterObject(new ReleaseRavenDBWhenAppDomainIsTornDown());
			}
		}

		public static void Shutdown()
		{
			log.Debug("Shutting down Raven forwarders factory"); 
			lock (locker)
			{
				if (server != null)
					server.Dispose();

				if (database != null)
					database.Dispose();

				server = null;
				database = null;
			}
		}
	}
}