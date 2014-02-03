﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Database.Config;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server
{
	public sealed class RavenDBOptions : IDisposable
	{
		private readonly DatabasesLandlord databasesLandlord;
		private readonly MixedModeRequestAuthorizer mixedModeRequestAuthorizer;
		private readonly DocumentDatabase systemDatabase;
		private readonly RequestManager requestManager;
		private readonly Task<RavenFileSystem> fileSystem;

		public RavenDBOptions(InMemoryRavenConfiguration configuration, DocumentDatabase db = null)
		{
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			
			try
			{
				HttpEndpointRegistration.RegisterHttpEndpointTarget();
				if (db == null)
				{
					systemDatabase = new DocumentDatabase(configuration);
					systemDatabase.SpinBackgroundWorkers();
				}
				else
				{
					systemDatabase = db;
				}
				var transportState = systemDatabase.TransportState;
				fileSystem = Task.Run(() => new RavenFileSystem(configuration, transportState));
				databasesLandlord = new DatabasesLandlord(systemDatabase);
				requestManager = new RequestManager(databasesLandlord);
				mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
				mixedModeRequestAuthorizer.Initialize(systemDatabase, new RavenServer(databasesLandlord.SystemDatabase, configuration));
			}
			catch
			{
				if (systemDatabase != null)
					systemDatabase.Dispose();
				throw;
			}
		}

		public DocumentDatabase SystemDatabase
		{
			get { return systemDatabase; }
		}

		public MixedModeRequestAuthorizer MixedModeRequestAuthorizer
		{
			get { return mixedModeRequestAuthorizer; }
		}

		public DatabasesLandlord Landlord
		{
			get { return databasesLandlord; }
		}

		public RequestManager RequestManager
		{
			get { return requestManager; }
		}

		public RavenFileSystem FileSystem
		{
			get { return fileSystem.Result; }
		}

		public void Dispose()
		{
		    var toDispose = new List<IDisposable>
		                    {
		                        mixedModeRequestAuthorizer, 
                                databasesLandlord, 
                                systemDatabase, 
                                requestManager
		                    };

            var errors = new List<Exception>();

		    try
		    {
                toDispose.Add(FileSystem); // adding task result
		    }
		    catch (Exception e)
		    {
                errors.Add(e);
		    }

            toDispose.Add(fileSystem); // adding task

		    foreach (var disposable in toDispose)
		    {
                try
                {
                    if (disposable != null)
                        disposable.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
		    }

			if (errors.Count != 0)
                throw new AggregateException(errors);
		}

		private class RavenServer : IRavenServer
		{
			private readonly InMemoryRavenConfiguration systemConfiguration;
			private readonly DocumentDatabase systemDatabase;

			public RavenServer(DocumentDatabase systemDatabase, InMemoryRavenConfiguration systemConfiguration)
			{
				this.systemDatabase = systemDatabase;
				this.systemConfiguration = systemConfiguration;
			}

			public DocumentDatabase SystemDatabase
			{
				get { return systemDatabase; }
			}

			public InMemoryRavenConfiguration SystemConfiguration
			{
				get { return systemConfiguration; }
			}
		}
	}
}