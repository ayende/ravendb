using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationLoader
    {
        private readonly MetricsScheduler _metricsScheduler;
        private const int MaxSupportedSqlReplication = int.MaxValue; // TODO: Maybe this should be 128, 1024 or configurable?

        private BlittableJsonReaderObject _connections;
		protected readonly ILog _log;
		protected readonly DocumentDatabase _database;
		public readonly ConcurrentSet<SqlReplication> Replications = new ConcurrentSet<SqlReplication>();


		public Action<SqlReplicationStatistics> AfterReplicationCompleted;

        public SqlReplicationLoader(DocumentDatabase database, MetricsScheduler metricsScheduler)
        {
            _metricsScheduler = metricsScheduler;
			_database = database;
			_log = LogManager.GetLogger(GetType());
			_database.Notifications.OnDocumentChange += WakeReplication;
			_database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
		}

		public void Initialize()
		{
			LoadConfigurations();
		}

		private void WakeReplication(DocumentChangeNotification documentChangeNotification)
		{
			foreach (var replication in Replications)
				replication.WaitForChanges.SetByAsyncCompletion();
		}

		private void HandleSystemDocumentChange(DocumentChangeNotification notification)
		{
			if (ShouldReloadConfiguration(notification.Key))
			{
				foreach (var replication in Replications)
					replication.Dispose();

				Replications.Clear();
				LoadConfigurations();

				if (_log.IsDebugEnabled)
					_log.Debug($"Replication configuration was changed: {notification.Key}");
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return
                systemDocumentKey.StartsWith(Constants.SqlReplication.SqlReplicationConfigurationPrefix,
                    StringComparison.OrdinalIgnoreCase) ||
                systemDocumentKey.Equals(Constants.SqlReplication.SqlReplicationConnections, StringComparison.OrdinalIgnoreCase);
        }

        protected void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var sqlReplicationConnections = _database.DocumentsStorage.Get(context, Constants.SqlReplication.SqlReplicationConnections);
                if (sqlReplicationConnections != null)
                {
                    object connections;
                    if (sqlReplicationConnections.Data.TryGetMember("Connections", out connections))
                    {
                        _connections = connections as BlittableJsonReaderObject;
                    }
                }

                var documents = _database.DocumentsStorage.GetDocumentsStartingWith(context, Constants.SqlReplication.SqlReplicationConfigurationPrefix, null, null, 0, MaxSupportedSqlReplication);
                foreach (var document in documents)
                {
                    var configuration = JsonDeserialization.SqlReplicationConfiguration(document.Data);
                    var sqlReplication = new SqlReplication(_database, configuration, _metricsScheduler);
                    Replications.Add(sqlReplication);
                    if (sqlReplication.ValidateName() == false ||
                        sqlReplication.PrepareSqlReplicationConfig(_connections) == false)
                        return;
                    sqlReplication.Start();
                }
            }
        }

        public DynamicJsonValue SimulateSqlReplicationSqlQueries(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context)
        {
            try
            {
                var document = _database.DocumentsStorage.Get(context, simulateSqlReplication.DocumentId);
                var sqlReplication = new SqlReplication(_database, simulateSqlReplication.Configuration, _metricsScheduler);

                var result = sqlReplication.ApplyConversionScript(new List<Document> { document }, context);

                if (sqlReplication.PrepareSqlReplicationConfig(_connections, false) == false)
                {
                    return new DynamicJsonValue
                    {
                        ["LastAlert"] = sqlReplication.Statistics.LastAlert,
                    };
                }

                return sqlReplication.Simulate(simulateSqlReplication, context, result);
            }
            catch (Exception e)
            {
                return new DynamicJsonValue
                {
                    ["LastAlert"] = new Alert
                    {
                        IsError = true,
                        CreatedAt = SystemTime.UtcNow,
                        Message = "Last SQL replication operation for " + simulateSqlReplication.Configuration.Name + " was failed",
                        Title = "SQL replication error",
                        Exception = e.ToString(),
                        UniqueKey = "Sql Replication Error: " + simulateSqlReplication.Configuration.Name
                    },
                };
            }
        }

		public void Dispose()
		{
			_database.Notifications.OnDocumentChange -= WakeReplication;
			_database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
		}
	}
}