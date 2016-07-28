using System.Collections.Concurrent;
using System.Linq;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationMetricsCountersManager
    {
        private readonly MetricsScheduler _metricsScheduler;
        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }
        public ConcurrentDictionary<string, SqlReplicationTableMetrics> TablesMetrics { get; set; }
        public ConcurrentQueue<SqlReplicationPerformanceStats> ReplicationPerformanceStats { get; set; }

        public SqlReplicationMetricsCountersManager(MetricsScheduler metricsScheduler)
        {
            _metricsScheduler = metricsScheduler;
            SqlReplicationBatchSizeMeter = new MeterMetric(metricsScheduler);
            TablesMetrics = new ConcurrentDictionary<string, SqlReplicationTableMetrics>();
            ReplicationPerformanceStats = new ConcurrentQueue<SqlReplicationPerformanceStats>();
        }

        public SqlReplicationTableMetrics GetTableMetrics(string tableName)
        {
            return TablesMetrics.GetOrAdd(tableName, name => new SqlReplicationTableMetrics(_metricsScheduler, name));
        }

        public DynamicJsonValue ToSqlReplicationMetricsData()
        {
            return new DynamicJsonValue
            {
                ["GeneralMetrics"] = new DynamicJsonValue
                {
                    ["Batch Size Meter"] = SqlReplicationBatchSizeMeter.CreateMeterData()
                },
                ["TablesMetrics"] = TablesMetrics.ToDictionary(x => x.Key, x => x.Value.ToSqlReplicationTableMetricsDataDictionary()),
            };
        }

        public void UpdateReplicationPerformance(SqlReplicationPerformanceStats performance)
        {
            ReplicationPerformanceStats.Enqueue(performance);
            while (ReplicationPerformanceStats.Count > 25)
            {
                SqlReplicationPerformanceStats _;
                ReplicationPerformanceStats.TryDequeue(out _);
            }
        }
    }
}