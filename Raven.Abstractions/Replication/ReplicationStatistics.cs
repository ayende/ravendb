using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Replication
{
    using Raven.Json.Linq;

    public class ReplicationStatistics
    {
        public string Self { get; set; }
        public Etag MostRecentDocumentEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag MostRecentAttachmentEtag { get; set; }

        public List<DestinationStats> Stats { get; set; }

        public ReplicationStatistics()
        {
            Stats = new List<DestinationStats>();
        }
    }

    public class DestinationStats
    {
        public DestinationStats()
        {
            LastStats = new RavenJArray();
        }

        public int FailureCountInternal = 0;
        public string Url { get; set; }
        public DateTime? LastHeartbeatReceived { get; set; }
        public Etag LastEtagCheckedForReplication { get; set; }
        public Etag LastReplicatedEtag { get; set; }
        public Etag LastReplicatedAttachmentEtag { get; set; }
        public DateTime? LastReplicatedLastModified { get; set; }
        public DateTime? LastSuccessTimestamp { get; set; }
        public DateTime? LastFailureTimestamp { get; set; }
        public DateTime? FirstFailureInCycleTimestamp { get; set; }
        public int FailureCount { get { return FailureCountInternal; } }
        public string LastError { get; set; }
        public RavenJArray LastStats { get; set; }
    }

    public class ReplicationPerformanceStats
    {
        public int BatchSize { get; set; }
        public TimeSpan Duration { get; set; }
        public DateTime Started { get; set; }
        public double DurationMilliseconds { get { return Math.Round(Duration.TotalMilliseconds, 2); } }

        public override string ToString()
        {
            return string.Format("BatchSize: {0}, Started: {1}, Duration: {2}", BatchSize, Started, Duration);
        }

        protected bool Equals(ReplicationPerformanceStats other)
        {
            return BatchSize == other.BatchSize && Duration.Equals(other.Duration) && Started.Equals(other.Started);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ReplicationPerformanceStats)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = BatchSize;
                hashCode = (hashCode * 397) ^ Duration.GetHashCode();
                hashCode = (hashCode * 397) ^ Started.GetHashCode();
                return hashCode;
            }
        }
    }
}
