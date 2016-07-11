using System;

namespace Raven.Abstractions.Replication
{
    public class ReplicationOrigin
    {
		public Guid SourceDbId { get; set; }

		public string SourceDbName { get; set; }

		public string SourceURL { get; set; }
    }
}
