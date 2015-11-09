// -----------------------------------------------------------------------
//  <copyright file="ReplicationDestinationTineSinceLastReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Abstractions;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Bundles.Replication
{
    public class ReplicationDestinationTimeSinceLastReplication : ReplicationDestinationScalarObjectBase<TimeTicks>
    {
        public ReplicationDestinationTimeSinceLastReplication(string databaseName, DatabasesLandlord landlord, int databaseIndex, string destinationUrl, int destinationIndex)
            : base(databaseName, landlord, databaseIndex, destinationUrl, destinationIndex, "3")
        {
        }

        public override TimeTicks GetData(DocumentDatabase database, ReplicationTask task, ReplicationStrategy destination)
        {
            var sourceReplicationInformationWithBatchInformation = task.GetLastReplicatedEtagFrom(destination);
            if (sourceReplicationInformationWithBatchInformation == null)
                return null;

            if (sourceReplicationInformationWithBatchInformation.LastModified.HasValue == false)
                return null;

            return new TimeTicks(SystemTime.UtcNow - sourceReplicationInformationWithBatchInformation.LastModified.Value);
        }
    }
}
