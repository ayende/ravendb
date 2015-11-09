// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexAttempts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes
{
    public class DatabaseIndexSuccesses : DatabaseIndexScalarObjectBase<Integer32>
    {
        public DatabaseIndexSuccesses(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, "6")
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var stats = GetIndexStats(database);
            return new Integer32(stats.IndexingSuccesses);
        }
    }
}
