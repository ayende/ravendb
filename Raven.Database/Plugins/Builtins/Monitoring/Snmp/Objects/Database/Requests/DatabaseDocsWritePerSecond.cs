// -----------------------------------------------------------------------
//  <copyright file="DatabaseDocsWritePerSecond.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Requests
{
    public class DatabaseDocsWritePerSecond : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseDocsWritePerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.3.1", index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            var metricsCounters = database.WorkContext.MetricsCounters;
            return (int)metricsCounters.DocsPerSecond.CurrentValue;
        }
    }
}
