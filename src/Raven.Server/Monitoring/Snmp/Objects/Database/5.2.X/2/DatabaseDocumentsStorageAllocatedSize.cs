// -----------------------------------------------------------------------
//  <copyright file="DatabaseTransactionalStorageAllocatedSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseDocumentsStorageAllocatedSize : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseDocumentsStorageAllocatedSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.2.1", index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var stats = database.DocumentsStorage.Environment.Stats();
            return new Gauge32(stats.AllocatedDataFileSizeInBytes / 1024L / 1024L);
        }
    }
}
