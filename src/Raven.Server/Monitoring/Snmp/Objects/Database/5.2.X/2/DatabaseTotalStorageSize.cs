using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseTotalStorageSize : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseTotalStorageSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.2.5", index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var size = database.GetAllStoragesEnvironment().Sum(x => x.Environment.Stats().AllocatedDataFileSizeInBytes);
            return new Gauge32(size / 1024L / 1024L);
        }
    }
}
