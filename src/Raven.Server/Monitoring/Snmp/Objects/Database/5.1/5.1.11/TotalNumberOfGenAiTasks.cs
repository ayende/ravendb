using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfGenAiTasks : OngoingTasksBase
{
    public TotalNumberOfGenAiTasks(ServerStore serverStore)
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfGenAiTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfGenAiTasks(database);
    }
}
