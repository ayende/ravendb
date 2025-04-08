using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddGenAiCommand : AddEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public AddGenAiCommand()
    {
        // for deserialization
    }

    public AddGenAiCommand(GenAiConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
    {

    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        Validate(record);

        Add(ref record.GenAiEtls, record, etag);
    }

    private void Validate(DatabaseRecord databaseRecord)
    {
       //TODO
    }
}
