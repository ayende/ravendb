using System;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddAiGenCommand : AddEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public AddAiGenCommand()
    {
        // for deserialization
    }

    public AddAiGenCommand(GenAiConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
    {

    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        Validate(record);

        Add(ref record.AiGenEtls, record, etag);
    }

    private void Validate(DatabaseRecord databaseRecord)
    {
       //TODO
    }
}
