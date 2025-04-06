using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Handlers.Processors;

internal sealed class GenAiHandlerProcessorForPostScriptTest : AbstractDatabaseEtlHandlerProcessorForTest<TestGenAiScript, GenAiConfiguration, AiConnectionString>
{
    public GenAiHandlerProcessorForPostScriptTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestGenAiScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestGenAiScript(json);
}
