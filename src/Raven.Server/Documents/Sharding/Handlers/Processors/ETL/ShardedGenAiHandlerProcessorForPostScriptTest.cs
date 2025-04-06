using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedGenAiHandlerProcessorForPostScriptTest : AbstractShardedEtlHandlerProcessorForTest<TestGenAiScript, GenAiConfiguration, AiConnectionString>
{
    public ShardedGenAiHandlerProcessorForPostScriptTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        // TODO : do we want to mark it as NotSupported like in ShardedEmbeddingsGenerationHandlerProcessorForPostScriptTest ?
        throw new NotImplementedException(); 
    }

    protected override TestGenAiScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestGenAiScript(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new GenAiTestCommand(RequestHandler.ShardExecutor.Conventions, json);
}
