using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.AiGen;

public class AiGenTestScriptResult(string documentId, BlittableJsonReaderObject context, string aiHash) : TestEtlScriptResult
{
    public string DocumentId { get; init; } = documentId;
    public BlittableJsonReaderObject Context { get; init; } = context;
    public string AiHash { get; init; } = aiHash;
}
