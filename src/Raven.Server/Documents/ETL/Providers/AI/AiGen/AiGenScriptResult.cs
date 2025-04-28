using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.AiGen;

public record AiGenScriptResult(string DocumentId, BlittableJsonReaderObject Context, string AiHash)
{
    public BlittableJsonReaderObject Output;
}
