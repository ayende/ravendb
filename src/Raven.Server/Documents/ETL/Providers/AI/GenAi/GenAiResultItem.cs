using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public class GenAiResultItem
{
    public List<string> DebugOutput { get; set; }
    public DynamicJsonValue DebugActions { get; set; }
    public BlittableJsonReaderObject Usage { get; set; }
    public BlittableJsonReaderObject Context { get; set; }
    public bool IsCached { get; set; }
    public BlittableJsonReaderObject ModelOutput { get; set; }
    public string AiHash { get; set; }
    internal string DocId { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DebugOutput)] = DebugOutput == null ? null : new DynamicJsonArray(DebugOutput),
            [nameof(DebugActions)] = DebugActions,
            [nameof(Usage)] = Usage,
            [nameof(Context)] = Context,
            [nameof(IsCached)] = IsCached,
            [nameof(ModelOutput)] = ModelOutput,
            [nameof(AiHash)] = AiHash
        };
    }
}
