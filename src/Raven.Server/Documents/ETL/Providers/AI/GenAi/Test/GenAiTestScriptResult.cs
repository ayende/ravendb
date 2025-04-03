using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;

public class GenAiTestScriptResult : TestEtlScriptResult
{
    public List<SingleItemResult> Results;

    public BlittableJsonReaderObject InputDocument;
    public BlittableJsonReaderObject OutputDocument;

}

public class SingleItemResult
{
    public List<string> DebugOutput { get; set; }
    public DynamicJsonValue DebugActions { get; set; }
    public BlittableJsonReaderObject Usage { get; set; }
    public BlittableJsonReaderObject Context { get; set; }
    public bool IsCached { get; set; }
    public BlittableJsonReaderObject ModelOutput { get; set; }
    public string AiHash { get; set; }

    internal string DocId { get; set; }
}
