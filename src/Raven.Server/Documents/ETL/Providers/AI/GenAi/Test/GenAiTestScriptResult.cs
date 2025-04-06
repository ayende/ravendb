using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;

public class GenAiTestScriptResult : TestEtlScriptResult
{
    public List<GenAiResultItem> Results;

    public BlittableJsonReaderObject InputDocument;

    public BlittableJsonReaderObject OutputDocument;

    public override DynamicJsonValue ToJson(JsonOperationContext context)
    {
        var json = base.ToJson(context);
        json[nameof(InputDocument)] = InputDocument;
        json[nameof(OutputDocument)] = OutputDocument;

        if (Results != null)
            json[nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()));

        return json;
    }
}
