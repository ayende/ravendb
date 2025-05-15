using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

internal sealed class GenAiScriptTransformer : EtlTransformer<AiEtlItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation>
{
    private readonly GenAiConfiguration _configuration;
    private List<GenAiScriptResult> _currentRun;
    private readonly PatchRequest _mainScript;

    public GenAiScriptTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, GenAiConfiguration configuration) : base(database, context, null, behaviorFunctions)
    {
        _configuration = configuration;
        _mainScript = new PatchRequest(transformation.Script, PatchRequestType.AiGen);
    }

    public override void Initialize(bool debugMode)
    {
        Database.Scripts.GetScriptRunner(_mainScript, true, out DocumentScript);

        if (DocumentScript == null)
            return;

        if (debugMode)
            DocumentScript.DebugMode = true;

        var contextFunc = new ClrFunction(DocumentScript.ScriptEngine, "context", AddContext);
        DocumentScript.ScriptEngine.SetValue("context", contextFunc);
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        throw new NotImplementedException();
    }

    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new NotImplementedException();
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new NotImplementedException();
    }

    protected override string[] LoadToDestinations { get; }

    protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
    {
        throw new NotImplementedException();
    }

    public override IEnumerable<GenAiScriptResult> GetTransformedResults()
    {
        return _currentRun ?? Enumerable.Empty<GenAiScriptResult>();
    }

    public override void Transform(AiEtlItem item, GenAiStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= [];

        Debug.Assert(item.IsDelete is false);
        
        DocumentScript.Run(Context, Context, "execute", [Current.Document]);
    }
    
    private JsValue AddContext(JsValue self, JsValue[] args)
    {
        const string methodDecl = "context(ctx);";
        if (args.Length != 1)
            throw new InvalidOperationException($"Invalid number of arguments for {methodDecl}, got {args.Length} but expected 1.");

        if (args[0].IsObject() is false)
            throw new ArgumentException("Expected 'ctx' to be an object, but was: " + args[0].Type + ", " + args[0]);

        var context = JsBlittableBridge.Translate(Context, DocumentScript.ScriptEngine, args[0].AsObject());
        string hash = CalculateHash(context);
        var isCached = ShouldSendContext(hash, _configuration.Name, Current.Document) == false;

        using (context)
        {
            _currentRun.Add(new GenAiScriptResult(Current.DocumentId, context.CloneOnTheSameContext(), hash, isCached));
        }

        return JsValue.Null;
    }

    private static bool ShouldSendContext(string hash, string taskName, Document doc)
    {
        if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
            metadata.TryGet(GenAiTask.GenAiHashesMetadataKey, out BlittableJsonReaderObject hashesSection) == false ||
            hashesSection.TryGet(taskName, out BlittableJsonReaderArray existingHashes) == false)
            return true; // hash not found, should send

        foreach (var h in existingHashes)
        {
            if (string.Equals(hash, h?.ToString(), StringComparison.OrdinalIgnoreCase))
                return false; // already sent
        }

        return true; // hash not found, should send
    }

    private string CalculateHash(BlittableJsonReaderObject contextObj)
    {
        var djv = new DynamicJsonValue
        {
            ["Context"] = contextObj,
            ["Prompt"] = _configuration.Prompt,
            ["Schema"] = _configuration.JsonSchema,
            ["Update"] = _configuration.Update
        };

        using var ctx = Context.ReadObject(djv, "hash");
        return AttachmentsStorageHelper.CalculateHash(ctx.AsSpan());
    }
}
