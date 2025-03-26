using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.AiGen;

internal sealed class AiGenScriptTransformer : EtlTransformer<AiEtlItem, AiGenScriptResult, AiGenStatsScope, AiGenPerformanceOperation>
{
    private readonly AiGenConfiguration _configuration;
    private List<AiGenScriptResult> _currentRun;
    private readonly PatchRequest _mainScript;

    public AiGenScriptTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, AiGenConfiguration configuration) : base(database, context, null, behaviorFunctions)
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

    public override IEnumerable<AiGenScriptResult> GetTransformedResults()
    {
        return _currentRun ?? Enumerable.Empty<AiGenScriptResult>();
    }

    public override void Transform(AiEtlItem item, AiGenStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= [];

        Debug.Assert(item.IsDelete is false);
        
        DocumentScript.Run(Context, Context, "execute", [Current.Document]);
    }
    
    private JsValue AddContext(JsValue self, JsValue[] args)
    {
        const string methodDecl = "context(ctx, hash);";
        if(args.Length != 2)
            throw new InvalidOperationException($"Invalid number of arguments for {methodDecl}, got {args.Length} but expected 2.");

        if (args[0].IsObject() is false)
            throw new ArgumentException("Expected 'ctx' to be an object, but was: " + args[0].Type + ", " + args[0]);

        var context = JsBlittableBridge.Translate(Context, DocumentScript.ScriptEngine, args[0].AsObject());
        string hash = args[1].Type switch
        {
            Types.Null or Types.Undefined => null,
            Types.String => args[1].AsString(),
            _ => throw new ArgumentException($"The 'hash' argument must be string or null, but was: " + args[1].Type + ", " + args[1])
        };

        _currentRun.Add(new AiGenScriptResult(Current.DocumentId, context, hash));
        return JsValue.Null;
    }
}
