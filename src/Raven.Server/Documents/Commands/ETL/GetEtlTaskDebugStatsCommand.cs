﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class GetEtlTaskDebugStatsCommand : RavenCommand<EtlTaskDebugStats[]>
{
    private readonly string[] _names;

    public GetEtlTaskDebugStatsCommand(string[] names, string nodeTag)
    {
        _names = names;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/etl/debug/stats";

        if (_names is { Length: > 0 })
        {
            for (var i = 0; i < _names.Length; i++)
                url += $"{(i == 0 ? "?" : "&")}name={Uri.EscapeDataString(_names[i])}";
        }

        var request = new HttpRequestMessage { Method = HttpMethod.Get };

        return request;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<EtlTaskDebugStatsResponse>(response).Results;
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    private sealed class EtlTaskDebugStatsResponse
    {
        public EtlTaskDebugStats[] Results { get; set; }
    }
}
