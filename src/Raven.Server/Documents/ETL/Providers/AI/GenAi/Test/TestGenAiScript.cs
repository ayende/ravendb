using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Test
{
    public sealed class TestGenAiScript : TestEtlScript<GenAiConfiguration, AiConnectionString>
    {
        public List<GenAiResultItem> Results { get; set; }

        public bool CreateContextObjects { get; set; } = true;

        public bool SendToModel { get; set; } = true;

        public bool ApplyUpdateScript { get; set; } = true;

    }
}
