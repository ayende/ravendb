﻿using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class QueriesDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/queries/running", "GET")]
        public Task RunningQueries()
        {
            var indexes = Database
                .IndexStore
                .GetIndexes()
                .ToList();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                foreach (var index in indexes)
                {
                    writer.WritePropertyName(index.Name);
                    writer.WriteStartArray();

                    var isFirstInternal = true;
                    foreach (var query in index.CurrentlyRunningQueries)
                    {
                        if (isFirstInternal == false)
                            writer.WriteComma();

                        isFirstInternal = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName((nameof(query.Duration)));
                        writer.WriteString(query.Duration.ToString());
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.QueryId)));
                        writer.WriteInteger(query.QueryId);
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.StartTime)));
                        writer.WriteString(query.StartTime.GetDefaultRavenFormat());
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.QueryInfo)));
                        writer.WriteIndexQuery(context, query.QueryInfo);

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
    }
}