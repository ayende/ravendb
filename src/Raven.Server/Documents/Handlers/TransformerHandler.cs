﻿using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class TransformerHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/transformers", "PUT")]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), name);

                // validating transformer definition
                var transformerDefinition = JsonDeserializationServer.TransformerDefinition(json);

                // validate that this transformer compiles
                IndexAndTransformerCompilationCache.GetTransformerInstance(transformerDefinition);

                transformerDefinition.Name = name;
                long index;
                using (var putTransfomerCommand = context.ReadObject(new DynamicJsonValue
                {
                    ["Type"] = nameof(PutUpdateTransformerCommand),
                    [nameof(PutUpdateTransformerCommand.TransformerDefinition)] = json,
                    [nameof(PutUpdateTransformerCommand.DatabaseName)] = Database.Name,
                }, "put-transformer-cmd"))
                {
                    index = await ServerStore.SendToLeaderAsync(putTransfomerCommand);
                }

                await ServerStore.Cluster.WaitForIndexNotification(index);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Transformer");
                    writer.WriteString(name);
                    writer.WriteComma();

                    // todo: we probably won't need that
                    writer.WritePropertyName("Etag");
                    writer.WriteInteger(index);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/transformers", "GET")]
        public Task GetAll()
        {
            var name = GetStringQueryString("name", required: false);

            var start = GetStart();
            var pageSize = GetPageSize(Database.Configuration.Core.MaxPageSize);
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                TransformerDefinition[] transformerDefinitions;
                if (string.IsNullOrEmpty(name))
                    transformerDefinitions = Database.TransformerStore
                        .GetTransformers()
                        .OrderBy(x => x.Name)
                        .Skip(start)
                        .Take(pageSize)
                        .Select(x => x.Definition)
                        .ToArray();
                else
                {
                    var transformer = Database.TransformerStore.GetTransformer(name);
                    if (transformer == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    transformerDefinitions = new[] { transformer.Definition };
                }

                writer.WriteStartObject();
                writer.WriteResults(context, transformerDefinitions, (w, c, definition) =>
                {
                    if (namesOnly)
                    {
                        w.WriteString(definition.Name);
                        return;
                    }

                    w.WriteTransformerDefinition(c, definition);
                });

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/transformers/rename", "POST")]
        public async Task Rename()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var newName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("newName");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var existingTransformer = Database.TransformerStore.GetTransformer(name);
                var transformerBlittable = EntityToBlittable.ConvertEntityToBlittable(existingTransformer, DocumentConventions.Default, context);

                var delVal = new DynamicJsonValue
                {
                    ["Type"] = nameof(DeleteUpdateTransformerCommand),
                    [nameof(DeleteUpdateTransformerCommand.TransformerName)] = name,
                    [nameof(DeleteUpdateTransformerCommand.DatabaseName)] = Database.Name,
                };
                var putVal = new DynamicJsonValue
                {
                    ["Type"] = nameof(PutUpdateTransformerCommand),
                    [nameof(PutUpdateTransformerCommand.TransformerDefinition)] = transformerBlittable,
                    [nameof(PutUpdateTransformerCommand.DatabaseName)] = Database.Name,
                };

                using (var deleteTransformerCommand = context.ReadObject(delVal, "delete-transformer-cmd"))
                using (var putTransfomerCommand = context.ReadObject(putVal, "put-transformer-cmd"))
                {
                    var del = ServerStore.SendToLeaderAsync(deleteTransformerCommand);
                    var put = ServerStore.SendToLeaderAsync(putTransfomerCommand);
                    await Task.WhenAll(del, put);
                    var index = await put;
                    await ServerStore.Cluster.WaitForIndexNotification(index);
                }
               
                NoContentStatus();
            }
        }

        [RavenAction("/databases/*/transformers/set-lock", "POST")]
        public async Task SetLockMode()
        {
            var names = GetStringValuesQueryString("name");
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");

            TransformerLockMode mode;
            if (Enum.TryParse(modeStr, out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                long index = 0;

                foreach (var name in names)
                {
                    var transformer = Database.TransformerStore.GetTransformer(name);
                    if (transformer == null)
                        TransformerDoesNotExistException.ThrowFor(name);
                    var faultyInMemoryTransformer = transformer as FaultyInMemoryTransformer;
                    if(faultyInMemoryTransformer != null)
                        throw new NotSupportedException("Cannot change lock mode on faulty index", faultyInMemoryTransformer.Error);

                    using (var setTranformerLockModeCommand = context.ReadObject(new DynamicJsonValue
                    {
                        ["Type"] = nameof(SetUpdateTransformerLockModeCommand),
                        [nameof(SetUpdateTransformerLockModeCommand.LockMode)] = mode,
                        [nameof(SetUpdateTransformerLockModeCommand.DatabaseName)] = Database.Name,
                        [nameof(SetUpdateTransformerLockModeCommand.TransformerName)] = name,
                    }, "set-transformer_lock_mode-cmd"))
                    {
                        index = await ServerStore.SendToLeaderAsync(setTranformerLockModeCommand);
                    }
                }
                await ServerStore.Cluster.WaitForIndexNotification(index);

                NoContentStatus();
            }
        }

        [RavenAction("/databases/*/transformers", "DELETE")]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                // validating transformer definition

                long index = 0;
                using (var deleteTransformerCommand = context.ReadObject(new DynamicJsonValue
                {
                    ["Type"] = nameof(DeleteUpdateTransformerCommand),
                    [nameof(DeleteUpdateTransformerCommand.TransformerName)] = name,
                    [nameof(DeleteUpdateTransformerCommand.DatabaseName)] = Database.Name,
                }, "delete-transformer-cmd"))
                {
                    index = await ServerStore.SendToLeaderAsync(deleteTransformerCommand);
                }

                await ServerStore.Cluster.WaitForIndexNotification(index);
                NoContentStatus();
            }
        }
    }
}