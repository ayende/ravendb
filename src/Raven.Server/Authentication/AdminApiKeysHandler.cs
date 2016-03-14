﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;

namespace Raven.Server.Authentication
{
    public class AdminApiKeysHandler : RequestHandler
    {
        [RavenAction("/admin/api-keys", "PUT", "/admin/api-keys?name={api-key-name:string}", SkipTryAuthorized = true)]
        public Task PutApiKey()
        {
            TransactionOperationContext ctx;
            using (ServerStore.ContextPool.AllocateOperationContext(out ctx))
            {
                var name = HttpContext.Request.Query["name"];

                if (name.Count != 1)
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync("'name' query string must have exactly one value");
                }

                var apiKey = ctx.ReadForDisk(RequestBodyStream(), name[0]);

                //TODO: Validate API Key Structure

                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Write(ctx, Constants.ApiKeyPrefix + name[0], apiKey);

                    tx.Commit();
                }
                return Task.CompletedTask;
            }
        }


        //TODO: read (+ paging) / delete / put
    }
}
