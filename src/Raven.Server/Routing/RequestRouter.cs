﻿// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Document;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Routing
{
    public class RequestRouter
    {
        private readonly Trie<RouteInformation> _trie;

        public RequestRouter(Dictionary<string, RouteInformation> routes)
        {
            _trie = Trie<RouteInformation>.Build(routes);
        }

        public async Task HandlePath(HttpContext context)
        {
            var tryMatch = _trie.TryMatch(context.Request.Method, context.Request.Path);
            if (tryMatch.Match.Success == false)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync($"There is no handler for path: {context.Request.Path}, method used: {context.Request.Method}");
                return;
            }

            var serverStore = context.ApplicationServices.GetRequiredService<ServerStore>();
            var reqCtx = new RequestHandlerContext
            {
                HttpContext = context,
                ServerStore = serverStore,
                RouteMatch = tryMatch.Match,
            };

            var handler = await tryMatch.Value.CreateHandler(reqCtx);
            if (handler == null)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("There is no handler for path: " + context.Request.Path + " with method: " + context.Request.Method);
                return;
            }

            await handler(reqCtx);
        }
    }
}