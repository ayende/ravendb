﻿using System;
using System.Linq;
using System.IO;
using System.IO.Compression;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RequestHandler).FullName);

        protected HttpContext HttpContext;
        public ServerStore ServerStore;
        public RouteMatch RouteMatch;

        public virtual void Init(RequestHandlerContext context)
        {
            HttpContext = context.HttpContext;
            ServerStore = context.ServerStore;
            RouteMatch = context.RouteMatch;
        }

        public string[] GetQueryStringValues(string key)
        {
            var items = HttpContext.Request.Query.Where(pair => pair.Key == key);
            return items.SelectMany(pair => pair.Value)
                        .Select(v => v != null ? Uri.UnescapeDataString(v) : null)
                        .ToArray();
        }

        protected Stream RequestBodyStream()
        {
            var requestBodyStream = HttpContext.Request.Body;

            if(IsGzipRequest()==false)
                return  requestBodyStream;

            var gZipStream = new GZipStream(requestBodyStream, CompressionMode.Decompress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        private bool IsGzipRequest()
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in HttpContext.Request.Headers["Content-Encoding"])
            {
                if (val == "gzip")
                    return true;
            }
            return false;
        }

        protected Stream ResponseBodyStream()
        {
            var responseBodyStream = HttpContext.Response.Body;

            if(CanAcceptGzip()==false)
                return responseBodyStream;

            HttpContext.Response.Headers["Content-Encoding"] = "gzip";
            var gZipStream = new GZipStream(responseBodyStream, CompressionMode.Compress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        private bool CanAcceptGzip()
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in HttpContext.Request.Headers["Accept-Encoding"])
            {
                if (val == "gzip")
                    return true;
            }
            return false;
        }
    }
}