﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public abstract class RavenCommand<TResult>
    {
        public CancellationToken CancellationToken = CancellationToken.None;

        public HashSet<ServerNode> FailedNodes;

        public TResult Result;
        public int AuthenticationRetries;
        public bool IsReadRequest = true;

        public abstract HttpRequestMessage CreateRequest(out string url);
        public abstract void SetResponse(BlittableJsonReaderObject response);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected string UrlEncode(string value)
        {
            return WebUtility.UrlEncode(value);
        }

        public static void EnsureIsNotNullOrEmpty(string value, string name)
        {
            if (string.IsNullOrEmpty(value))
                throw new ArgumentException($"{name} cannot be null or empty", name);
        }

        public bool IsFailedWithNode(ServerNode leaderNode)
        {
            return FailedNodes != null && FailedNodes.Contains(leaderNode);
        }
    }
}