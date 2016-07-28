﻿// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class BuildVersionHandler : RequestHandler
    {
        private static readonly Lazy<byte[]> VersionBuffer = new Lazy<byte[]>(GetVersionBuffer);

        private static byte[] GetVersionBuffer()
        {
            using (var pool = new UnmanagedBuffersPool("build/version"))
            using (var context = new JsonOperationContext(pool))
            {
                var stream = new MemoryStream();
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["BuildVersion"] = ServerVersion.Build,
                        ["ProductVersion"] = ServerVersion.Version,
                        ["CommitHash"] = ServerVersion.CommitHash
                    });
                }
                var versionBuffer = stream.ToArray();
                return versionBuffer;
            }
        }

        [RavenAction("/build/version", "GET", "build-version", NoAuthorizationRequired = true)]
        public async Task Get()
        {
            var versionBuffer = VersionBuffer.Value;
            await ResponseBodyStream().WriteAsync(versionBuffer, 0, versionBuffer.Length);
        }
    }
}