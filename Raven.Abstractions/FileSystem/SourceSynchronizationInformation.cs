﻿using System;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.FileSystem
{
    public class SourceSynchronizationInformation
    {
        public Etag LastSourceFileEtag { get; set; }
        public string SourceServerUrl { get; set; }
        public Guid DestinationServerId { get; set; }

        public override string ToString()
        {
            return string.Format("LastSourceFileEtag: {0}", LastSourceFileEtag);
        }
    }
}
