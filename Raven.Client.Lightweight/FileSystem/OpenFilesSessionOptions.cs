﻿using System.Net;

namespace Raven.Client.FileSystem
{
    public class OpenFilesSessionOptions
    {
        public string FileSystem { get; set; }
        public ICredentials Credentials { get; set; }
        public string ApiKey { get; set; }
    }
}
