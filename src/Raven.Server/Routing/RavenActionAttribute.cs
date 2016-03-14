using System;
using System.Net.Http;

namespace Raven.Server.Routing
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class RavenActionAttribute : Attribute
    {
        public string Path { get; }

        public string Method { get; }

        public string Description { get; }

        public bool SkipTryAuthorized { get; set; } // "NeverSecret"

        public bool IgnoreDbRoute { get; set; }

        public RavenActionAttribute(string path, string method, string description = null)
        {
            Path = path;
            Method = method;
            Description = description;
        }
    }
}