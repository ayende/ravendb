using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Actions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;
using System.IO;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using System.Collections.Generic;

namespace Raven.Database.Server.Controllers.Admin
{
    public class BenchmarkController : RavenDbApiController
    {

        [HttpGet]
        [RavenRoute("Benchmark/EmptyMessage")]
        public HttpResponseMessage EmptyMessageTest()
        {
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
    }
}
