// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2670.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4462 : NoDisposalNeeded
    {
        [Fact]
        public void ShouldBlockDateTimeZoneHandlingEditing()
        {
            var conventions = new DocumentConvention
            {
                CustomizeJsonSerializer = serializer =>
                {
                    serializer.DateTimeZoneHandling = DateTimeZoneHandling.Utc;
                }
            };

            Assert.Throws<NotSupportedException>(() => conventions.CreateSerializer());
        }
    }
}

