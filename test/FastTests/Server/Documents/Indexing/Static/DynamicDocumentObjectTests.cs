﻿using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Abstractions.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class DynamicDocumentObjectTests
    {
        private readonly UnmanagedBuffersPool _pool;
        private readonly JsonOperationContext _ctx;
        private readonly List<BlittableJsonReaderObject> _docs = new List<BlittableJsonReaderObject>();

        public DynamicDocumentObjectTests()
        {
            _pool = new UnmanagedBuffersPool("foo");
            _ctx = new JsonOperationContext(_pool);
        }

        [Fact]
        public void Can_get_simple_values()
        {
            var now = SystemTime.UtcNow;

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Arek",
                ["Address"] = new DynamicJsonValue
                {
                    ["City"] = "NYC"
                },
                ["NullField"] = null,
                ["Age"] = new LazyDoubleValue(_ctx.GetLazyString("22.0")),
                ["LazyName"] = _ctx.GetLazyString("Arek"),
                ["Friends"] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        ["Name"] = "Joe"
                    },
                    new DynamicJsonValue
                    {
                        ["Name"] = "John"
                    }
                },
                [Constants.Metadata] = new DynamicJsonValue
                {
                    [Constants.Headers.RavenEntityName] = "Users",
                    [Constants.Headers.RavenLastModified] = now.GetDefaultRavenFormat(true)
                }
            }, "users/1");

            dynamic user = new DynamicDocumentObject(doc);

            Assert.Equal("Arek", user.Name);
            Assert.Equal("NYC", user.Address.City);
            Assert.Equal("users/1", user.Id);
            Assert.Equal(null, user.NullField);
            Assert.Equal(22.0, user.Age);
            Assert.Equal("Arek", user.LazyName);
            Assert.Equal(2, user.Friends.Length);
            Assert.Equal("Users", user[Constants.Metadata][Constants.Headers.RavenEntityName]);
            Assert.Equal(now, user[Constants.Metadata].Value<DateTime>(Constants.Headers.RavenLastModified));
        }

        public Document create_doc(DynamicJsonValue document, string id)
        {
            var data = _ctx.ReadObject(document, id);

            _docs.Add(data);

            return new Document
            {
                Data = data,
                Key = _ctx.GetLazyString(id)
            };
        }

        public void Dispose()
        {
            foreach (var docReader in _docs)
            {
                docReader.Dispose();
            }

            _ctx.Dispose();
            _pool.Dispose();
        }
    }
}