﻿using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public unsafe class UnmanageJsonReaderTests
    {
        [Theory]
        [MemberData(nameof(Samples))]
        public void CanReadAll(string name)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var stream = typeof(UnmanageJsonReaderTests).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            using (var parser = new UnmanagedJsonParser(ctx, new JsonParserState(), "test"))
            {
                var buffer = new byte[4096];
                while (stream.Position != stream.Length)
                {
                    var read = stream.Read(buffer, 0, buffer.Length);
                    parser.SetBuffer(buffer, read);
                    while (parser.Read())
                    {
                        
                    }
                }
            }
        }

        public static IEnumerable<object[]> Samples()
        {
            var assembly = typeof(BlittableFormatTests).GetTypeInfo().Assembly;

            foreach (var name in assembly.GetManifestResourceNames())
            {
                if (Path.GetExtension(name) == ".json")
                {
                    yield return new object[] { name };
                }
            }
        }

        [Theory]
        [MemberData(nameof(InvalidJsons))]
        public void FailsOnInvalidJson(string invalidJson)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = Encoding.UTF8.GetBytes(invalidJson);
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    parser.SetBuffer(buffer, buffer.Length);
                    var writer = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "test", parser, state);

                    writer.ReadObject();
                    Assert.Throws<InvalidDataException>(() => writer.Read());
                }
            }
        }


        public static IEnumerable<object[]> InvalidJsons()
        {
            return new List<object[]>
            {
                new object[] { "sssssssssssssssss{\"Name\":\"Oren\"}" },
                new object[] { "nnnnnnnnnn{\"Name\":\"Oren\"}" }
            };
        }
    }
}