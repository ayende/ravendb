using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using BlittableTests.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Sparrow;
using Voron;
using Voron.Tests.Bugs;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }

            public User()
            {
                Age = 33;
            }
        }
        public unsafe static void Main(string[] args)
        {
            DoStuff().Wait();
        }

        private static async Task DoStuff()
        {
            var doc = new DynamicJsonValue
            {
                ["Foo"] = "Bar",
                ["User"] = new DynamicJsonValue
                {
                    ["FirstName"] = "John",
                    ["LastName"] = "Doe"
                },
                ["Dogs"] = new DynamicJsonArray
                {
                    "Arava",
                    "Oscar"
                }
            };
            using (var pool = new UnmanagedBuffersPool("foo"))
            using (var ctx = new RavenOperationContext(pool))
            {
                using (var reader = await ctx.ReadObject(doc, "foo"))
                {
                    var propertyNames = reader.GetPropertyNames();
                    var foo = reader["Foo"];
                    var dogs = reader["Dogs"] as BlittableJsonReaderArray;
                    var user = reader["User"] as BlittableJsonReaderObject;
                    var embeddedPropertyNames = user.GetPropertyNames();
                }
            }

        }
    }
}