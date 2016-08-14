using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.SlowTests.Indexes
{
    public class MapReduceIndexOnLargeDataSet : RavenTestBase
    {
        [Fact]
        public async Task WillNotProduceAnyErrors()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Maps = { "from x in docs.Users select new { x.Name, Count = 1}" },
                    Reduce = "from r in results group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x=>x.Count) }"
                });

                for (int i = 0; i < 200; i++)
                {
                    using (var s = store.OpenSession())
                    {
                        for (int j = 0; j < 25; j++)
                        {
                            s.Store(new User { Name = "User #" + j });
                        }
                        s.SaveChanges();
                    }
                }

                using (var s = store.OpenSession())
                {
                    var ret = s.Query<User>("test")
                               .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(1)))
                               .ToArray();
                    Assert.Equal(25, ret.Length);
                    foreach (var x in ret)
                    {
                        try
                        {
                            Assert.Equal(200, x.Count);
                        }
                        catch (Exception)
                        {
                            PrintServerErrors(store.DatabaseCommands.GetIndexErrors());

                            var missed = ret.Where(item => item.Count != 200)
                                .Select(item => "Name: " + item.Name + ". Count: " + item.Count)
                                .ToList();
                            Console.WriteLine("Missed documents: ");
                            Console.WriteLine(string.Join(", ", missed));

                            throw;
                        }
                    }
                }

                var indexErrors = store.DatabaseCommands.GetIndexErrors().SelectMany(x => x.Errors);
                Assert.Empty(indexErrors);
            }
        }

        protected static void PrintServerErrors(IndexErrors[] indexErrors)
        {
            if (indexErrors.Any())
            {
                Console.WriteLine("Index errors count: " + indexErrors.SelectMany(x => x.Errors).Count());
                foreach (var indexError in indexErrors)
                {
                    Console.WriteLine("Index error for: " + indexError.Name);
                    foreach (var error in indexError.Errors)
                    {
                        Console.WriteLine("Index error: " + error);
                    }
                }
            }
            else
                Console.WriteLine("No server errors");
        }
    }
}
