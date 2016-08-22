using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class Gal : RavenTestBase
    {
        private class BlogPost
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
        }

        [Fact(Skip = "TODO arek: how to handle queries with @in<__document_id>() only")]
        public async Task UsingInQuery()
        {
            var id1 = Guid.Parse("00000000-0000-0000-0000-000000000001");
            var id2 = Guid.Parse("00000000-0000-0000-0000-000000000002");
            var id3 = Guid.Parse("00000000-0000-0000-0000-000000000003");

            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Id = id1,
                        Name = "one",
                    });
                    session.Store(new BlogPost
                    {
                        Id = id2,
                        Name = "two"
                    });
                    session.Store(new BlogPost
                    {
                        Id = id3,
                        Name = "three"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var myGroupOfIds = new[] { id1, id3 };

                    var goodResult = session.Query<BlogPost>()
                        .Where(i => i.Id.In(myGroupOfIds)).ToArray();

                    Assert.Equal(2, goodResult.Select(i => i.Name).ToArray().Length);

                    RavenQueryStatistics stats;
                    var badResult = session.Query<BlogPost>()
                        .Statistics(out stats)
                        .Where(i => i.Id.In(myGroupOfIds)).Select(i => new { i.Name }).ToArray();

                    Assert.Equal(2, badResult.Select(i => i.Name).ToArray().Length);

                }
            }
        }
    }
}
