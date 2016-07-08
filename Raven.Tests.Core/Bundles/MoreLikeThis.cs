using Raven.Abstractions.Data;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Raven.Tests.Core.Utils.Transformers;
using System.Linq;
using Xunit;

namespace Raven.Tests.Core.Bundles
{
    public class MoreLikeThis : RavenCoreTestBase
    {
        [Fact]
        public void CanUseBasicMoreLikeThis()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Posts_ByTitleAndContent();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Post { Id = "posts/1", Title = "doduck", Desc = "prototype" });
                    session.Store(new Post { Id = "posts/2", Title = "doduck", Desc = "prototype your idea" });
                    session.Store(new Post { Id = "posts/3", Title = "doduck", Desc = "love programming" });
                    session.Store(new Post { Id = "posts/4", Title = "We do", Desc = "prototype" });
                    session.Store(new Post { Id = "posts/5", Title = "We love", Desc = "challange" });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var list = session.Advanced.MoreLikeThis<Post, Posts_ByTitleAndContent>(new MoreLikeThisQuery 
                        { 
                            DocumentId = "posts/1",
                            MinimumDocumentFrequency = 1,
                            MinimumTermFrequency = 0
                        });

                    Assert.Equal(3, list.Length);
                    Assert.Equal("doduck", list[0].Title);
                    Assert.Equal("prototype your idea", list[0].Desc);
                    Assert.Equal("doduck", list[1].Title);
                    Assert.Equal("love programming", list[1].Desc);
                    Assert.Equal("We do", list[2].Title);
                    Assert.Equal("prototype", list[2].Desc);
                }
            }
        }

        [Fact]
        public void CanUseMoreLikeThisWithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Posts_ByTitleAndContent();
                index.Execute(store);
                var transformer = new PostWithContentTransformer();
                transformer.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Post { Id = "posts/1", Title = "doduck", Desc = "prototype" });
                    session.Store(new Post { Id = "posts/2", Title = "doduck", Desc = "prototype your idea" });
                    session.Store(new Post { Id = "posts/3", Title = "doduck", Desc = "love programming" });
                    session.Store(new Post { Id = "posts/4", Title = "We do", Desc = "prototype" });
                    session.Store(new Post { Id = "posts/5", Title = "We love", Desc = "challange" });
                    session.Store(new Post { Id = "posts/6", Title = "We love", Desc = "challange" });
                    session.Store(new PostContent { Id = "posts/1/content", Text = "transform1" });
                    session.Store(new PostContent { Id = "posts/2/content", Text = "transform2" });
                    session.Store(new PostContent { Id = "posts/3/content", Text = "transform3" });
                    session.Store(new PostContent { Id = "posts/4/content", Text = "transform4" });
                    session.Store(new PostContent { Id = "posts/5/content", Text = "transform5" });
                    session.Store(new PostContent { Id = "posts/6/content", Text = "transform6" });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var list = session.Advanced.MoreLikeThis<PostWithContentTransformer.Result>(index.IndexName, transformer.TransformerName, new MoreLikeThisQuery
                    {
                        DocumentId = "posts/1",
                        MinimumDocumentFrequency = 1,
                        MinimumTermFrequency = 0
                    });

                    Assert.Equal(3, list.Length);
                    Assert.Equal("doduck", list[0].Title);
                    Assert.Equal("prototype your idea", list[0].Desc);
                    Assert.Equal("transform2", list[0].Content);
                    Assert.Equal("doduck", list[1].Title);
                    Assert.Equal("love programming", list[1].Desc);
                    Assert.Equal("transform3", list[1].Content);
                    Assert.Equal("We do", list[2].Title);
                    Assert.Equal("prototype", list[2].Desc);
                    Assert.Equal("transform4", list[2].Content);
                }
            }
        }

        [Fact]
        public void CanUseMoreLikeThisWithIncludes()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "users/1", AddressId = "addresses/1", Name = "John" });
                    session.Store(new User { Id = "users/2", AddressId = "addresses/2", Name = "John Doe" });
                    session.Store(new Address { Id = "addresses/1", City = "New York", Country = "USA" });
                    session.Store(new Address { Id = "addresses/2", City = "New York2", Country = "USA2" });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var list = session.Advanced.MoreLikeThis<User, Users_ByName>(new MoreLikeThisQuery
                    {
                        DocumentId = "users/1",
                        Includes = new [] {"AddressId"},
                        MinimumDocumentFrequency = 1,
                        MinimumTermFrequency = 0
                    });

                    Assert.Equal(1, list.Length);
                    Assert.Equal("John Doe", list[0].Name);

                    var address = session.Load<Address>(list[0].AddressId);
                    Assert.Equal("USA2", address.Country);
                }
            }
        }
    }
}
