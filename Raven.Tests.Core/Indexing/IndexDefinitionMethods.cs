using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Xunit;

namespace Raven.Tests.Core.Indexing
{
    public class IndexDefinitionMethods : RavenCoreTestBase
    {
        [Fact]
        public void CanUseMetadataFor()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_CompanyByType().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var contact1 = new Contact { FirstName = "FirstName1" };
                    var contact2 = new Contact { FirstName = "FirstName2" };
                    var contact3 = new Contact { FirstName = "FirstName3" };
                    session.SaveChanges();

                    session.Store(new Company
                    {
                        Type = Company.CompanyType.Public,
                        Contacts = new List<Contact> { contact1, contact2, contact3 }
                    });
                    session.Store(new Company
                    {
                        Type = Company.CompanyType.Public,
                        Contacts = new List<Contact> { contact3 }
                    });
                    session.Store(new Company
                    {
                        Type = Company.CompanyType.Public,
                        Contacts = new List<Contact> { contact1, contact2 }
                    });
                    session.Store(new Company
                    {
                        Type = Company.CompanyType.Private,
                        Contacts = new List<Contact> { contact1, contact2 }
                    });
                    session.Store(new Company
                    {
                        Type = Company.CompanyType.Private,
                        Contacts = new List<Contact> { contact1, contact2, contact3 }
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    Companies_CompanyByType.ReduceResult[] companies = session.Query<Companies_CompanyByType.ReduceResult, Companies_CompanyByType>()
                        .OrderBy(x => x.Type)
                        .ToArray();
                    Assert.Equal(2, companies.Length);
                    Assert.Equal(Company.CompanyType.Private, companies[0].Type);
                    Assert.Equal(5, companies[0].ContactsCount);
                    Assert.NotNull(companies[0].LastModified);
                    Assert.Equal(Company.CompanyType.Public, companies[1].Type);
                    Assert.Equal(6, companies[1].ContactsCount);
                    Assert.NotNull(companies[1].LastModified);
                }
            }
        }

        [Fact]
        public void CanUseAsDocumentToIndexAllDocumentFields()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_AllProperties().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Name2", Address1 = "Address1" });
                    session.Store(new Company { Name = "Name0", Address1 = "Some Address" });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var companies = session.Query<Companies_AllProperties.Result, Companies_AllProperties>()
                        .Where(x => x.Query == "Address1")
                        .OfType<Company>()
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    Assert.Equal("Address1", companies[0].Address1);
                }
            }
        }

        [Fact]
        public void CanUseRecurse()
        {
            using (var store = GetDocumentStore())
            {
                new Posts_Recurse().Execute(store);

                using (var session = store.OpenSession())
                {
                    var post1 = new Post { Title = "Post1", Desc = "Post1 desc" };
                    var post2 = new Post { Title = "Post2", Desc = "Post2 desc", Comments = new Post[] { post1 } };
                    var post3 = new Post { Title = "Post3", Desc = "Post3 desc", Comments = new Post[] { post2 } };
                    var post4 = new Post { Title = "Post4", Desc = "Post4 desc", Comments = new Post[] { post3 } };
                    session.Store(post4);
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var posts = session.Query<Post, Posts_Recurse>()
                        .ToArray();
                    Assert.Equal(1, posts.Length);
                    Assert.Equal("Post4", posts[0].Title);
                    Assert.Equal("Post3", posts[0].Comments[0].Title);
                    Assert.Equal("Post2", posts[0].Comments[0].Comments[0].Title);
                    Assert.Equal("Post1", posts[0].Comments[0].Comments[0].Comments[0].Title);
                }
            }
        }

        [Fact]
        public void CanUseLoadAttachmentForIndexing()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Post_LoadAttachment();
                index.Execute(store);

                store.DatabaseCommands.PutAttachment("posts/1/attachments/1", null,
                    new MemoryStream(Encoding.UTF8.GetBytes("Lorem ipsum")), new RavenJObject());

                store.DatabaseCommands.PutAttachment("posts/1/attachments/2", null,
                    new MemoryStream(Encoding.UTF8.GetBytes("dolor sit amet")), new RavenJObject());

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Id = "posts/1",
                        AttachmentIds = new[] { "posts/1/attachments/1", "posts/1/attachments/2" }
                    });

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var post = session.Advanced.DocumentQuery<Post, Post_LoadAttachment>().WhereEquals("AttachmentContent", "Lorem").First();

                    Assert.NotNull(post);

                    post = session.Advanced.DocumentQuery<Post, Post_LoadAttachment>().WhereEquals("AttachmentContent", "sit").First();

                    Assert.NotNull(post);
                }
            }
        }
    }
}
