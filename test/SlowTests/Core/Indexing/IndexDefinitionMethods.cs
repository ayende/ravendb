using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using FastTests;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using SlowTests.Core.Utils.Transformers;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using Contact = SlowTests.Core.Utils.Entities.Contact;
using Post = SlowTests.Core.Utils.Entities.Post;

namespace SlowTests.Core.Indexing
{
    public class IndexDefinitionMethods : RavenTestBase
    {
        [Fact]
        public async Task CanUseMetadataFor()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUseAsDocumentToIndexAllDocumentFields()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Name2", Address1 = "Address1" });
                    session.Store(new Company { Name = "Name0", Address1 = "Some Address" });
                    session.SaveChanges();

                    new Companies_AllProperties().Execute(store);

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
        public async Task CanUseRecurse()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CreateAndQuerySimpleIndexWithReferencedDocuments()
        {
            using (var store = await GetDocumentStore())
            {
                new Companies_WithReferencedEmployees().Execute(store);
                new CompanyEmployeesTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { Id = "employees/1", LastName = "Last Name 1" });
                    session.Store(new Employee { Id = "employees/2", LastName = "Last Name 2" });
                    session.Store(new Employee { Id = "employees/3", LastName = "Last Name 3" });
                    session.Store(new Company { Name = "Company", EmployeesIds = new List<string> { "employees/1", "employees/2", "employees/3" } });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var companies = session.Query<Company, Companies_WithReferencedEmployees>()
                        .TransformWith<CompanyEmployeesTransformer, Companies_WithReferencedEmployees.CompanyEmployees>()
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    Assert.Equal("Company", companies[0].Name);
                    Assert.NotNull(companies[0].Employees);
                    Assert.Equal("Last Name 1", companies[0].Employees[0]);
                    Assert.Equal("Last Name 2", companies[0].Employees[1]);
                    Assert.Equal("Last Name 3", companies[0].Employees[2]);
                }
            }
        }
    }
}
