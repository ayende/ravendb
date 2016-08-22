// -----------------------------------------------------------------------
//  <copyright file="BrianVallelunga.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class BrianVallelunga : RavenTestBase
    {
        private class Profile
        {
            public string Name { get; set; }
            public string FavoriteColor { get; set; }
        }

        private class Account
        {
            public string Id { get; set; }
            public Profile Profile { get; set; }
        }

        [Fact]
        public async Task CanProjectAndSort()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   orderby a.Profile.Name
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }

        [Fact]
        public async Task CanSort()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }

        [Fact]
        public async Task CanProjectAndSort_Remote()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   orderby a.Profile.Name
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }

        [Fact]
        public async Task CanSort_Remote()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }
    }
}
