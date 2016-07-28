using System;
using System.Threading.Tasks;

using FastTests;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Session
{
    public class Advanced : RavenTestBase
    {
        [Fact]
        public async Task CanGetChangesInformation()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.HasChanges);

                    var user = new User { Id = "users/1", Name = "John" };
                    session.Store(user);

                    Assert.True(session.Advanced.HasChanged(user));
                    Assert.True(session.Advanced.HasChanges);

                    session.SaveChanges();

                    Assert.False(session.Advanced.HasChanged(user));
                    Assert.False(session.Advanced.HasChanges);

                    user.AddressId = "addresses/1";
                    Assert.True(session.Advanced.HasChanged(user));
                    Assert.True(session.Advanced.HasChanges);

                    var whatChanged = session.Advanced.WhatChanged();
                    Assert.Equal("AddressId", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldName);
                    Assert.Equal("", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldOldValue);
                    Assert.Equal("addresses/1", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldNewValue);

                    session.Advanced.Clear();
                    Assert.False(session.Advanced.HasChanges);

                    var user2 = new User { Id = "users/2", Name = "John" };
                    session.Store(user2);
                    session.Delete(user2);

                    Assert.True(session.Advanced.HasChanged(user2));
                    Assert.True(session.Advanced.HasChanges);
                }
            }
        }

        [Fact]
        public async Task CanUseEvict()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Evict(user);

                    session.Load<User>("users/1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanUseClear()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Clear();

                    session.Load<User>("users/1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanUseIsLoaded()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.IsLoaded("users/1"));

                    session.Load<User>("users/1");

                    Assert.True(session.Advanced.IsLoaded("users/1"));
                    Assert.False(session.Advanced.IsLoaded("users/2"));

                    session.Advanced.Clear();

                    Assert.False(session.Advanced.IsLoaded("users/1"));
                }
            }
        }

        [Fact]
        public async Task CanUseRefresh()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    var u = store.DatabaseCommands.Get("users/1");
                    u.DataAsJson["Name"] = "Jonathan";
                    store.DatabaseCommands.Put("users/1", u.Etag, u.DataAsJson, u.Metadata);

                    user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    session.Advanced.Refresh(user);

                    Assert.NotNull(user);
                    Assert.Equal("Jonathan", user.Name);
                }
            }
        }

        [Fact(Skip = "Missing feature: Optimistic concurrency")]
        public async Task CanUseOptmisticConcurrency()
        {
            const string entityId = "users/1";

            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.UseOptimisticConcurrency);
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user);
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Equal("PUT attempted on document '" + entityId + "' using a non current etag", e.Message);
                }
            }
        }

        [Fact]
        public async Task CanGetDocumentUrl()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = "companies/1" });
                    session.SaveChanges();

                    var company = session.Load<Company>("companies/1");
                    Assert.NotNull(company);
                    var uri = new Uri(session.Advanced.GetDocumentUrl(company));
                    Assert.Equal(store.Url + "/databases/" + store.DefaultDatabase + "/docs?id=companies/1", uri.AbsoluteUri);
                }
            }
        }

        [Fact]
        public async Task CanGetDocumentMetadata()
        {
            const string companyId = "companies/1";
            const string attrKey = "SetDocumentMetadataTestKey";
            const string attrVal = "SetDocumentMetadataTestValue";

            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    companyId,
                    null,
                    RavenJObject.FromObject(new Company { Id = companyId }),
                    new RavenJObject { { attrKey, attrVal } }
                    );

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(companyId);
                    var result = session.Advanced.GetMetadataFor<Company>(company);
                    Assert.NotNull(result);
                    Assert.Equal(attrVal, result.Value<string>(attrKey));
                }
            }
        }

        [Fact]
        public async Task CanUseNumberOfRequests()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var company = new Company();
                    company.Name = "NumberOfRequestsTest";

                    session.Store(company);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var company2 = session.Load<Company>(company.Id);
                    company2.Name = "NumberOfRequestsTest2";
                    session.Store(company2);
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanUseMaxNumberOfRequestsPerSession()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 2;

                    var company = new Company();
                    session.Store(company);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    company.Name = "1";
                    session.Store(company);
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    try
                    {
                        company.Name = "2";
                        session.Store(company);
                        session.SaveChanges();
                        Assert.False(true, "I expected InvalidOperationException to be thrown here.");
                    }
                    catch (InvalidOperationException)
                    {
                    }
                }
            }
        }

        [Fact]
        public async Task CanMarkReadOnly()
        {
            const string categoryName = "MarkReadOnlyTest";

            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = "companies/1" });
                    session.SaveChanges();

                    var company = session.Load<Company>("companies/1");
                    session.Advanced.MarkReadOnly(company);
                    company.Name = categoryName;
                    Assert.True(session.Advanced.HasChanges);

                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.True(session.Advanced.GetMetadataFor<Company>(company).Value<bool>("Raven-Read-Only"));
                }
            }
        }

        [Fact]
        public async Task CanGetEtagFor()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    "companies/1",
                    null,
                    RavenJObject.FromObject(new Company { Id = "companies/1" }),
                    new RavenJObject()
                    );

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal(1, session.Advanced.GetEtagFor(company));
                }
            }
        }

        [Fact]
        public async Task CanLazilyLoadEntity()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    COMPANY1_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY1_ID }),
                    new RavenJObject()
                    );
                store.DatabaseCommands.Put(
                    COMPANY2_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY2_ID }),
                    new RavenJObject()
                    );

                using (var session = store.OpenSession())
                {
                    Lazy<Company> lazyOrder = session.Advanced.Lazily.Load<Company>(COMPANY1_ID);
                    Assert.False(lazyOrder.IsValueCreated);
                    var order = lazyOrder.Value;
                    Assert.Equal(COMPANY1_ID, order.Id);

                    Lazy<Company[]> lazyOrders = session.Advanced.Lazily.Load<Company>(new String[] { COMPANY1_ID, COMPANY2_ID });
                    Assert.False(lazyOrders.IsValueCreated);
                    Company[] orders = lazyOrders.Value;
                    Assert.Equal(2, orders.Length);
                    Assert.Equal(COMPANY1_ID, orders[0].Id);
                    Assert.Equal(COMPANY2_ID, orders[1].Id);
                }
            }
        }

        [Fact]
        public async Task CanExecuteAllPendingLazyOperations()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    COMPANY1_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY1_ID }),
                    new RavenJObject()
                    );
                store.DatabaseCommands.Put(
                    COMPANY2_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY2_ID }),
                    new RavenJObject()
                    );

                using (var session = store.OpenSession())
                {
                    Company company1 = null;
                    Company company2 = null;

                    session.Advanced.Lazily.Load<Company>(COMPANY1_ID, x => company1 = x);
                    session.Advanced.Lazily.Load<Company>(COMPANY2_ID, x => company2 = x);
                    Assert.Null(company1);
                    Assert.Null(company2);

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(company1);
                    Assert.NotNull(company2);
                    Assert.Equal(COMPANY1_ID, company1.Id);
                    Assert.Equal(COMPANY2_ID, company2.Id);
                }
            }
        }

        [Fact]
        public async Task CanUseDefer()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var commands = new ICommandData[]
                    {
                        new PutCommandData
                        {
                            Document =
                                RavenJObject.FromObject(new Company {Name = "company 1"}),
                            Etag = null,
                            Key = "company1"
                        },
                        new PutCommandData
                        {
                            Document =
                                RavenJObject.FromObject(new Company {Name = "company 2"}),
                            Etag = null,
                            Key = "company2"
                        }
                    };

                    session.Advanced.Defer(commands);
                    session.Advanced.Defer(new DeleteCommandData { Key = "company1" });

                    session.SaveChanges();

                    Assert.Null(session.Load<Company>("company1"));
                    Assert.NotNull(session.Load<Company>("company2"));
                }
            }
        }

        [Fact]
        public async Task CanAggressivelyCacheFor()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "users/1", Name = "Name" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                    session.Load<User>("users/1");
                    Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    session.Load<User>("users/1");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    for (var i = 0; i <= 20; i++)
                    {
                        using (store.AggressivelyCacheFor(TimeSpan.FromSeconds(30)))
                        {
                            session.Load<User>("users/1");
                        }
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
