﻿using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class RavenDB5743 : RavenTestBase
    {
        public RavenDB5743(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions, LicenseRequired = true)]
        public async Task WillNotFilterMetadataPropertiesStartingWithAt()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company, "users/1");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata["@foo"] = "bar";
                    metadata["custom-info"] = "should be there";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(company3);
                    Assert.True(metadata.ContainsKey("@foo"));
                    Assert.Equal("should be there", metadata.GetString("custom-info"));
                }
            }
        }
    }
}
