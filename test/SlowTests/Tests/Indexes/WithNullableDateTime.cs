// -----------------------------------------------------------------------
//  <copyright file="WithNullableDateTime.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class WithNullableDateTime : RavenTestBase
    {
        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public async Task CanCreate()
        {
            using (var documentStore = await GetDocumentStore())
            {
                new FooIndex().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { NullableDateTime = new DateTime(1989, 3, 15, 7, 28, 1, 1) });
                    session.SaveChanges();

                    Assert.NotNull(session.Query<Foo, FooIndex>()
                               .Customize(c => c.WaitForNonStaleResults())
                               .FirstOrDefault());
                }
            }
        }

        public class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map =
                    docs => from doc in docs
                            where doc.NullableDateTime != null
                            select new
                            {
                                doc.NullableDateTime.GetValueOrDefault().Date,
                            };
            }
        }

        public class Foo
        {
            public DateTime? NullableDateTime { get; set; }
        }
    }
}
