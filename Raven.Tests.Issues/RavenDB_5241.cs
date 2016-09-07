// -----------------------------------------------------------------------
//  <copyright file="RavenDB_5241.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5241 : RavenTestBase
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                new TestDocumentTransformer().Execute(store);

                const string document1Id = "TestDocuments/1";
                const string document2Id = "TestDocuments/2";
                using (var session = store.OpenSession())
                {
                    var existing1 = session.Load<TestDocument>(document1Id);
                    if (existing1 == null)
                        session.Store(new TestDocument { Id = document1Id, Value = 1 });

                    var existing2 = session.Load<TestDocument>(document2Id);
                    if (existing2 == null)
                        session.Store(new TestDocument { Id = document2Id, Value = 2 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Load<TestDocumentTransformer, TestDocumentTransformer.Output>(new[] { document1Id, document1Id, document2Id, document1Id, document2Id });

                    Assert.Equal(5, docs.Length);
                    Assert.Equal(document1Id, docs[0].DocumentKey);
                    Assert.Equal(document1Id, docs[1].DocumentKey);
                    Assert.Equal(document2Id, docs[2].DocumentKey);
                    Assert.Equal(document1Id, docs[3].DocumentKey);
                    Assert.Equal(document2Id, docs[4].DocumentKey);
                }
            }
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public int Value { get; set; }
        }

        private class TestDocumentTransformer : AbstractTransformerCreationTask<TestDocument>
        {
            public class Output
            {
                public string DocumentKey { get; set; }
            }

            public TestDocumentTransformer()
            {
                TransformResults = results =>
                    from result in results
                    select new Output
                    {
                        DocumentKey = result.Id
                    };
            }
        }
    }
}