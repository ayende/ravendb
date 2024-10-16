﻿// -----------------------------------------------------------------------
//  <copyright file="WhereClauseTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class WhereClauseTest : RavenTestBase
    {
        public WhereClauseTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ATest(Options options)
        {
            using (var ds = GetDocumentStore(options))
            {
                using (IDocumentSession session = ds.OpenSession())
                {
                    session.Store(new TestEntity(int.MaxValue));
                    session.SaveChanges();
                }


                using (IDocumentSession qSession = ds.OpenSession())
                {
                    var entities = qSession.Query<TestEntity>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.IntType > 0)
                        .ToList();

                    Assert.True(entities.Count > 0);
                }
            }
        }

        private class TestEntity
        {
            public TestEntity(int intValue)
            {
                IntType = intValue;
            }

            public string Id { get; set; }
            public int IntType { get; set; }
        }
    }
}
