using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class Contains : RavenTestBase
    {
        private class TestDoc
        {
            public string SomeProperty { get; set; }
            public string[] StringArray { get; set; }
        }

        [Fact]
        public async Task CanQueryArrayWithContains()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var otherDoc = new TestDoc { SomeProperty = "foo" };
                    var doc = session.Query<TestDoc>()
                        .FirstOrDefault(ar => ar.StringArray.Contains(otherDoc.SomeProperty));
                    Assert.NotNull(doc);
                }
            }
        }

        [Fact]
        public async Task CanQueryListWithContainsAny()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();

                    session.Query<TestDoc>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToList();

                    var items = new[] { "a", "b", "c" };
                    var test = session.Query<TestDoc>()
                                .Where(ar => ar.StringArray.ContainsAny(items) &&
                                             ar.SomeProperty == "somethingElse")
                                .ToString();
                    Assert.Equal("(StringArray:a OR StringArray:b OR StringArray:c) AND SomeProperty:somethingElse", test);

                    var results = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAny(new[] { "test", "NOTmatch" }))
                                         .ToList();
                    Assert.Equal(1, results.Count);

                    var noResults = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAny(new[] { "NOTmatch", "random" }))
                                         .ToList();
                    Assert.Equal(0, noResults.Count);
                }
            }
        }

        [Fact]
        public async Task CanQueryListWithContainsAll()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();

                    session.Query<TestDoc>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToList();

                    var items = new[] { "a", "b", "c" };
                    var test = session.Query<TestDoc>()
                                .Where(ar => ar.StringArray.ContainsAll(items) &&
                                             ar.SomeProperty == "somethingElse")
                                .ToString();
                    Assert.Equal("(StringArray:a AND StringArray:b AND StringArray:c) AND SomeProperty:somethingElse", test);

                    var results = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAll(new[] { "test", "doc", "foo" }))
                                         .ToList();
                    Assert.Equal(1, results.Count);

                    var noResults = session.Query<TestDoc>()
                                         .Where(t => t.StringArray.ContainsAll(new[] { "test", "doc", "foo", "NOTmatch" }))
                                         .ToList();
                    Assert.Equal(0, noResults.Count);
                }
            }
        }

        [Fact]
        public async Task DoesNotSupportStrings()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { SomeProperty = "Ensure that Contains on IEnumerable<Char> is not supported." };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var otherDoc = new TestDoc { SomeProperty = "Contains" };
                    var exception = Assert.Throws<NotSupportedException>(() =>
                    {
                        session.Query<TestDoc>().FirstOrDefault(ar => ar.SomeProperty.Contains(otherDoc.SomeProperty));
                    });
                    Assert.Contains("Contains is not supported, doing a substring match", exception.InnerException.Message);
                }
            }
        }
    }
}
