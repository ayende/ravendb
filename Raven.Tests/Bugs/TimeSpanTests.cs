using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class TimeSpanTests : RavenTestBase
    {
        public class Foo
        {
            public string Id { get; set; }
            public TimeSpan? Start { get; set; }
            public TimeSpan Until { get; set; }
        }

        public class Bar
        {
            public string Id { get; set; }
            public string SomeData { get; set; }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Under_A_Day()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(20) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(15);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Over_A_Day()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(30), Until = TimeSpan.FromHours(40) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(35);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Mixed_Days()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(20), Until = TimeSpan.FromHours(30) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(25);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_VeryLarge()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromDays(100) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromDays(2);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Negatives()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(-10), Until = TimeSpan.FromHours(10) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(1);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void Can_Sort_On_TimeSpans()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Start = TimeSpan.FromSeconds(10) });
                    session.Store(new Foo { Id = "2", Start = TimeSpan.FromSeconds(20) });
                    session.Store(new Foo { Id = "3", Start = TimeSpan.FromSeconds(15) });

                    session.SaveChanges();
                }

                WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .OrderBy(x => x.Start)
                        .ToArray();

                    Assert.Equal("1", results[0].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[2].Id);
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .OrderByDescending(x => x.Start)
                        .ToArray();

                    Assert.Equal("1", results[2].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[0].Id);
                }
            }
        }

        [Fact]
        public void Can_Sort_On_TimeSpans_With_Nulls()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Start = TimeSpan.FromSeconds(10) });
                    session.Store(new Foo { Id = "2", Start = TimeSpan.FromSeconds(20) });
                    session.Store(new Foo { Id = "3", Start = TimeSpan.FromSeconds(15) });
                    session.Store(new Foo { Id = "4", Start = null});

                    session.SaveChanges();
                }

                WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .OrderBy(x => x.Start)
                        .ToArray();

                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .OrderByDescending(x => x.Start)
                        .ToArray();

                    Assert.Equal("4", results[3].Id);
                    Assert.Equal("1", results[2].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[0].Id);
                }
            }
        }

        [Fact]
        public void Can_Sort_On_TimeSpans_With_Nulls_Using_MultiMap_Idx()
        {
            using (var documentStore = NewDocumentStore())
            {
                new TimeSpanTestMultiMapIndex().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Start = TimeSpan.FromSeconds(10) });
                    session.Store(new Foo { Id = "2", Start = TimeSpan.FromSeconds(20) });
                    session.Store(new Foo { Id = "3", Start = TimeSpan.FromSeconds(15) });
                    session.Store(new Bar { Id = "4" });

                    session.SaveChanges();
                }

                WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo, TimeSpanTestMultiMapIndex>()
                        .OrderBy(x => x.Start)
                        .AsProjection<Foo>()
                        .ToArray();

                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo, TimeSpanTestMultiMapIndex>()
                        .OrderByDescending(x => x.Start)
                        .AsProjection<Foo>()
                        .ToArray();

                    Assert.Equal("4", results[3].Id);
                    Assert.Equal("1", results[2].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[0].Id);
                }
            }
        }

        protected class TimeSpanTestMultiMapIndex : AbstractMultiMapIndexCreationTask<Foo>
        {
            public TimeSpanTestMultiMapIndex()
            {
                AddMap<Foo>(docs => from d in docs
                                    select new Foo
                                    {
                                        Start = d.Start,
                                    });

                AddMap<Bar>(docs => from d in docs
                                    select new Foo
                                    {
                                        Start = null,
                                    });
            }
        }
    }
}
