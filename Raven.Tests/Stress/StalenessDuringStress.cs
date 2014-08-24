using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Stress
{
    public class StalenessDuringStress : RavenTest
    {
        [Fact]
        public void Should_record_stalesness_correctly()
        {
            var startSlice = DateTime.UtcNow.AddYears(-10);
            using (var documentStore = NewDocumentStore())
            {
                new TimeoutDatasIndex().Execute(documentStore);

                var expected = new List<Tuple<string, DateTime>>();
                var lastExpectedTimeout = DateTime.UtcNow;
                var finishedAdding = false;

                new Thread(() =>
                {
                    var sagaId = Guid.NewGuid().ToString();
                    for (var i = 0; i < 10000; i++)
                    {
                        var td = new TimeoutData
                        {
                            SagaId = sagaId,
                            Time = DateTime.UtcNow.AddSeconds(RandomProvider.GetThreadRandom().Next(1, 20)),
                            StringData = "",
                        };

                        using (var session = documentStore.OpenSession())
                        {
                            session.Store(td);
                            session.SaveChanges();
                        }

                        expected.Add(new Tuple<string, DateTime>(td.Id, td.Time));
                        lastExpectedTimeout = (td.Time > lastExpectedTimeout) ? td.Time : lastExpectedTimeout;
                    }
                    finishedAdding = true;
                    Console.WriteLine("*** Finished adding ***");
                }).Start();

                var found = 0;
                while (!finishedAdding || startSlice < lastExpectedTimeout)
                {
                    List<Tuple<string, DateTime>> timeoutDatas;
                    using (var session = documentStore.OpenSession())
                    {
                        RavenQueryStatistics stats;
                        var now = DateTime.UtcNow;
                        timeoutDatas = session.Query<TimeoutData, TimeoutDatasIndex>()
                            .Statistics(out stats)
                            .OrderBy(t => t.Time)
                            .Where(t => t.StringData == String.Empty)
                            .Where(t => t.Time > startSlice && t.Time <= now)
                            .Select(t => new
                            {
                                t.Id,
                                t.Time
                            })
                            .Take(1024)
                            .ToList()
                            .Select(arg => new Tuple<string, DateTime>(arg.Id, arg.Time))
                            .ToList()
                            ;

                        if (!stats.IsStale)
                        {
                            // Raven claims results aren't stale. Let's check this. This may be just due to a race condition but most
                            // chances are this points at an inconsistency in reporting stale indexes.
                            Console.WriteLine(timeoutDatas.Count + " results found"); // this usually happens when 0 results are returned
                            var dbstats = documentStore.DatabaseCommands.GetStatistics();
                            Assert.False(dbstats.StaleIndexes.Any(x => x.Equals("TimeoutDatasIndex")));
                        }
                    }

                    foreach (var timeoutData in timeoutDatas)
                    {
                        if (startSlice < timeoutData.Item2)
                        {
                            startSlice = timeoutData.Item2;
                        }

                        using (var session = documentStore.OpenSession())
                        {
                            var td = session.Load<TimeoutData>(timeoutData.Item1);
                            Assert.NotNull(td); // we can assume this is not due to stale results, since we move forward with chunks
                            session.Delete(td);
                            session.SaveChanges();
                            found++;
                        }
                    }
                }
            }
        }

        class TimeoutDatasIndex : AbstractIndexCreationTask<TimeoutData>
        {
            public TimeoutDatasIndex()
            {
                Map = docs => from td in docs
                    select new
                    {td.SagaId, td.StringData, td.Time,};
            } 
        }

        static class RandomProvider
        {
            private static int seed = Environment.TickCount;

            private static readonly ThreadLocal<Random> randomWrapper = new ThreadLocal<Random>(() =>
                new Random(Interlocked.Increment(ref seed))
            );

            public static Random GetThreadRandom()
            {
                return randomWrapper.Value;
            }
        }

        class TimeoutData
        {
            public string Id { get; set; }
            public string SagaId { get; set; }
            public DateTime Time { get; set; }
            public string StringData { get; set; }
        }
    }
}
