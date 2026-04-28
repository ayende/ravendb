using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{

    public class CdcSinkProcessStatisticsTests : RavenTestBase
    {
        public CdcSinkProcessStatisticsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task RecordConsumeError_FirstErrorWithZeroSuccesses_ShouldNotThrow()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var stats = new CdcSinkProcessStatistics(
                processTag: "CdcSink",
                processName: "test-stats",
                notificationCenter: database.NotificationCenter);

            // Currently throws InvalidOperationException with the message
            //   "Consume error ratio is too high (errors: 1, successes: 0)..."
            // because RecordConsumeError unconditionally throws when ConsumeErrors > ConsumeSuccesses,
            // with no minimum-error-count tolerance.
            var ex = Record.Exception(() => stats.RecordConsumeError("first stream-side error"));

            Assert.Null(ex);
            Assert.Equal(1, stats.ConsumeErrors);
            Assert.False(stats.WasLatestConsumeSuccessful);
            Assert.NotNull(stats.LastConsumeErrorTime);
        }
    }
}
