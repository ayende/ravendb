/*using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Replication;
using Xunit.Extensions;

namespace Raven.Tests.TimeSeries
{
    public class TimeSeriesLoadBalancingTests : RavenBaseTimeSeriesTest
    {
        [Theory]
        [InlineData(6)]
        [InlineData(9)]
        [InlineData(30)]
        public async Task When_replicating_can_do_read_striping(int requestCount)
        {
            using (var serverA = GetNewServer(8077))
            using (var serverB = GetNewServer(8076))
            using (var serverC = GetNewServer(8075))
            {
                using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA))
                using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
                using (var ravenStoreC = NewRemoteDocumentStore(ravenDbServer: serverC))
                {
                    using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreA))
                    using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreB))
                    using (var storeC = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, ravenStore: ravenStoreC))
                    {
                        storeA.TimeSeriesConvention.FailoverBehavior = FailoverBehavior.ReadFromAllServers;
                        await SetupReplicationAsync(storeA, storeB, storeC);

                        //make sure we get replication nodes info
                        await storeA.ReplicationInformer.UpdateReplicationInformationIfNeededAsync();

                        serverA.Server.ResetNumberOfRequests();
                        serverB.Server.ResetNumberOfRequests();
                        serverC.Server.ResetNumberOfRequests();
                        for (int i = 0; i < requestCount; i++)
                            await storeA.ChangeAsync("group", "time series", 2);

                        serverA.Server.NumberOfRequests.Should().BeGreaterThan(0);
                        serverB.Server.NumberOfRequests.Should().BeGreaterThan(0);
                        serverC.Server.NumberOfRequests.Should().BeGreaterThan(0);
                    }
                }
            }
        }
    }
}*/
