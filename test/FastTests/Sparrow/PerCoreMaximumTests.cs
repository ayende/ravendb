using System.Linq;
using System.Threading.Tasks;
using Sparrow.Server.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class PerCoreMaximumTests : NoDisposalNeeded
    {
        public PerCoreMaximumTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void RaisesButDoesNotLower()
        {
            var maximum = new PerCoreMaximum(10);
            Assert.Equal(10, maximum.Value);

            maximum.Raise(5);
            Assert.Equal(10, maximum.Value);

            maximum.Raise(20);
            Assert.Equal(20, maximum.Value);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ResetCanLower()
        {
            var maximum = new PerCoreMaximum(10);
            maximum.Raise(20);

            maximum.Reset(long.MinValue);
            Assert.Equal(long.MinValue, maximum.Value);

            maximum.Raise(3);
            Assert.Equal(3, maximum.Value);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ValueIsTheMaximumOfAllTheCores()
        {
            var maximum = new PerCoreMaximum(0);

            Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 16 }, i =>
            {
                for (int j = 1; j <= 1000; j++)
                    maximum.Raise(i * 1000 + j);
            });

            Assert.Equal(64_000, maximum.Value);
        }
    }
}
