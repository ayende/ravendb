using System.Runtime.InteropServices;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Graphs;

public class BasicGraphs : StorageTest
{
    public BasicGraphs(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateEmptyGraph()
    {
        long id;
        using (var txw = Env.WriteTransaction())
        {
            id = Hnsw.Create(txw.LowLevelTransaction, 16, 3, 12);

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var options = Hnsw.ReadOptions(txr.LowLevelTransaction, id);
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfNeighbors);
            Assert.Equal(0, options.CountOfItems);
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void AddOneItem()
    {
        float[] f = [0.1f, 0.2f, 0.3f, 0.4f];
        long  id;
  
        using (var txw = Env.WriteTransaction())
        {
            id = Hnsw.Create(txw.LowLevelTransaction, 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, id))
            {
                registration.Register(1, MemoryMarshal.Cast<float, byte>(f));
            }
            
            txw.Commit();
        }
        
        using (var txr = Env.ReadTransaction())
        {
            var options = Hnsw.ReadOptions(txr.LowLevelTransaction, id);
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfNeighbors);
            Assert.Equal(1, options.CountOfItems);
        }
    }
}
