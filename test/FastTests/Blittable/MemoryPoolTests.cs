﻿using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable
{
    public unsafe class MemoryPoolTests
    {
        [Fact]
        public void SerialAllocationAndRelease()
        {
            using (var pool = new UnmanagedBuffersPool(string.Empty))
            {
                var allocatedMemory = new List<AllocatedMemoryData>();
                for (var i = 0; i < 1000; i++)
                {
                    allocatedMemory.Add(pool.Allocate(i));
                }
                foreach (var data in allocatedMemory)
                {
                    pool.Return(data);
                }
            }
        }

        [Fact]
        public void ParallelAllocationAndReleaseSeperately()
        {
            using (var pool = new UnmanagedBuffersPool(string.Empty))
            {
                var allocatedMemory = new global::Sparrow.Collections.ConcurrentSet<AllocatedMemoryData>();
                Parallel.For(0, 100, x =>
                {
                    for (var i = 0; i < 10; i++)
                    {
                        allocatedMemory.Add(pool.Allocate(i));
                    }
                });

                Parallel.ForEach(allocatedMemory, item =>
                {
                    pool.Return(item);
                });
            }
        }

        [Fact]
        public void ParallelSerialAllocationAndRelease()
        {
            using (var pool = new UnmanagedBuffersPool(string.Empty))
            {
                var allocatedMemory = new BlockingCollection<AllocatedMemoryData>();
                Task.Run(() =>
                {
                    for (var i = 0; i < 100; i++)
                    {
                        allocatedMemory.Add(pool.Allocate(i));
                    }
                    allocatedMemory.CompleteAdding();
                });
                
                while (allocatedMemory.IsCompleted == false)
                {
                    AllocatedMemoryData tuple;
                    if (allocatedMemory.TryTake(out tuple, 100))
                        pool.Return(tuple);
                }
            }
        }
    }
}
