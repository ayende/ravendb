﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Server.Json;
using Sparrow;
using Xunit;

namespace BlittableTests
{
    public unsafe class UnmanagedStreamTests
    {
        [Fact]
        public void EnsureSingleChunk()
        {
            using (var unmanagedByteArrayPool = new UnmanagedBuffersPool(string.Empty))
            using (var ctx = new RavenOperationContext(unmanagedByteArrayPool))
            {
                var newStream = ctx.GetStream("tst");
                var buffer = new byte[1337];
                new Random(1337).NextBytes(buffer);
                fixed (byte* p = buffer)
                {
                    for (int i = 0; i < 7; i++)
                    {
                        newStream.Write(p, buffer.Length);
                    }
                }
                byte* ptr;
                int size;
                newStream.EnsureSingleChunk(out ptr, out size);

                buffer = new byte[buffer.Length*7];
                fixed (byte* p = buffer)
                {
                    newStream.CopyTo(p);

                    Assert.Equal(size, newStream.SizeInBytes);
                    Assert.Equal(0, Memory.Compare(p, ptr, size));
                }
            }
        }

        [Fact]
        public void BulkWriteAscendingSizeTest()
        {
            using (var unmanagedByteArrayPool = new UnmanagedBuffersPool(string.Empty))
                using(var ctx = new RavenOperationContext(unmanagedByteArrayPool))
            {
                var allocatedMemory = new List<UnmanagedBuffersPool.AllocatedMemoryData>();
                var newStream = ctx.GetStream("tst");
                var totalSize = 0;
                var rand = new Random();
                for (var i = 1; i < 5000; i+=500)
                {
                    var pointer = ctx.GetMemory(rand.Next(1, i * 7));
                    totalSize += pointer.SizeInBytes;
                    FillData((byte*)pointer.Address, pointer.SizeInBytes);
                    allocatedMemory.Add(pointer);
                    newStream.Write((byte*)pointer.Address, pointer.SizeInBytes);
                }
                var buffer = ctx.GetMemory(newStream.SizeInBytes);

                var copiedSize = newStream.CopyTo((byte*)buffer.Address);
                Assert.Equal(copiedSize, newStream.SizeInBytes);

                var curIndex = 0;
                var curTuple = 0;
                foreach (var tuple in allocatedMemory)
                {
                    curTuple++;
                    for (var i = 0; i < tuple.SizeInBytes; i++)
                    {
                        Assert.Equal(*((byte*)buffer.Address + curIndex), *((byte*)((byte*)tuple.Address + i)));
                        curIndex++;
                    }

                    unmanagedByteArrayPool.Return(tuple);
                }
                unmanagedByteArrayPool.Return(buffer);
            }
        }


        [Fact]
        public void BigAlloc()
        {
            var size = 3917701;
           
            using (var unmanagedByteArrayPool = new UnmanagedBuffersPool(string.Empty))
            using (var ctx = new RavenOperationContext(unmanagedByteArrayPool))
            {
                var data = ctx.GetMemory(size);
                ctx.ReturnMemory(data);
            }
        }

        [Fact]
        public void BulkWriteDescendingSizeTest()
        {
            using (var unmanagedByteArrayPool = new UnmanagedBuffersPool(string.Empty))
            using(var ctx = new RavenOperationContext(unmanagedByteArrayPool))
            {
                var allocatedMemory = new List<UnmanagedBuffersPool.AllocatedMemoryData>();
                var newStream = ctx.GetStream("tst");
                var rand = new Random();
                for (var i = 5000; i > 1; i-=500)
                {
                    var pointer = ctx.GetMemory(rand.Next(1, i * 7));
                    FillData((byte*)pointer.Address, pointer.SizeInBytes);
                    allocatedMemory.Add(pointer);
                    newStream.Write((byte*)pointer.Address, pointer.SizeInBytes);
                }

                var buffer = ctx.GetMemory(newStream.SizeInBytes);

                var copiedSize = newStream.CopyTo((byte*)buffer.Address);
                Assert.Equal(copiedSize, newStream.SizeInBytes);

                var curIndex = 0;
                foreach (var tuple in allocatedMemory)
                {
                    for (var i = 0; i < tuple.SizeInBytes; i++)
                    {
                        Assert.Equal(*((byte*)buffer.Address + curIndex), *((byte*)((byte*)tuple.Address+ i)));
                        curIndex++;
                    }

                    ctx.ReturnMemory(tuple);
                }
                ctx.ReturnMemory(buffer);
            }
        }

        [Fact]
        public void SingleByteWritesTest()
        {
            using (var unmanagedByteArrayPool = new UnmanagedBuffersPool(string.Empty))
            using (var ctx = new RavenOperationContext(unmanagedByteArrayPool))
            {
                var allocatedMemory = new List<UnmanagedBuffersPool.AllocatedMemoryData>();
                var newStream = ctx.GetStream("tst");
                var rand = new Random();
                for (var i = 1; i < 5000; i+=500)
                {
                    var pointer = ctx.GetMemory(rand.Next(1, i*7));
                    FillData((byte*)pointer.Address, pointer.SizeInBytes);
                    allocatedMemory.Add(pointer);
                    for (var j = 0; j < pointer.SizeInBytes; j++)
                    {
                        newStream.WriteByte(*((byte*)pointer.Address + j));
                    }
                }

                var buffer = ctx.GetMemory(newStream.SizeInBytes);

                try
                {
                    var copiedSize = newStream.CopyTo((byte*)buffer.Address);
                    Assert.Equal(copiedSize, newStream.SizeInBytes);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                var curIndex = 0;
                var curTuple = 0;
                foreach (var tuple in allocatedMemory)
                {
                    curTuple++;
                    for (var i = 0; i < tuple.SizeInBytes; i++)
                    {
                        try
                        {
                            Assert.Equal(*((byte*)buffer.Address + curIndex), *((byte*) ((byte*)tuple.Address+ i)));
                            curIndex++;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }

                    ctx.ReturnMemory(tuple);
                }
                ctx.ReturnMemory(buffer);
            }
        }

        private void FillData(byte* ptr, int size)
        {
            for (var i = 0; i < size; i++)
            {
                *ptr = (byte) (i%4);
                ptr++;
            }
        }
    }
}