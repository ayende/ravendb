using System;
using System.Collections.Generic;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.VisualBasic;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Container = Voron.Data.Containers.Container;

namespace Voron.Data.Graphs;

public unsafe class Hnsw
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 64)]
    public struct Options
    {
        [FieldOffset(0)]
        public int VectorSizeBytes;

        [FieldOffset(4)] // parameter M in the algorithm
        public int NumberOfNeighbors;

        [FieldOffset(8)] // parameter efConstruction in the algorithm
        public int NumberOfCandidates;

        [FieldOffset(12)] // this is used only in debug, not important for persistence
        public int Version;  
        
        [FieldOffset(16)]
        public long EntryPointId;

        [FieldOffset(24)]
        public long VectorsContainer;

        [FieldOffset(32)]
        public long NodesContainer;

        [FieldOffset(40)]
        public long Maintenance;
        
        [FieldOffset(48)]
        public long CountOfItems;
        
        [FieldOffset(56)]
        public long Reserved;
    }

    public ref struct NodeReader(Span<byte> buffer)
    {
        public long VectorId;
        public int CountOfLevels;
        public Span<byte> Buffer = buffer;

        public NodeLevelIterator Read(int level)
        {
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(level < 0 || level > CountOfLevels);
            int size = 0;
            int offset = -1;
            for (int i = 0; i <= level; i++)
            {
                size += VariableSizeEncoding.ReadReverse(Buffer[..^size], out offset);
            }

            int count = VariableSizeEncoding.Read<int>(Buffer[offset..], out int levelOffset);
            return new NodeLevelIterator(Buffer[(offset + levelOffset)..], count);
        } 
    }

    public ref struct NodeLevelIterator(Span<byte> buffer, int count)
    {
        private Span<byte> _buffer = buffer;
        private int _index = 0;

        public bool MoveNext(out long item)
        {
            if (_index == count)
            {
                item = -1;
                return false;
            }
            item = VariableSizeEncoding.Read<long>(_buffer, out int offset);
            _buffer = _buffer[offset..];
            _index++;
            return true;
        }
    }

    /// <summary>
    /// Format is:
    /// varint64 - EntryId
    /// ------
    /// repeated:
    /// - varint32 count of items
    /// - delta encoded varint64
    /// ------
    /// offsets for each level - reverse varint32
    /// varint32 - number of levels
    /// reverse varint64 - vector id
    /// </summary>
    private struct Node
    {
        public long EntryId;
        public long VectorId;
        public NativeList<NativeList<long>> NeighborsPerLevel;

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.Get(llt, id).ToSpan();
            var size = VariableSizeEncoding.ReadReverse(span, out long vectorId);
            size += VariableSizeEncoding.ReadReverse(span[..^size], out int countOfLevels);
            
            return new NodeReader(span[..^size])
            {
                VectorId = vectorId,
                CountOfLevels = countOfLevels
            };
        }
        
        [SkipLocalsInit]
        public Span<byte> Encode(ByteStringContext bsc, ref ByteString buffer, ref ByteStringContext.InternalScope scope)
        {
            Span<int> offsets = stackalloc int[64];
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>(NeighborsPerLevel.Count > 64, 
                "NeighborsPerLevel.Count > 64 - shouldn never be possible, meaning 64 levels!");

            Span<byte> tmp = stackalloc byte[256 * 10]; // max number of neighbors * 10 bytes max encoding size  

            int globalPos = VariableSizeEncoding.Write(tmp, EntryId);

            int countOfLevels = NeighborsPerLevel.Count;
            
            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = NeighborsPerLevel[i].ToSpan();
                var num = Sorting.SortAndRemoveDuplicates(span);
                int runPos = 0;
                long prev = 0;
                runPos += VariableSizeEncoding.Write(tmp, num, runPos);
                for (int j = 0; j < num; j++)
                {
                    var delta = span[j] - prev;
                    runPos += VariableSizeEncoding.Write(tmp, delta, runPos);
                }
                while (globalPos + runPos > buffer.Length)
                {
                    bsc.GrowAllocation(ref buffer, ref scope, Bits.PowerOf2(buffer.Length + 1));
                }
                tmp[..runPos].CopyTo(buffer.ToSpan()[globalPos..]);
                offsets[i] = globalPos;
                globalPos += runPos;
            }
            
            while (globalPos + (countOfLevels + 2 * 10) > buffer.Length)
            {
                bsc.GrowAllocation(ref buffer, ref scope, Bits.PowerOf2(buffer.Length + 1));
            }

            var bufferSpan = buffer.ToSpan();
            for (int i = countOfLevels - 1; i >= 0; i--)
            {
                globalPos += VariableSizeEncoding.WriteReverse(bufferSpan[globalPos..], offsets[i]);
            }
            globalPos += VariableSizeEncoding.WriteReverse(bufferSpan[globalPos..], countOfLevels);
            globalPos += VariableSizeEncoding.WriteReverse(bufferSpan[globalPos..], VectorId);

            return bufferSpan[..globalPos];
        }
        
    }
    
    private struct NodeMaintenance
    {
        public long NodeId;
        public float MinimumDistance;
        public byte Neighbors;

        public const int MaxBufferSize = 32;

        public int Encode(Span<byte> buffer)
        {
            int offset = VariableSizeEncoding.Write(buffer, NodeId);
            
            MemoryMarshal.Write(buffer[offset..], (Half)MinimumDistance);
            offset += sizeof(Half);
            buffer[offset++] = Neighbors;

            return offset;
        }

        public static NodeMaintenance Decode(ReadOnlySpan<byte> buffer)
        {
            int pos = 0;
            var nodeId = VariableSizeEncoding.Read<long>(buffer, out int offset, pos);
            pos += offset;
            var minimumDistance = (float)MemoryMarshal.Read<Half>(buffer[pos..]);
            pos += sizeof(Half);
            var neighbors = buffer[pos];

            return new NodeMaintenance
            {
                NodeId = nodeId,
                MinimumDistance = minimumDistance,
                Neighbors = neighbors
            };
        }
    }
    
    
    public static long Create(LowLevelTransaction llt, int vectorSizeBytes, int numberOfNeighbors, int numberOfCandidates)
    {
        long vectors = Container.Create(llt);
        long nodes = Container.Create(llt);
        long maintenance = Container.Create(llt);
        

        var id = Container.Allocate(llt, maintenance, sizeof(Options), out var span);
        MemoryMarshal.AsRef<Options>(span) = new Options
        {
            NodesContainer = nodes,
            VectorsContainer = vectors,
            EntryPointId = -1,
            NumberOfCandidates = numberOfCandidates,
            Version = 1,
            NumberOfNeighbors = numberOfNeighbors,
            VectorSizeBytes = vectorSizeBytes,
            Maintenance = maintenance,
            CountOfItems = 0,
        };

        return id;
    }

    public struct Reader(LowLevelTransaction Llt, Options Options)
    {
        public int MaxLevel = BitOperations.Log2((uint)Options.CountOfItems) + 1;
        
        public long Search(Span<byte> vector)
        {
            var current = Options.EntryPointId;
            
            var reader = Node.Decode(Llt, current);
            var bestDistance = Distance(vector, reader.VectorId);  
            while (level >= 0)
            {
                NodeLevelIterator iterator = reader.Read(level);
                while (iterator.MoveNext(out var nodeId))
                {
                
                }
            }
        }
    }

    private static float Distance(in Options options, Span<byte> vector, long otherVectorId)
    {
        
        TensorPrimitives.CosineSimilarity()
    }

    

    public struct Registration : IDisposable
    {
        public Options Options;
        public LowLevelTransaction Llt;
        public long Id;

        public long Register(long entryId, Span<byte> vector)
        {
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                vector.Length != Options.VectorSizeBytes,
                $"Vector size {vector.Length} does not match expected size: {Options.VectorSizeBytes}");

            var vectorId = Container.Allocate(Llt, Options.VectorsContainer, vector.Length, out var vectorStorage);
            vector.CopyTo(vectorStorage);

            if (Options.CountOfItems is 0) // first item in the graph
            {
                return CreateEntryPoint(entryId, vectorId);
            }

            int level = GetLevelForNewNode();
            
            Console.WriteLine(level);
            return -1;
        }

        private int GetLevelForNewNode()
        {
            int maxLevel = BitOperations.Log2((ulong)Options.CountOfItems) + 1;
            int level = 0;
            while ((Random.Shared.Next() & 1) == 0 && // 50% chance 
                   level < maxLevel)
            {
                level++;
            }
            return level;
        }

        private long CreateEntryPoint(long entryId, long vectorId)
        {
            var nodeId = Container.Allocate(Llt, Options.NodesContainer, sizeof(long), out var nodeSpan);
            MemoryMarshal.Write(nodeSpan, vectorId);
                
            var maintenance = new NodeMaintenance
            {
                NodeId = nodeId,
                Neighbors = 0,
                MinimumDistance = float.MaxValue
            };

            Span<byte> buffer = stackalloc byte[NodeMaintenance.MaxBufferSize];
            int encodedSize = maintenance.Encode(buffer);

            var id = Container.Allocate(Llt, Options.Maintenance, encodedSize, out var storage);
            buffer[..encodedSize].CopyTo(storage);
            Options.EntryPointId = id;
            Options.CountOfItems++;
            return id;
        }

        public void Dispose()
        {
            // flush the local modifications
            MemoryMarshal.AsRef<Options>(Container.GetMutable(Llt, Id)) = Options;
        }
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, long id)
    {
        var item = Container.Get(llt, id);
        return new Registration
        {
            Id = id,
            Options = MemoryMarshal.Read<Options>(item.ToSpan()),
            Llt = llt
        };
    }

    public static Options ReadOptions(LowLevelTransaction llt, long id)
    {
        var item = Container.Get(llt, id);
        return MemoryMarshal.Read<Options>(item.ToSpan());
    }
}
