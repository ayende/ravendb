using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe ref struct NodeReader(ByteStringContext allocator, Span<byte> buffer)
    {
        public long PostingListId;
        public long VectorId;
        public int CountOfLevels;

        private int _offset;
        private readonly Span<byte> _buffer = buffer;

        public void LoadInto(ref Node node)
        {
            node.VectorId = VectorId;
            node.PostingListId = PostingListId;
            node.EdgesPerLevel.EnsureCapacityFor(allocator, CountOfLevels);
            while (NextReadEdges(out var list))
            {
                node.EdgesPerLevel.AddUnsafe(list);
            }
        }

        private bool NextReadEdges(out NativeList<long> list)
        {
            if (_offset >= _buffer.Length)
            {
                list = default;
                return false;
            }

            var count = VariableSizeEncoding.Read<int>(_buffer, out int offset, _offset);
            _offset += offset;
            list = new NativeList<long>();
            list.EnsureCapacityFor(allocator, count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                var item = VariableSizeEncoding.Read<long>(_buffer, out offset, _offset);
                _offset += offset;
                prev += item;
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }

            return true;
        }

        public UnmanagedSpan ReadVector(in SearchState state) => ReadVector(VectorId, in state);

        public static UnmanagedSpan ReadVector(long vectorId, in SearchState state) => Hnsw.ReadVector(state.Llt, vectorId, state.Options.VectorSizeBytes);
    }
}
