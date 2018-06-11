using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Json;

namespace Tryouts.Corax
{
    public unsafe struct PackedBitmapBuilder : IDisposable  
    {
        private UnmanagedWriteBuffer _writer;
        internal readonly ulong* _bitmapBuffer;
        internal readonly ushort* _arrayBuffer;

        private ulong _currentContainer;
        private bool _useBitmap;
        private int _disjointAdds;
        private int _arrayIndex;
        private JsonOperationContext.ReturnBuffer _returnBuffer;
        private ushort _prevOffsetInContainer;

        public ulong NumberOfSetBits;

        public PackedBitmapBuilder(JsonOperationContext ctx)
        {
            _returnBuffer = ctx.GetManagedBuffer(out var buffer);
            _writer = ctx.GetStream(8192);
            _bitmapBuffer = (ulong*)buffer.Pointer;
            _arrayBuffer = (ushort*)(buffer.Pointer + 8192);
            // we use it for array & bitmap handling, and when 
            // merging packed bitmaps
            Debug.Assert(buffer.Length == 8192 * 4);
            _arrayIndex = 0;
            _disjointAdds = 0;
            _currentContainer = 0;
            NumberOfSetBits = 0;
            _useBitmap = false;
            _prevOffsetInContainer = ushort.MaxValue;
        }

        public void Set(ulong pos)
        {
            NumberOfSetBits++;
            var container = pos >> 16;
            var offset = (ushort)pos;
            if (container != _currentContainer)
            {
                PushContainer(container);
            }
            if (offset != _prevOffsetInContainer + 1)
                _disjointAdds++;
            _prevOffsetInContainer = offset;

            if (_useBitmap)
            {
                _bitmapBuffer[offset >> 6] |= 1UL << offset;
                return;
            }

            if (_arrayIndex == 4096)
            {
                Memory.Set((byte*)_bitmapBuffer, 0, 8192);
                for (int i = 0; i < 4096; i++)
                {
                    var val = _arrayBuffer[i];
                    _bitmapBuffer[val >> 6] |= 1UL << val;
                }
                _bitmapBuffer[offset >> 6] |= 1UL << offset;
                _useBitmap = true;
                return;
            }


            _arrayBuffer[_arrayIndex++] = offset;
        }

        public void Complete(out PackedBitmapReader reader)
        {
            PushContainer(_currentContainer);
            _writer.EnsureSingleChunk(out var ptr, out var size);
            reader = new PackedBitmapReader(ptr, size, _writer);
            _writer = default;
        }

        internal static (ushort Start, int Length) FindRun(ulong* bitmap, int offset)
        {
            ushort start = (ushort)offset;
            int length = 0;
            var bitOffset = offset % 64;
            var byteOffset = offset >> 6;
            if (bitOffset != 0)
            {
                if (ScanPartialLong(bitOffset) == false)
                    return (start, length);
                byteOffset++;
            }
            bitOffset = 64;
            for (; byteOffset < 1024; byteOffset++)
            {
                if (bitmap[byteOffset] == ulong.MaxValue)
                {
                    length += 64;
                }
                else if (bitmap[byteOffset] == 0UL)
                {
                    if (length != 0)
                        return (start, length);
                    start += 64;
                }
                else if (ScanPartialLong(0) == false)
                    return (start, length);
            }
            ScanPartialLong(bitOffset);
            return (start, length);

            bool ScanPartialLong(int bitStart)
            {
                for (int i = bitStart; i < 64; i++)
                {
                    if ((bitmap[byteOffset] & (1UL << i)) == 0)
                    {
                        if (length != 0)
                            return false;
                        start++;
                    }
                    else
                    {
                        length++;
                    }
                }
                return true;
            }
        }

        private void PushContainer(ulong container)
        {
            if (_useBitmap)
            {
                if (_disjointAdds <= 2048)
                {
                    int index = 0;
                    // can get better space saving here
                    int start = 0;
                    while (start < ushort.MaxValue)
                    {
                        var r = FindRun(_bitmapBuffer, start);
                        if (r.Length == 0)
                            break;
                        if (r.Length > ushort.MaxValue)
                        {
                            var half = r.Length / 2;
                            _arrayBuffer[index++] = r.Start;
                            _arrayBuffer[index++] = (ushort)half;
                            r.Length -= half;
                            r.Start += (ushort)half;
                        }
                        _arrayBuffer[index++] = r.Start;
                        _arrayBuffer[index++] = (ushort)r.Length;
                        start = r.Start + r.Length;
                    }
                    WriteArray(_arrayBuffer, index, ContainerType.RunLength);
                }
                else
                {
                    WriteBitmap((byte*)_bitmapBuffer);
                }
            }
            else if (_arrayIndex > 0)
            {
                if (_disjointAdds < _arrayIndex / 2)
                {
                    var temp = (ushort*)_bitmapBuffer;
                    temp[0] = _arrayBuffer[0];
                    int index = 1;
                    ushort runs = 1;
                    for (int i = 1; i < _arrayIndex; i++)
                    {
                        if (_arrayBuffer[i - 1] + 1 == _arrayBuffer[i])
                        {
                            runs++;
                            continue;
                        }

                        temp[index++] = runs;
                        temp[index++] = _arrayBuffer[i];
                        runs = 1;
                    }
                    temp[index++] = runs;
                    WriteArray(temp, index, ContainerType.RunLength);
                }
                else
                {
                    WriteArray(_arrayBuffer, _arrayIndex, ContainerType.Array);
                }
            }
            WriteSkippedContainers(container);

            _currentContainer = container;
            _arrayIndex = 0;
            _useBitmap = false;
            _disjointAdds = 0;
            _prevOffsetInContainer = ushort.MaxValue;
        }

        internal void WriteArray(ushort* array, int count, ContainerType type)
        {
            _writer.WriteByte((byte)type);
            _writer.WriteVariableSizeInt(count);
            _writer.Write((byte*)array, count * sizeof(ushort));
        }

        internal void WriteBitmap(byte* bitmap)
        {
            _writer.WriteByte((byte)ContainerType.Bitmap);
            _writer.Write(bitmap, 8192);
        }

        internal void WriteSkippedContainers(ulong container)
        {
            if (container - _currentContainer > 1)
            {
                _writer.WriteByte((byte)ContainerType.Skip);
                ulong skip = container - (_currentContainer + 1);
                _writer.WriteVariableSizeLong((long)skip);
            }
        }

        public void Dispose()
        {
            _returnBuffer.Dispose();
            _writer.Dispose();
        }
    }
}
