using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Json;

namespace Tryouts.Corax
{
    public unsafe struct PackedBitmapBuilder
    {
        private UnmanagedWriteBuffer _writer;
        private readonly ulong* _bitmapBuffer;
        private readonly ushort* _arrayBuffer;

        private ulong _currentContainer;
        private bool _useBitmap;
        private int _disjointAdds;
        private int _arrayIndex;

        private ushort _prevOffsetInContainer;

        public PackedBitmapBuilder(UnmanagedWriteBuffer writer, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            _bitmapBuffer = (ulong*)buffer.Pointer;
            _arrayBuffer = (ushort*)(buffer.Pointer + 8192);
            Debug.Assert(buffer.Length > 8192 * 2);// we use it for array & bitmap handling
            _writer = writer;
            _arrayIndex = 0;
            _disjointAdds = 0;
            _currentContainer = 0;
            _useBitmap = false;
            _prevOffsetInContainer = ushort.MaxValue;
        }

        public void Set(ulong pos)
        {
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

        public void Complete(out byte* ptr, out int size)
        {
            PushContainer(_currentContainer);
            _writer.EnsureSingleChunk(out ptr, out size);
        }

        private (ushort Start, ushort Length) FindRun(int offset)
        {
            ushort start = (ushort)offset;
            ushort length = 0;
            var bitOffset = offset % 64;
            var byteOffset = offset >> 6;
            if (bitOffset != 0)
            {
                if (ScanPartialLong(bitOffset, _bitmapBuffer) == false)
                    return (start, length);
                byteOffset++;
            }
            bitOffset = 64;
            for (; byteOffset < 1024; byteOffset++)
            {
                if (_bitmapBuffer[byteOffset] == ulong.MaxValue)
                {
                    length += 64;
                }
                else if (_bitmapBuffer[byteOffset] == 0UL)
                {
                    if (length != 0)
                        return (start, length);
                    start += 64;
                }
                else if (ScanPartialLong(0, _bitmapBuffer) == false)
                    return (start, length);
            }
            ScanPartialLong(bitOffset, _bitmapBuffer);
            return (start, length);

            bool ScanPartialLong(int bitStart, ulong* bitmap)
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
                        var r = FindRun(start);
                        if (r.Length == 0)
                            break;
                        _arrayBuffer[index++] = r.Start;
                        _arrayBuffer[index++] = r.Length;
                        start = r.Start + r.Length;
                    }
                    _writer.WriteByte((byte)'R');
                    _writer.WriteVariableSizeInt(index);
                    _writer.Write((byte*)_arrayBuffer, index * sizeof(ushort));
                }
                else
                {
                    _writer.WriteByte((byte)'B');
                    _writer.Write((byte*)_bitmapBuffer, 8192);
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
                    }
                    temp[index++] = runs;
                    _writer.WriteByte((byte)'R');
                    _writer.WriteVariableSizeInt(index);
                    _writer.Write((byte*)temp, index * sizeof(ushort));
                }
                else
                {
                    _writer.WriteByte((byte)'A');
                    _writer.WriteVariableSizeInt(_arrayIndex);
                    _writer.Write((byte*)_arrayBuffer, _arrayIndex * sizeof(ushort));
                }
            }
            if (container - _currentContainer > 1)
            {
                _writer.WriteByte((byte)'S');
                ulong skip = container - (_currentContainer + 1);
                _writer.WriteVariableSizeLong((long)skip);
            }

            _currentContainer = container;
            _arrayIndex = 0;
            _useBitmap = false;
            _disjointAdds = 0;
            _prevOffsetInContainer = ushort.MaxValue;
        }
    }
}
