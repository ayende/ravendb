using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Jint;
using SlowTests.Issues;
using Sparrow;
using Sparrow.Json;
using Voron.Data.PostingList;

namespace Tryouts.Corax
{
    public unsafe struct PackedBitmapReader : IEnumerator<ulong>
    {
        private readonly UnmanagedWriteBuffer _writer;
        private byte* _start;
        private byte* _current;
        private ulong* _bitmap;
        private ushort* _array;
        private readonly byte* _end;
        private ulong _currentContainer;
        private int _arraySize, _arrayIndex;
        private int _bitPos;
        private ContainerType _currentContainerType;

        public int SizeInBytes => (int)(_end - _start);

        public PackedBitmapReader(byte* data, int size, UnmanagedWriteBuffer writer = default)
        {
            _start = data;
            _writer = writer;
            _current = data;
            _end = data + size;
            _currentContainerType = 0;
            _array = null;
            _bitmap = null;
            _currentContainer = 0;
            _arraySize = 0;
            _arrayIndex = 0;
            _bitPos = 0;
            Current = 0;
        }

        private bool Done => _current == _end;

        public bool MoveNext()
        {
            while (true)
            {
                switch (_currentContainerType)
                {
                    case ContainerType.Skip:
                    case ContainerType.None: // need to read the next one
                        if (_current == _end)
                            return false;
                        SwitchContainers();
                        continue;// select next container behavior immediately
                    case ContainerType.Bitmap:
                        while (_bitPos <= ushort.MaxValue)
                        {
                            if ((_bitmap[_bitPos >> 6] & (1UL << _bitPos)) == 0)
                            {
                                _bitPos++;
                                continue;
                            }

                            Current = (_currentContainer << 16) | (uint)_bitPos;
                            _bitPos++;
                            return true;
                        }
                        break;
                    case ContainerType.Array:
                        if (_arrayIndex < _arraySize)
                        {
                            Current = (_currentContainer << 16) | _array[_arrayIndex++];
                            return true;
                        }
                        break;
                    case ContainerType.RunLength:
                        while (_arrayIndex < _arraySize)
                        {
                            if (_array[_arrayIndex + 1] == _bitPos)
                            {
                                _bitPos = 0;
                                _arrayIndex += 2;
                                continue;
                            }
                            var val = (ushort)(_array[_arrayIndex] + _bitPos++);
                            Current = (_currentContainer << 16) | val;
                            return true;
                        }
                        break;
                    default:
                        ThrowInvalidContainerType();
                        break;
                }

                _currentContainerType = 0;
                _currentContainer++;
            }
        }

        private void SwitchContainers()
        {
            _currentContainerType = (ContainerType)(*(_current++));
            switch (_currentContainerType)
            {
                case ContainerType.Skip:
                    var delta = PostingListBuffer.ReadVariableSizeLong(ref _current);
                    _currentContainer += (ulong)delta;
                    _currentContainerType = ContainerType.Skip;
                    break;
                case ContainerType.Bitmap:
                    _bitmap = (ulong*)_current;
                    _current += 8192;
                    _bitPos = 0;
                    break;
                case ContainerType.Array:
                case ContainerType.RunLength:
                    _arraySize = PostingListBuffer.ReadVariableSizeInt(ref _current);
                    _array = (ushort*)_current;
                    _arrayIndex = 0;
                    _bitPos = 0;
                    _current += _arraySize * sizeof(ushort);
                    break;
                default:
                    ThrowInvalidContainerType();
                    break;
            }
        }

        private void ThrowInvalidContainerType()
        {
            throw new InvalidOperationException("Uknown container type: " + (char)_currentContainerType);
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public ulong Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _writer.Dispose();
        }

        private interface IBinaryOp
        {
            void OtherSideMissing(ref PackedBitmapReader x, in PackedBitmapBuilder builder);
            void Merge(ulong* pos, ulong a, ulong b);
            void MergeContainer(ulong* buffer, in PackedBitmapReader a, in PackedBitmapReader b);
            void MergeWithBitmap(ulong* bitmap, in PackedBitmapReader b);
        }

        private struct OrOperation : IBinaryOp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Merge(ulong* pos, ulong a, ulong b)
            {
                *pos = a | b;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void MergeContainer(ulong* buffer, in PackedBitmapReader a, in PackedBitmapReader b)
            {
                Memory.Set((byte*)buffer, 0, 8192);
                OrBitsFromReader(buffer, a);
                OrBitsFromReader(buffer, b);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void MergeWithBitmap(ulong* bitmap, in PackedBitmapReader b)
            {
                OrBitsFromReader(bitmap, b);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OtherSideMissing(ref PackedBitmapReader x, in PackedBitmapBuilder builder)
            {
                CopyAndAdvanceContainer(ref x, in builder);
            }
        }

        private struct AndOperation : IBinaryOp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Merge(ulong* pos, ulong a, ulong b)
            {
                *pos = a & b;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void MergeContainer(ulong* buffer, in PackedBitmapReader a, in PackedBitmapReader b)
            {
                Memory.Set((byte*)buffer, 0, 8192);
                OrBitsFromReader(buffer, a);
                var tempArray = (ulong*)((byte*)buffer + 16 * 1024);
                Memory.Set((byte*)tempArray, 0, 8192);
                OrBitsFromReader(tempArray, b);
                for (int i = 0; i < 1024; i++)
                {
                    buffer[i] &= tempArray[i];
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void MergeWithBitmap(ulong* bitmap, in PackedBitmapReader b)
            {
                var tempArray = (ulong*)((byte*)bitmap + 16 * 1024);
                Memory.Set((byte*)tempArray, 0, 8192);
                OrBitsFromReader(tempArray, b);

                for (int i = 0; i < 1024; i++)
                {
                    bitmap[i] &= tempArray[i];
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OtherSideMissing(ref PackedBitmapReader x, in PackedBitmapBuilder builder)
            {
                x.SwitchContainers();
            }
        }

        private struct XorOperation : IBinaryOp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Merge(ulong* pos, ulong a, ulong b)
            {
                *pos = a ^ b;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void MergeContainer(ulong* buffer, in PackedBitmapReader a, in PackedBitmapReader b)
            {
                Memory.Set((byte*)buffer, 0, 8192);
                OrBitsFromReader(buffer, a);
                var tempArray = (ulong*)((byte*)buffer + 16 * 1024);
                Memory.Set((byte*)tempArray, 0, 8192);
                OrBitsFromReader(tempArray, b);
                for (int i = 0; i < 1024; i++)
                {
                    buffer[i] ^= tempArray[i];
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void MergeWithBitmap(ulong* bitmap, in PackedBitmapReader b)
            {
                var tempArray = (ulong*)((byte*)bitmap + 16 * 1024);
                Memory.Set((byte*)tempArray, 0, 8192);
                OrBitsFromReader(tempArray, b);

                for (int i = 0; i < 1024; i++)
                {
                    bitmap[i] ^= tempArray[i];
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OtherSideMissing(ref PackedBitmapReader x, in PackedBitmapBuilder builder)
            {
                CopyAndAdvanceContainer(ref x, in builder);
            }
        }

        public static PackedBitmapReader Or(JsonOperationContext ctx, ref PackedBitmapReader a, ref PackedBitmapReader b)
        {
            return Operate<OrOperation>(ctx, ref a, ref b);
        }

        public static PackedBitmapReader And(JsonOperationContext ctx, ref PackedBitmapReader a, ref PackedBitmapReader b)
        {
            return Operate<AndOperation>(ctx, ref a, ref b);
        }

        public static PackedBitmapReader Xor(JsonOperationContext ctx, ref PackedBitmapReader a, ref PackedBitmapReader b)
        {
            return Operate<XorOperation>(ctx, ref a, ref b);
        }


        private static PackedBitmapReader Operate<TOp>(JsonOperationContext ctx, ref PackedBitmapReader a, ref PackedBitmapReader b)
            where TOp : struct, IBinaryOp
        {
            var op = new TOp();

            using (var builder = new PackedBitmapBuilder(ctx))
            {
                a._current = a._start;
                b._current = b._start;

                a.SwitchContainers();
                b.SwitchContainers();

                do
                {
                    while (a._currentContainer < b._currentContainer)
                    {
                        op.OtherSideMissing(ref a, in builder);
                    }
                    while (b._currentContainer < a._currentContainer)
                    {
                        op.OtherSideMissing(ref b, in builder);
                    }
                    if (a._currentContainer != b._currentContainer)
                        continue;

                    if (a._currentContainerType == ContainerType.Bitmap &&
                        b._currentContainerType == ContainerType.Bitmap)
                    {
                        for (int i = 0; i < 1024; i++)
                        {
                            op.Merge(builder._bitmapBuffer + i, a._bitmap[i], b._bitmap[i]);
                        }
                        builder.WriteBitmap((byte*)builder._bitmapBuffer);
                        continue;
                    }
                    if (a._currentContainerType == ContainerType.Bitmap ||
                        b._currentContainerType == ContainerType.Bitmap)
                    {
                        var withBitmap = a._currentContainerType == ContainerType.Bitmap ? a : b;
                        var withoutBitmap = a._currentContainerType != ContainerType.Bitmap ? a : b;
                        Memory.Copy(builder._bitmapBuffer, withBitmap._bitmap, 8192);
                        op.MergeWithBitmap(builder._bitmapBuffer, in withoutBitmap);
                        builder.WriteBitmap((byte*)builder._bitmapBuffer);
                        continue;
                    }
                    op.MergeContainer(builder._bitmapBuffer, in a, in b);

                    // now need to see if can optimize
                    OptimizeSingleContainer(in builder);

                } while (a.Done == false && b.Done == false);

                builder.Complete(out var output);
                return output;
            }
        }

        private static void OptimizeSingleContainer(in PackedBitmapBuilder builder)
        {
            int runsIndex = 0;
            int start = 0;
            int numberOfBitsSet = 0;
            var mode = ContainerType.RunLength;
            while (true)
            {
                var r = PackedBitmapBuilder.FindRun(builder._bitmapBuffer, start);
                if (r.Length == 0)
                {
                    builder.WriteArray(builder._arrayBuffer, runsIndex, mode);
                    break;
                }
                if (runsIndex == 4096)
                {
                    if (numberOfBitsSet < 4096 && mode == ContainerType.RunLength)
                    {
                        // we run out of room, but only because we used
                        // runs, maybe we can try using array here? Let's try
                        var tempArray = (ushort*)((byte*)builder._bitmapBuffer + 16 * 1024);
                        int tempIndex = 0;
                        for (int i = 0; i < runsIndex; i += 2)
                        {
                            var val = builder._arrayBuffer[i];
                            var runs = builder._arrayBuffer[i + 1];
                            for (ushort j = 0; j < runs; j++)
                            {
                                tempArray[tempIndex++] = (ushort)(val + j);
                            }
                        }
                        runsIndex = tempIndex;
                        Memory.Copy(builder._arrayBuffer, tempArray, 8192);
                        mode = ContainerType.Array;
                    }
                    else
                    {
                        // about to overrun, just use bitmap
                        builder.WriteBitmap((byte*)builder._bitmapBuffer);
                        break;
                    }
                }
                if (mode == ContainerType.RunLength)
                {
                    if (r.Length > ushort.MaxValue)
                    {
                        var half = r.Length / 2;
                        builder._arrayBuffer[runsIndex++] = r.Start;
                        builder._arrayBuffer[runsIndex++] = (ushort)half;
                        r.Start += (ushort)half;
                        r.Length -= half;
                        numberOfBitsSet += half;
                    }
                    builder._arrayBuffer[runsIndex++] = r.Start;
                    builder._arrayBuffer[runsIndex++] = (ushort)r.Length;
                }
                else
                {
                    Debug.Assert(numberOfBitsSet == runsIndex);
                    Debug.Assert(mode == ContainerType.Array);
                    if (runsIndex + r.Length > 4096)
                    {
                        // about to overrun, just use bitmap
                        builder.WriteBitmap((byte*)builder._bitmapBuffer);
                        break;
                    }

                    for (int i = 0; i < r.Length; i++)
                    {
                        builder._arrayBuffer[runsIndex++] = (ushort)(r.Start + i);
                    }
                }
                start = r.Start + r.Length;
                numberOfBitsSet += r.Length;
            }
        }


        private static void OrBitsFromReader(ulong* buffer, PackedBitmapReader withoutBitmap)
        {
            switch (withoutBitmap._currentContainerType)
            {
                case ContainerType.Array:
                    for (int i = 0; i < withoutBitmap._arraySize; i++)
                    {
                        var val = withoutBitmap._array[i];
                        buffer[val >> 6] |= 1UL << val;
                    }
                    break;
                case ContainerType.RunLength:
                    for (int i = 0; i < withoutBitmap._arraySize; i += 2)
                    {
                        var val = withoutBitmap._array[i];
                        var times = withoutBitmap._array[i + 1];
                        for (int j = 0; j < times; j++)
                        {
                            buffer[val >> 6] |= 1UL << val;
                            val++;
                        }
                    }
                    break;
                case ContainerType.Skip:
                    // nothing to do here, can skip :-)
                    break;
                default:
                    break;
            }
        }

        private static void SetBits(PackedBitmapReader a, ulong* bitmapBuffer)
        {
            throw new NotImplementedException();
        }

        private static void CopyAndAdvanceContainer(ref PackedBitmapReader x, in PackedBitmapBuilder builder)
        {
            switch (x._currentContainerType)
            {
                case ContainerType.Bitmap:
                    builder.WriteBitmap((byte*)x._bitmap);
                    break;
                case ContainerType.Array:
                case ContainerType.RunLength:
                    builder.WriteArray(x._array, x._arraySize, x._currentContainerType);
                    break;
                case ContainerType.Skip:
                    builder.WriteSkippedContainers(x._currentContainer);
                    break;
                case ContainerType.None:
                default:
                    throw new InvalidOperationException("Unexpected container type: " + x._currentContainerType);
            }
            x.SwitchContainers();
        }
    }
}
