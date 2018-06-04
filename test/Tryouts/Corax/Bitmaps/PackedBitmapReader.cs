using System;
using System.Collections;
using System.Collections.Generic;
using Jint;
using SlowTests.Issues;
using Voron.Data.PostingList;

namespace Tryouts.Corax
{
    public unsafe struct PackedBitmapReader : IEnumerator<ulong>
    {
        private byte* _data;
        private ulong* _bitmap;
        private ushort* _array;
        private readonly byte* _end;
        private ulong _currentContainer;
        private int _arraySize, _arrayIndex;
        private int _bitPos;
        private byte _currentContainerType;

        public PackedBitmapReader(byte* data, int size)
        {
            _data = data;
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

        public bool MoveNext()
        {
            while (true)
            {
                switch (_currentContainerType)
                {
                    case 0: // need to read the next one
                        if (_data == _end)
                            return false;
                        SwitchContainers();
                        continue;// select next container behavior immediately
                    case (byte)'B':
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
                    case (byte)'A':
                        if (_arrayIndex < _arraySize)
                        {
                            Current = (_currentContainer << 16) | _array[_arrayIndex++];
                            return true;
                        }
                        break;
                    case (byte)'R':
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
            _currentContainerType = *(_data++);
            switch (_currentContainerType)
            {
                case (byte)'S': // skip
                    var delta = PostingListBuffer.ReadVariableSizeLong(ref _data);
                    _currentContainer += (ulong)delta;
                    _currentContainerType = 0;
                    break;
                case (byte)'B':
                    _bitmap = (ulong*)_data;
                    _data += 8192;
                    _bitPos = 0;
                    break;
                case (byte)'A':
                case (byte)'R':
                    _arraySize = PostingListBuffer.ReadVariableSizeInt(ref _data);
                    _array = (ushort*)_data;
                    _arrayIndex = 0;
                    _bitPos = 0;
                    _data += _arraySize * sizeof(ushort);
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
        }
    }
}
