using System;
using System.Collections;
using System.Collections.Generic;
using Jint;
using SlowTests.Issues;
using Sparrow.Json;
using Voron.Data.PostingList;

namespace Tryouts.Corax
{
    public unsafe struct PackedBitmapReader : IEnumerator<ulong>
    {
        private byte* _start;
        private byte* _current;
        private ulong* _bitmap;
        private ushort* _array;
        private readonly byte* _end;
        private ulong _currentContainer;
        private int _arraySize, _arrayIndex;
        private int _bitPos;
        private ContainerType _currentContainerType;

        public PackedBitmapReader(byte* data, int size)
        {
            _start = data;
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
        }
    }
}
