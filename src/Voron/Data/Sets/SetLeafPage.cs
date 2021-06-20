using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Sets
{
    public readonly unsafe struct SetLeafPage
    {
        private readonly byte* _base;
        private const int MaxNumberOfRawValues = 256;
        private const int MinNumberOfRawValues = 64;
        private const int MaxNumberOfCompressedEntries = 16;
        public SetLeafPageHeader* Header => ((SetLeafPageHeader*)_base);
        
        public Span<byte> Span => new Span<byte>(_base, Constants.Storage.PageSize);

        private struct CompressedHeader
        {
            public ushort Position;
            public ushort Length;
        }
        private Span<CompressedHeader> Positions => new Span<CompressedHeader>(_base + PageHeader.SizeOf, Header->NumberOfCompressedPositions);
        private int OffsetOfRawValuesStart => Constants.Storage.PageSize - (Header->NumberOfRawValues * sizeof(int));
        private Span<int> RawValues => new Span<int>(_base + OffsetOfRawValuesStart, Header->NumberOfRawValues);
        
        public SetLeafPage(byte* @base)
        {
            _base = @base;
        }

        public void Init(long baseline)
        {
            Header->Baseline = baseline & ~int.MaxValue;
            Header->Flags = PageFlags.Single | PageFlags.SetPage;
            Header->SetFlags = SetPageFlags.Leaf;
            Header->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries  * sizeof(CompressedHeader));
            Header->NumberOfCompressedPositions = 0;
            Header->NumberOfRawValues = 0;
        }

        public ref struct Iterator
        {
            private readonly SetLeafPage _parent;
            private readonly Span<int> _scratch;
            private Span<int> _current;
            private int _rawValuesIndex, _compressedEntryIndex;
            private PForDecoder _decoder;
            private bool _hasDecoder;

            public Iterator(SetLeafPage parent, Span<int> scratch)
            {
                _parent = parent;
                _scratch = scratch;
                _rawValuesIndex = _parent.Header->NumberOfRawValues-1;
                _compressedEntryIndex = 0;
                _current = default;
                if (parent.Header->NumberOfCompressedPositions > 0)
                {
                    ref var pos = ref parent.Positions[0];
                    var compressedEntryBuffer = _parent.Span.Slice(pos.Position, pos.Length);
                    _decoder = new PForDecoder(compressedEntryBuffer, scratch);
                    _hasDecoder = true;
                }
                else
                {
                    _decoder = default;
                    _hasDecoder = false;
                }
            }

            public bool MoveNext(out long i)
            {
                while (_current.IsEmpty && _hasDecoder)
                {
                    _current = _decoder.Decode();
                    if (_current.IsEmpty == false)
                        break;
                    
                    if (++_compressedEntryIndex >= _parent.Header->NumberOfCompressedPositions)
                    {
                        _hasDecoder = false;
                        break;
                    }

                    ref var pos = ref _parent.Positions[_compressedEntryIndex];
                    var compressedEntryBuffer = _parent.Span.Slice(pos.Position, pos.Length);
                    _decoder = new PForDecoder(compressedEntryBuffer, _scratch);
                }
                
                
                while (_rawValuesIndex >= 0)
                {
                    // note, reading in reverse!
                    int rawValue = _parent.RawValues[_rawValuesIndex];
                    int rawValueMasked = rawValue & int.MaxValue;
                    if (_current.IsEmpty == false)
                    {
                        if(rawValueMasked > _current[0])
                            break; // need to read from the compressed first
                        if (rawValueMasked == _current[0])
                        {
                            _current = _current.Slice(1); // skip this one
                        }
                    }
                    _rawValuesIndex--;
                    if (rawValue < 0) // removed, ignore
                        continue; 
                    i = rawValue;
                    return true;
                }

                if (_current.IsEmpty)
                {
                    i = default;
                    return false;
                }

                i = _parent.Header->Baseline | _current[0];
                _current = _current.Slice(1);
                return true;
            }

            public bool HasMoreRawValues => _rawValuesIndex < _parent.Header->NumberOfRawValues;

            public void SkipTo(long val)
            {
                var iVal = (int)(val & int.MaxValue);
                _rawValuesIndex = _parent.RawValues.BinarySearch(iVal, new CompareIntsWithoutSignDescending());
                if (_rawValuesIndex < 0)
                    _rawValuesIndex = ~_rawValuesIndex - 1; // we are _after_ the value, so let's go back one step

                SkipToCompressedEntryFor(iVal);
            }

            public int CompressedEntryIndex => _compressedEntryIndex;

            internal void SkipToCompressedEntryFor(int value)
            {
                _compressedEntryIndex = 0;
                for (; _compressedEntryIndex < _parent.Header->NumberOfCompressedPositions; _compressedEntryIndex++)
                {
                    var end = _parent.GetCompressRangeEnd(ref _parent.Positions[_compressedEntryIndex]);
                    if (end >= value)
                        break;
                }
            }
        }

        public List<long> GetDebugOutput()
        {
            var list = new List<long>();
            Span<int> scratch = stackalloc int[128];
            var it = GetIterator(scratch);
            while (it.MoveNext(out var cur))
            {
                list.Add(cur);
            }
            return list;
        }

        public Iterator GetIterator(Span<int> scratch) => new Iterator(this, scratch);

        public bool Add(LowLevelTransaction tx, long value)
        {
            Debug.Assert(IsValidValue(value));
            return AddInternal(tx, (int)value & int.MaxValue);
        }

        public bool IsValidValue(long value)
        {
            return (value & ~int.MaxValue) == Header->Baseline;
        }

        public bool Remove(LowLevelTransaction tx, long value)
        {
            Debug.Assert((value & ~int.MaxValue) == Header->Baseline);
            return AddInternal(tx, int.MinValue | ((int)value & int.MaxValue));
        }

        private bool AddInternal(LowLevelTransaction tx, int value)
        {
            var oldRawValues = RawValues;

            var index = oldRawValues.BinarySearch(value,
                // using descending values to ensure that adding new values in order
                // will do a minimum number of memcpy()s
                new CompareIntsWithoutSignDescending());
            if (index >= 0)
            {
                // overwrite it (maybe add on removed value.
                oldRawValues[index] = value;
                return true;
            }

            // need to add a value, let's check if we can...
            if (Header->NumberOfRawValues == MaxNumberOfRawValues || // the raw values range is full
                Header->CompressedValuesCeiling > OffsetOfRawValuesStart - sizeof(int)) // run into the compressed, cannot proceed
            {
                using var cmp = new Compressor(this, tx);
                if (cmp.TryCompressRawValues() == false)
                    return false;

                // we didn't free enough space
                if (Header->CompressedValuesCeiling > Constants.Storage.PageSize - MinNumberOfRawValues * sizeof(int))
                    return false;
                return AddInternal(tx, value);
            }

            Header->NumberOfRawValues++; // increase the size of the buffer _downward_
            var newRawValues = RawValues;
            index = ~index;
            oldRawValues.Slice(0, index).CopyTo(newRawValues);
            newRawValues[index] = value;
            return true;
        }

        public int SpaceUsed
        {
            get
            {
                var positions = Positions;
                var size = RawValues.Length * sizeof(int) + positions.Length * sizeof(CompressedHeader);
                for (int i = 0; i < positions.Length; i++)
                {
                    size += positions[i].Position;
                }
                return size;
            }
        }

        private int GetCompressRangeEnd(ref CompressedHeader pos)
        {
            var compressed = new Span<byte>(_base + pos.Position, pos.Length);
            var end = MemoryMarshal.Cast<byte, int>(compressed.Slice(compressed.Length -4))[0];
            return end;
        }

        private ref struct Compressor
        {
            private readonly SetLeafPage _parent;
            private readonly TemporaryPage _tmpPage;
            private readonly IDisposable _releaseTempPage;
            private readonly Span<byte> _output;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseOutput;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseScratch;
            private readonly Span<uint> _scratchEncoder;
            private readonly Span<int> _scratchDecoder;
            private int _valIndex;
            private readonly Span<int> _rawValues;
            private int _currentValMasked;
            private readonly SetLeafPageHeader* _tempHeader;
            private readonly Span<CompressedHeader> _tempPositions;

            public void Dispose()
            {
                _releaseTempPage.Dispose();
                _releaseOutput.Dispose();
                _releaseScratch.Dispose();
            }

            public Compressor(SetLeafPage parent, LowLevelTransaction tx)
            {
                _parent = parent;
                _releaseTempPage = tx.Environment.GetTemporaryPage(tx, out _tmpPage);
                _tmpPage.AsSpan().Clear();
                _releaseOutput = tx.Allocator.Allocate(MaxNumberOfRawValues * sizeof(int), out _output);
                _output.Clear();
                _releaseScratch = tx.Allocator.Allocate(PForEncoder.BufferLen*2, out Span<int> scratch);
                _scratchDecoder = scratch.Slice(PForEncoder.BufferLen);
                _scratchEncoder = MemoryMarshal.Cast<int, uint>(scratch.Slice(0, PForEncoder.BufferLen));
                _rawValues = _parent.RawValues;
                _valIndex = _rawValues.Length - 1;
                _currentValMasked = -1;
                _tempHeader = (SetLeafPageHeader*)_tmpPage.TempPagePointer;
                _tempHeader->PageNumber = _parent.Header->PageNumber;
                _tempHeader->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries * sizeof(CompressedHeader));
                _tempHeader->NumberOfRawValues = 0;
                _tempPositions = new Span<CompressedHeader>(_tmpPage.TempPagePointer + PageHeader.SizeOf, MaxNumberOfCompressedEntries);
            }

            private const int MaxPreferredEntrySize = (Constants.Storage.PageSize / MaxNumberOfCompressedEntries);

            public bool TryCompressRawValues()
            {
                if (_parent.Header->NumberOfCompressedPositions == MaxNumberOfCompressedEntries ||
                    _parent.Header->NumberOfRawValues == 0)
                {
                    return CompactCompressedEntries();
                }

                var it = _parent.GetIterator(_scratchDecoder);
                it.SkipToCompressedEntryFor(_rawValues[^1] & int.MaxValue);
                if (TryCopyPreviousCompressedEntries(it.CompressedEntryIndex) == false)
                    return false;

                var maxBits = _output.Length * 7;
                var encoder = new PForEncoder(_output, _scratchEncoder);
                while (it.HasMoreRawValues)
                {
                    if (it.MoveNext(out var v) == false)
                        break;
                    if (encoder.TryAdd((int)v & int.MaxValue) == false)
                        return false;
                    if (encoder.ConsumedBits > maxBits)
                    {
                        // 
                    }
                }
            }

            private readonly bool TryCopyPreviousCompressedEntries(int compressedEntryIndex)
            {
                for (int i = 0; i < compressedEntryIndex; i++)
                {
                    _tempPositions[i] = new CompressedHeader
                    {
                        Length = _parent.Positions[i].Length,
                        Position = _tempHeader->CompressedValuesCeiling
                    };
                    if (_parent.Positions[i].Length + _tempHeader->CompressedValuesCeiling > Constants.Storage.PageSize)
                        return false;
                    _parent.Span.Slice(_parent.Positions[i].Position, _parent.Positions[i].Length)
                        .CopyTo(_tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling));
                    _tempHeader->CompressedValuesCeiling += _parent.Positions[i].Length;
                }
                return true;
            }

            private bool CompactCompressedEntries()
            {
                // we can now try to merge different entries to a single compressed entry
                _currentValMasked = _valIndex >= 0 ? _rawValues[_valIndex] & int.MaxValue : -1;
                int index = 0, estimatedSize = 0;
                _tempHeader->NumberOfCompressedPositions = 0;
                
                while (index < _parent.Header->NumberOfCompressedPositions)
                {
                    ref var pos = ref _parent.Positions[index];
                    int end = _parent.GetCompressRangeEnd(ref pos);
                    if (_valIndex >= 0 && _currentValMasked < end)
                    {
                        break; // need to merge it, overlapping values
                    }

                    if (pos.Length + pos.Length / 4 < MaxPreferredEntrySize)
                        break; // skip all the entries that are too big...

                    index++;
                    // can copy as is...
                    _parent.Span.Slice(pos.Position, pos.Length)
                        .CopyTo(_tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling));
                    _tempPositions[_tempHeader->NumberOfCompressedPositions++] = new CompressedHeader
                    {
                        Length =  pos.Length,
                        Position = _tempHeader->CompressedValuesCeiling
                    };
                    _tempHeader->CompressedValuesCeiling += pos.Length;
                }
                
                if ( _tempHeader->NumberOfCompressedPositions >= MaxNumberOfCompressedEntries)
                    return false; // not enough room for more compressed positions

                var encoder = new PForEncoder(_output, _scratchEncoder);
                _tempHeader->NumberOfCompressedPositions = (byte)index;
                for (; index < _parent.Header->NumberOfCompressedPositions; index++)
                {
                    ref var pos = ref _parent.Positions[index];

                    // here we assume that we don't care if we merge two entries into a bigger one
                    // we have an absolute limit of 1KB for an entry, but we'll try to keep it under
                    // 512 bytes if we can
                    if (estimatedSize + pos.Length> MaxPreferredEntrySize)
                    {
                        if (EncoderToNewCompressEntry(ref encoder) == false)
                            return false;
                        estimatedSize = 0;
                    }
                    estimatedSize += pos.Length;

                    if (EncodeWithRawValues(ref pos, ref encoder) == false)
                        return false;
                }
                
                if ( _tempHeader->NumberOfCompressedPositions >= MaxNumberOfCompressedEntries)
                    return false; // not enough room for more compressed positions

                return TryFlushRawValuesToNewCompressedEntry(ref encoder);
            }

            private bool EncoderToNewCompressEntry(ref PForEncoder encoder)
            {
                if (encoder.TryClose() == false)
                    return false;
                if (encoder.SizeInBytes + _tempHeader->CompressedValuesCeiling > _tmpPage.PageSize)
                    return false;
                _output.Slice(0, encoder.SizeInBytes).CopyTo(_tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling));
                _tempPositions[_tempHeader->NumberOfCompressedPositions++] = new CompressedHeader
                {
                    Length = (ushort)encoder.SizeInBytes, 
                    Position = _tempHeader->CompressedValuesCeiling
                };
                _tempHeader->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
                encoder = new PForEncoder(_output, _scratchEncoder);
                return true;
            }

            private bool TryFlushRawValuesToNewCompressedEntry(ref PForEncoder encoder)
            {
                for (; _valIndex >= 0; _valIndex--)
                {
                    if (_rawValues[_valIndex] >= 0)
                    {
                        if (encoder.TryAdd(_rawValues[_valIndex]) == false)
                            return false;
                    }
                }

                if (encoder.NumberOfAdditions > 0)
                {
                    if (encoder.TryClose() == false)
                        return false;

                    if (_tempHeader->CompressedValuesCeiling + encoder.SizeInBytes >= _tmpPage.PageSize)
                        return false;

                    _output.Slice(0, encoder.SizeInBytes).CopyTo(
                        _tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling)
                    );
                    _tempPositions[_tempHeader->NumberOfCompressedPositions++] = new CompressedHeader
                    {
                        Length = (ushort)encoder.SizeInBytes, Position = _tempHeader->CompressedValuesCeiling
                    };
                    _tempHeader->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
                }

                _tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling).Clear();
                _tmpPage.AsSpan().CopyTo(_parent.Span);
                return true;
            }

            private bool EncodeWithRawValues(ref CompressedHeader pos, ref PForEncoder encoder)
            {
                Span<byte> currentCompressBuffer = _parent.Span.Slice(pos.Position, pos.Length);
                var decoder = new PForDecoder(currentCompressBuffer, _scratchDecoder);
                while (true)
                {
                    var decoded = decoder.Decode();
                    if (decoded.Length == 0) break;
                    foreach (var currentDecoded in decoded)
                    {
                        if (_currentValMasked == currentDecoded)
                        {
                            if (_rawValues[_valIndex] < 0) // deletion
                            {
                                MoveToNextRawValue();
                                continue; // just skip the write, then
                            }
                        }
                        while (currentDecoded > _currentValMasked && _valIndex >= 0)
                        {
                            if (_rawValues[_valIndex] >= 0)
                            {
                                if (encoder.TryAdd(_rawValues[_valIndex]) == false)
                                    return false;
                            }
                            MoveToNextRawValue();
                        }
                        if (encoder.TryAdd(currentDecoded) == false)
                        {
                            return false;
                        }
                    }
                }

                return true;
            }
            
            private void MoveToNextRawValue()
            {
                _currentValMasked = --_valIndex >= 0 ? _rawValues[_valIndex] & int.MaxValue : -1;
            }
        }
        
        private struct CompareIntsWithoutSignDescending : IComparer<int>
        {
            public int Compare(int x, int y)
            {
                x &= int.MaxValue;
                y &= int.MaxValue;
                return y - x;
            }
        }

        public (long First, long Last) GetRange()
        {
            int? first = null, last = null;
            Debug.Assert(Header->NumberOfCompressedPositions > 0 || Header->NumberOfRawValues > 0);

            if (Header->NumberOfCompressedPositions > 0)
            {
                ref var pos = ref Positions[^1];
                last = GetCompressRangeEnd(ref pos);

                pos = ref Positions[0];
                Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
                var compressedEntryBuffer = Span.Slice(pos.Position, pos.Length);
                var decoder = new PForDecoder(compressedEntryBuffer, scratch);
                first = decoder.Decode()[0];
            }

            var values = RawValues;
            if (values.IsEmpty == false)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    if(values[i] < 0)
                        continue;
                    if (last == null || last.Value < values[i])
                        last = values[i];
                    break;
                }

                for (int i = values.Length - 1; i >= 0; i--)
                {
                    if(values[i] < 0)
                        continue;
                    if (first == null || first > values[i])
                        first = values[i];
                    break;
                }
            }

            Debug.Assert(first != null, nameof(first) + " != null");
            Debug.Assert(last != null, nameof(last) + " != null");
            return (Header->Baseline | first.Value, Header->Baseline | last.Value);
        }
    }
    
}
