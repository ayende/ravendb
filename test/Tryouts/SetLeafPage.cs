using System;
using System.Buffers;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Lucene.Net.Search.Spans;
using Voron;
using Voron.Data;
using Voron.Impl;
using Constants = Voron.Global.Constants;

namespace Tryouts
{
    

    public readonly unsafe struct SetLeafPage
    {
        private readonly Page _page;
        private const int MaxNumberOfRawValues = 256;
        private const int MaxNumberOfCompressedEntries = 16;
        private SetLeafPageHeader* Header => ((SetLeafPageHeader*)_page.Pointer);

        private struct CompressedHeader
        {
            public ushort Position;
            public ushort Length;
        }
        private Span<CompressedHeader> Positions => new Span<CompressedHeader>(_page.DataPointer, Header->NumberOfCompressedPositions);
        private int OffsetOfRawValuesStart => Constants.Storage.PageSize - (Header->NumberOfRawValues * sizeof(int));
        private Span<int> RawValues => new Span<int>(_page.Pointer + OffsetOfRawValuesStart, Header->NumberOfRawValues);
        
        public SetLeafPage(Page page)
        {
            _page = page;
        }

        public void Init(long baseline)
        {
            var header = (SetLeafPageHeader*)_page.Pointer;
            header->Baseline = baseline & ~int.MaxValue;
            header->Flags = PageFlags.Single | PageFlags.SetLeafPage;
            header->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries  * sizeof(CompressedHeader));
            header->NumberOfCompressedPositions = 0;
            header->NumberOfRawValues = 0;
        }

        public int TotalNumberOfEntries
        {
            get
            {
                int total = Header->NumberOfRawValues;
                var positions = Positions;
                Span<int> scratch = stackalloc int[128];
                Span<byte> page = _page.AsSpan();
                for (int i = 0; i < Header->NumberOfCompressedPositions; i++)
                {
                    var decoder = new PForDecoder(
                        page.Slice(positions[i].Position, positions[i].Length),
                        scratch
                    );
                    while (true)
                    {
                        var decoded = decoder.Decode();
                        if (decoded.IsEmpty) break;
                        total += decoded.Length;
                    }
                }
                return total;
            }
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
                    var compressedEntryBuffer = _parent._page.AsSpan().Slice(pos.Position, pos.Length);
                    _decoder = new PForDecoder(compressedEntryBuffer, scratch);
                    _hasDecoder = true;
                }
                else
                {
                    _decoder = default;
                    _hasDecoder = false;
                }
            }

            public bool MoveNext(out int i)
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
                    var compressedEntryBuffer = _parent._page.AsSpan().Slice(pos.Position, pos.Length);
                    _decoder = new PForDecoder(compressedEntryBuffer, _scratch);
                }
                
                
                if (_rawValuesIndex >= 0)
                {
                    // note, reading in reverse!
                    int rawValue = _parent.RawValues[_rawValuesIndex];
                    if (_current.IsEmpty || rawValue < _current[0])
                    {
                        _rawValuesIndex--;
                        i = rawValue;
                        return true;
                    }
                }

                if (_current.IsEmpty)
                {
                    i = default;
                    return false;
                }

                i = _current[0];
                _current = _current.Slice(1);
                return true;
            }
        }

        public Iterator GetIterator(Span<int> scratch) => new Iterator(this, scratch);

        public bool Add(LowLevelTransaction tx, long value)
        {
            var header = (SetLeafPageHeader*)_page.Pointer;
            Debug.Assert((value & ~int.MaxValue) == header->Baseline);

            int iVal = (int)value & int.MaxValue;
            var oldRawValues = RawValues;

            var index= oldRawValues.BinarySearch(iVal, 
                // using descending values to ensure that adding new values in order
                // will do a minimum number of memcpy()s
                new CompareIntsWithoutSignDescending());
            if (index >= 0)
            {
                // overwrite it (maybe add on removed value.
                oldRawValues[index] = iVal;
                return true;
            }
            // need to add a value, let's check if we can...
            if (Header->NumberOfRawValues == MaxNumberOfRawValues || // the raw values range is full
                header->CompressedValuesCeiling > OffsetOfRawValuesStart - sizeof(int)) // run into the compressed, cannot proceed
            {
                return TryCompressRawValues(tx) && Add(tx, value);
            }
            Header->NumberOfRawValues++; // increase the size of the buffer _downward_
            var newRawValues = RawValues;
            index = ~index;
            oldRawValues.Slice(0, index).CopyTo(newRawValues);
            newRawValues[index] = iVal;
            return true;
        }

        private int GetCompressRangeEnd(ref CompressedHeader pos)
        {
            var compressed = new Span<byte>(_page.Pointer + pos.Position, pos.Length);
            var end = MemoryMarshal.Cast<byte, int>(compressed.Slice(compressed.Length -4))[0];
            return end;
        }

        private bool TryCompressRawValues(LowLevelTransaction tx)
        {
            if (Header->NumberOfCompressedPositions == MaxNumberOfCompressedEntries)
            {
                return false; // no where to place this data
            }

            using var _ = tx.Environment.GetTemporaryPage(tx, out var tmpPage);
            _page.AsSpan().Slice(0, PageHeader.SizeOf).CopyTo(tmpPage.AsSpan());
            
            var rawValues = RawValues;
            Span<byte> output = stackalloc byte[MaxNumberOfRawValues * sizeof(int)];
            Span<uint> scratchEncoder = stackalloc uint[PForEncoder.BufferLen];
            Span<int> scratchDecoder = stackalloc int[PForEncoder.BufferLen];

            var tempHeader = (SetLeafPageHeader*)tmpPage.TempPagePointer;
            tempHeader->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries * sizeof(CompressedHeader));
            tempHeader->NumberOfRawValues = 0;
            var tempPositions = new Span<CompressedHeader>(tmpPage.TempPagePointer + PageHeader.SizeOf, MaxNumberOfCompressedEntries);
            
            var valIndex = rawValues.Length - 1;
            for (int i = 0; i < Header->NumberOfCompressedPositions; i++)
            {
                var currentValMasked = valIndex >= 0 ? rawValues[valIndex] & int.MaxValue : int.MinValue;
                ref var pos = ref Positions[i]; 
                var end = GetCompressRangeEnd(ref pos);
                if (end < currentValMasked)
                {
                    new Span<byte>(_page.Pointer + pos.Position, pos.Length)
                        .CopyTo(new Span<byte>(tmpPage.TempPagePointer + tempHeader->CompressedValuesCeiling, tmpPage.PageSize - tempHeader->CompressedValuesCeiling));
                    tempPositions[i] = new CompressedHeader {Length = pos.Length, Position = tempHeader->CompressedValuesCeiling};
                    tempHeader->CompressedValuesCeiling += pos.Length;
                    continue; // not in range, copy as is
                }
                
                var encodeAgain = new PForEncoder(output, scratchEncoder);
                Span<byte> currentCompressBuffer = new Span<byte>(_page.Pointer + pos.Position, pos.Length);
                var decoder = new PForDecoder(currentCompressBuffer, scratchDecoder);
                while (true)
                {
                    var decoded = decoder.Decode();
                    if (decoded.Length == 0) break;
                    foreach (var currentDecoded in decoded)
                    {
                        if (currentValMasked == currentDecoded)
                        {
                            if (rawValues[valIndex] < 0) // deletion
                            {
                                currentValMasked = GetNextRawValue(rawValues, ref valIndex);
                                continue; // just skip the write, then
                            }
                        }
                        while (currentDecoded > currentValMasked && valIndex >= 0)
                        {
                            Debug.Assert(rawValues[valIndex] >= 0);
                            if (encodeAgain.TryAdd(rawValues[valIndex]) == false)
                                return false; // shouldn't happen, but to be safe
                            currentValMasked = GetNextRawValue(rawValues, ref valIndex);
                        }
                        if (encodeAgain.TryAdd(currentDecoded) == false)
                            return false; // shouldn't happen
                    }
                }
                if (encodeAgain.TryClose() == false)
                    return false; // shouldn't happen
                if (tempHeader->CompressedValuesCeiling + encodeAgain.SizeInBytes > Constants.Storage.PageSize)
                {
                    return false; // cannot fit any more...
                }

                Span<byte> src = output.Slice(0, encodeAgain.SizeInBytes);
                src.CopyTo(tmpPage.AsSpan().Slice(tempHeader->CompressedValuesCeiling));
                src.Clear();
                
                tempPositions[i] = new CompressedHeader {Length = (ushort)encodeAgain.SizeInBytes, Position = tempHeader->CompressedValuesCeiling};
                tempHeader->CompressedValuesCeiling += (ushort)encodeAgain.SizeInBytes;
            }

            if (valIndex >= 0)
            {
                var encoder = new PForEncoder(output, scratchEncoder);
                for (; valIndex >= 0; valIndex--)
                {
                    if (encoder.TryAdd(rawValues[valIndex]) == false)
                        return false;
                }

                if (encoder.TryClose() == false)
                    return false;

                if (tempHeader->CompressedValuesCeiling + encoder.SizeInBytes >= tmpPage.PageSize)
                    return false;

                output.Slice(0, encoder.SizeInBytes).CopyTo(
                    tmpPage.AsSpan().Slice(tempHeader->CompressedValuesCeiling)
                    );
                tempPositions[Header->NumberOfCompressedPositions++] = new CompressedHeader
                {
                    Length = (ushort)encoder.SizeInBytes,
                    Position = tempHeader->CompressedValuesCeiling 
                };
                tempHeader->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
                tempHeader->NumberOfCompressedPositions++;
            }

            tmpPage.AsSpan().Slice(tempHeader->CompressedValuesCeiling).Clear();

            tmpPage.AsSpan().CopyTo(_page.AsSpan());
            return true;
        }

        private static int GetNextRawValue(Span<int> values, ref int valIndex)
        {
            valIndex--;
            if (valIndex >= 0)
            {
                return values[valIndex] & int.MaxValue;
            }
            return -1;
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
    }
    
}
