using System;
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
        private Span<CompressedHeader> Positions => new Span<CompressedHeader>(_page.DataPointer, Header->NumberOfCompressedEntries);
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
            header->NumberOfCompressedEntries = 0;
            header->NumberOfRawValues = 0;
        }

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
                Header->NumberOfEntries++;
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

        private (int Start, int End) GetRangeForCompressedAt(ref CompressedHeader pos)
        {
            Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
            var compressed = new Span<byte>(_page.Pointer + pos.Position, pos.Length);
            var decoder = new PForDecoder(compressed, scratch);
            var start = decoder.TryDecode();
            var end = MemoryMarshal.Read<int>(compressed[..4]);
            return (start[0], end);
        }

        private bool TryCompressRawValues(LowLevelTransaction tx)
        {
            if (Header->NumberOfCompressedEntries == MaxNumberOfCompressedEntries)
                return false; // no where to place this data
            using var _ = tx.Environment.GetTemporaryPage(tx, out var tmpPage);
            _page.AsSpan().Slice(0, PageHeader.SizeOf).CopyTo(tmpPage.AsSpan());
            
            var rawValues = RawValues;
            Span<byte> output = stackalloc byte[MaxNumberOfRawValues * sizeof(int)];
            Span<uint> scratchEncoder = stackalloc uint[PForEncoder.BufferLen];
            Span<int> scratchDecoder = stackalloc int[PForEncoder.BufferLen];

            var tempHeader = (SetLeafPageHeader*)tmpPage.TempPagePointer;
            tempHeader->CompressedValuesCeiling = PageHeader.SizeOf + MaxNumberOfCompressedEntries;
            tempHeader->NumberOfRawValues = 0;
            var tempPositions = new Span<CompressedHeader>(tmpPage.TempPagePointer + PageHeader.SizeOf, MaxNumberOfCompressedEntries);
            
            var valIndex = rawValues.Length - 1;
            for (int i = 0; i < Header->NumberOfCompressedEntries; i++)
            {
                var currentValMasked = valIndex >= 0 ? rawValues[valIndex] & int.MaxValue : int.MinValue;
                ref var pos = ref Positions[i]; 
                var (start, end) = GetRangeForCompressedAt(ref pos);
                if (start < currentValMasked)
                {
                    new Span<byte>(_page.Pointer + pos.Position, pos.Length)
                        .CopyTo(new Span<byte>(tmpPage.TempPagePointer + tempHeader->CompressedValuesCeiling, tmpPage.PageSize - tempHeader->CompressedValuesCeiling));
                    tempPositions[i] = new CompressedHeader {Length = pos.Length, Position = tempHeader->CompressedValuesCeiling};
                    tempHeader->CompressedValuesCeiling += pos.Length;
                    continue; // not in range, copy as is
                }
                
                var encodeAgain = new PForEncoder(output, scratchEncoder);
                Span<CompressedHeader> positions = Positions;
                Span<byte> currentCompressBuffer = new Span<byte>(_page.Pointer + pos.Position, pos.Length);
                var decoder = new PForDecoder(currentCompressBuffer, scratchDecoder);
                while (true)
                {
                    var decoded = decoder.TryDecode();
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

                        if (currentValMasked > currentDecoded)
                        {
                            Debug.Assert(rawValues[valIndex] >= 0);
                            if (encodeAgain.TryAdd(rawValues[valIndex]) == false)
                                return false; // shouldn't happen, but to be safe
                            currentValMasked = GetNextRawValue(rawValues, ref valIndex);
                        }

                        if (encodeAgain.TryAdd(currentDecoded) == false)
                            return false; // shouldn't happen
                    }

                    if (encodeAgain.TryClose() == false)
                        return false; // shouldn't happen
                }
                if (tempHeader->CompressedValuesCeiling + encodeAgain.SizeInBytes > Constants.Storage.PageSize)
                {
                    return false; // cannot fit any more...
                }
                
                output.Slice(0, encodeAgain.SizeInBytes).CopyTo(
                    new Span<byte>(tmpPage.TempPagePointer + Header->CompressedValuesCeiling, tmpPage.PageSize - Header->CompressedValuesCeiling)
                );
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

                output.Slice(encoder.SizeInBytes).CopyTo(tmpPage.AsSpan().Slice(tempHeader->CompressedValuesCeiling));
                tempPositions[Header->NumberOfCompressedEntries++] = new CompressedHeader
                {
                    Length = (ushort)encoder.SizeInBytes,
                    Position = tempHeader->CompressedValuesCeiling 
                };
                tempHeader->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
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
            return int.MinValue;
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
