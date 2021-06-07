using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Voron;
using Voron.Data;
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

        public bool Add(long value)
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
                return TryCompressRawValues() && Add(value);
            }
            Header->NumberOfRawValues++; // increase the size of the buffer _downward_
            var newRawValues = RawValues;
            index = ~index;
            oldRawValues.Slice(0, index).CopyTo(newRawValues);
            newRawValues[index] = iVal;
            return true;
        }

        private (int Start, int End) GetRangeForCompressedAt(int index)
        {
            var pos = Positions[index];
            Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
            var compressed = new Span<byte>(_page.Pointer + pos.Position, pos.Length);
            var decoder = new PForDecoder(compressed, scratch);
            var start = decoder.TryDecode();
            var end = MemoryMarshal.Read<int>(compressed[..4]);
            return (start[0], end);
        }

        private bool TryCompressRawValues()
        {
            if (Header->NumberOfCompressedEntries == MaxNumberOfCompressedEntries)
                return false; // no where to place this data
            
            Span<byte> output = stackalloc byte[MaxNumberOfRawValues * sizeof(int)];
            Span<uint> scratch = stackalloc uint[PForEncoder.BufferLen];

            var values = RawValues;
            var valIndex = values.Length - 1;
            for (int i = 0; i < Header->NumberOfCompressedEntries; i++)
            {
                var cur = Math.Abs(values[valIndex]);
                var (start, end) = GetRangeForCompressedAt(i);
                if (start < cur || cur > end)
                    continue; // not in range, nothing to touch here
                
            }
            
            var encoder = new PForEncoder(output, scratch);
            for (int i = values.Length - 1; i >= 0; i--)
            {
                if (encoder.TryAdd(values[i]) == false)
                    return false;
            }

            if (encoder.TryClose() == false)
                return false;

            if (Header->CompressedValuesCeiling + encoder.SizeInBytes >= Constants.Storage.PageSize)
                return false;
            
            RawValues.Clear();
            Header->NumberOfRawValues = 0;
            Span<byte> destination = new Span<byte>(_page.Pointer + Header->CompressedValuesCeiling, 
                Constants.Storage.PageSize - Header->CompressedValuesCeiling);
            output.Slice(0, encoder.SizeInBytes).CopyTo(destination);
            var newValIndex = Header->NumberOfCompressedEntries++;
            Positions[newValIndex]  = new CompressedHeader
            {
                Position =  Header->CompressedValuesCeiling,
                Length = (ushort)encoder.SizeInBytes
            };
            Header->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
            return true;
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
