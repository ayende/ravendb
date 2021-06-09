using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron;
using Voron.Data;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Tryouts
{
    public readonly unsafe struct SetLeafPage
    {
        private readonly Page _page;
        private const int MaxNumberOfRawValues = 256;
        private const int MinNumberOfRawValues = 64;
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
                    int rawValueMasked = rawValue & int.MaxValue;
                    while (_current.IsEmpty || rawValueMasked <= _current[0])
                    {
                        _rawValuesIndex--;
                        if (_current.IsEmpty == false &&
                            rawValueMasked == _current[0])
                        {
                            _current = _current.Slice(1);
                        }
                        if (rawValue < 0)
                            continue; // removed, ignore
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

        public List<int> GetDebugOutput()
        {
            var list = new List<int>();
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
            Debug.Assert((value & ~int.MaxValue) == Header->Baseline);

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
                Header->CompressedValuesCeiling > OffsetOfRawValuesStart - sizeof(int)) // run into the compressed, cannot proceed
            {
                using var cmp = new Compressor(this, tx);
                if(cmp.TryCompressRawValues() == false)
                    return false;
                
                // we didn't free enough space
                if (Header->CompressedValuesCeiling  > Constants.Storage.PageSize - MinNumberOfRawValues * sizeof(int))
                    return false;
                return Add(tx, value);
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
                _parent._page.AsSpan().CopyTo(_tmpPage.AsSpan());
                _releaseOutput = tx.Allocator.Allocate(MaxNumberOfRawValues * sizeof(int), out _output);
                _output.Clear();
                _releaseScratch = tx.Allocator.Allocate(PForEncoder.BufferLen*2, out Span<int> scratch);
                _scratchDecoder = scratch.Slice(PForEncoder.BufferLen);
                _scratchEncoder = MemoryMarshal.Cast<int, uint>(scratch.Slice(0, PForEncoder.BufferLen));
                _rawValues = _parent.RawValues;
                _valIndex = _rawValues.Length - 1;
                _currentValMasked = -1;
                _tempHeader = (SetLeafPageHeader*)_tmpPage.TempPagePointer;
                _tempHeader->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries * sizeof(CompressedHeader));
                _tempHeader->NumberOfRawValues = 0;
                _tempPositions = new Span<CompressedHeader>(_tmpPage.TempPagePointer + PageHeader.SizeOf, MaxNumberOfCompressedEntries);
            }

            private const int MaxPreferredEntrySize = (Constants.Storage.PageSize / MaxNumberOfCompressedEntries);

            public bool TryCompressRawValues()
            {
                if (_parent.Header->NumberOfCompressedPositions == MaxNumberOfCompressedEntries)
                {
                    return CompactCompressedEntries();
                }

                // here we are trying to merge only the compressed entries that overlap 
                // with the raw values
                _currentValMasked = _valIndex >= 0 ? _rawValues[_valIndex] & int.MaxValue : -1;
                for (int i = 0; i < _parent.Header->NumberOfCompressedPositions; i++)
                {
                    ref var pos = ref _parent.Positions[i];
                    var end = _parent.GetCompressRangeEnd(ref pos);
                    if (end < _currentValMasked || _valIndex < 0)
                    {
                        _parent._page.AsSpan().Slice(pos.Position, pos.Length)
                            .CopyTo(_tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling));
                        _tempPositions[i] = new CompressedHeader {Length = pos.Length, Position = _tempHeader->CompressedValuesCeiling};
                        _tempHeader->CompressedValuesCeiling += pos.Length;
                        continue; // not in range, copy as is
                    }

                    var encoder = new PForEncoder(_output, _scratchEncoder);
                    if (EncodeWithRawValues(ref pos, ref encoder) == false || 
                        encoder.TryClose() == false) 
                        return false;
                    
                    if (_tempHeader->CompressedValuesCeiling + encoder.SizeInBytes > Constants.Storage.PageSize)
                    {
                        return false; // cannot fit any more...
                    }

                    _output.Slice(0, encoder.SizeInBytes).CopyTo(_tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling));
                    _tempPositions[i] = new CompressedHeader {Length = (ushort)encoder.SizeInBytes, Position = _tempHeader->CompressedValuesCeiling};
                    _tempHeader->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
                }

                var rawValEncoder = new PForEncoder(_output, _scratchEncoder);
                return TryFlushRawValuesToNewCompressedEntry(ref rawValEncoder);
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
                    _parent._page.AsSpan().Slice(pos.Position, pos.Length)
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
                if (_valIndex < 0)
                    return true; // nothing to do here
                
                for (; _valIndex >= 0; _valIndex--)
                {
                    if (encoder.TryAdd(_rawValues[_valIndex]) == false)
                        return false;
                }
                
                if (encoder.TryClose() == false)
                    return false;

                if (_tempHeader->CompressedValuesCeiling + encoder.SizeInBytes >= _tmpPage.PageSize)
                    return false;

                _output.Slice(0, encoder.SizeInBytes).CopyTo(
                    _tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling)
                );
                _tempPositions[_tempHeader->NumberOfCompressedPositions++] = new CompressedHeader
                {
                    Length = (ushort)encoder.SizeInBytes, 
                    Position = _tempHeader->CompressedValuesCeiling
                };
                _tempHeader->CompressedValuesCeiling += (ushort)encoder.SizeInBytes;
                _tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling).Clear();
                _tmpPage.AsSpan().CopyTo(_parent._page.AsSpan());
                return true;
            }

            private bool EncodeWithRawValues(ref CompressedHeader pos, ref PForEncoder encoder)
            {
                Span<byte> currentCompressBuffer = _parent._page.AsSpan().Slice(pos.Position, pos.Length);
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
                            Debug.Assert(_rawValues[_valIndex] >= 0);
                            if (encoder.TryAdd(_rawValues[_valIndex]) == false)
                                return false;
                            MoveToNextRawValue();
                        }
                        if (encoder.TryAdd(currentDecoded) == false)
                            return false;
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
    }
    
}
