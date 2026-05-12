using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>
/// Two-phase driving match for DirectScanMatch:
/// Phase 1: Build a bitmap from the range query (via TermsProviderMatch — correct range filtering).
/// Phase 2: Walk the tree in field-value sort order via SortedIndexReader, filtering against the
///           bitmap via Contains. This preserves field-value order while respecting range bounds.
///
/// This is the same approach as SortUsingIndexFromBitmap but packaged as an IQueryMatch
/// so DirectScanMatch can use it as its driving match.
/// </summary>
public sealed class SortedDrivingMatch : IQueryMatch, IDisposable
{
    private readonly IndexSearcher _searcher;
    private readonly LowLevelTransaction _llt;
    private readonly IQueryMatch _rangeMatch;  // TermsProviderMatch for building the bitmap
    private readonly string _fieldName;
    private readonly bool _ascending;
    private readonly bool _nullFirst;
    private readonly object _seekValue;

    // Bitmap from the range query (built on first Fill)
    private RoaringBitmap _bitmap;
    private bool _bitmapBuilt;

    // Sorted tree walker
    private IDisposable _readerDisposable;
    private Func<Span<long>, int> _readFunc;

    // Dedup for multi-value fields
    private RoaringBitmap _emittedBitmap;

    public SortedDrivingMatch(IndexSearcher searcher, IQueryMatch rangeMatch, string fieldName,
        bool ascending, bool nullFirst = false, object seekValue = null)
    {
        _searcher = searcher;
        _llt = searcher.Transaction.LowLevelTransaction;
        _rangeMatch = rangeMatch;
        _fieldName = fieldName;
        _ascending = ascending;
        _nullFirst = nullFirst;
        _seekValue = seekValue;
        _emittedBitmap = new RoaringBitmap(searcher.Allocator);
    }

    public long Count => _bitmapBuilt ? _bitmap.Count : -1;
    public QueryCountConfidence Confidence => _bitmapBuilt ? QueryCountConfidence.High : QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

    public unsafe int Fill(Span<long> matches)
    {
        if (_bitmapBuilt == false)
        {
            BuildBitmapAndInitReader();
            _bitmapBuilt = true;
        }

        if (_readFunc == null)
            return 0;

        int count = 0;
        Span<long> buffer = stackalloc long[Math.Min(1024, matches.Length * 2)];

        while (count < matches.Length)
        {
            int read = _readFunc(buffer);
            if (read == 0)
                break;

            for (int i = 0; i < read && count < matches.Length; i++)
            {
                long id = buffer[i];
                // Filter against bitmap (range bounds) and dedup
                if (_bitmap.Contains(id) && _emittedBitmap.Contains(id) == false)
                {
                    _emittedBitmap.Add(id);
                    matches[count++] = id;
                }
            }
        }

        return count;
    }

    private void BuildBitmapAndInitReader()
    {
        // Phase 1: Build bitmap from range match (applies correct range bounds)
        _bitmap = new RoaringBitmap(_searcher.Allocator);
        Span<long> buf = stackalloc long[4096];
        int read;
        while ((read = _rangeMatch.Fill(buf)) > 0)
            _bitmap.AddRange(buf.Slice(0, read));
        _bitmap.PrepareForReading();

        // Phase 2: Create SortedIndexReader for the sort tree walk
        var fieldMeta = _searcher.FieldMetadataBuilder(_fieldName);
        Slice.From(_llt.Allocator, _fieldName, out var fieldSlice);
        long min = _bitmap.IsEmpty ? 0 : _bitmap.MinContainerKey * RoaringBitmap.ContainerSize;
        long max = _bitmap.IsEmpty ? 0 : (_bitmap.MaxContainerKey + 1) * RoaringBitmap.ContainerSize - 1;

        // Try numeric long
        var longMeta = fieldMeta.GetNumericFieldMetadata<long>(_llt.Allocator);
        var longTree = _searcher.GetLongTermsFor(longMeta.FieldName);
        if (longTree != null)
        {
            if (_ascending)
                CreateReader(longTree.Iterate<Lookup<Int64LookupKey>.ForwardIterator>(), longMeta, min, max);
            else
                CreateReader(longTree.Iterate<Lookup<Int64LookupKey>.BackwardIterator>(), longMeta, min, max);
            return;
        }

        // Try numeric double
        var doubleMeta = fieldMeta.GetNumericFieldMetadata<double>(_llt.Allocator);
        var doubleTree = _searcher.GetDoubleTermsFor(doubleMeta.FieldName);
        if (doubleTree != null)
        {
            if (_ascending)
                CreateReader(doubleTree.Iterate<Lookup<DoubleLookupKey>.ForwardIterator>(), doubleMeta, min, max);
            else
                CreateReader(doubleTree.Iterate<Lookup<DoubleLookupKey>.BackwardIterator>(), doubleMeta, min, max);
            return;
        }

        // String tree
        var termsTree = _searcher.GetTermsFor(fieldSlice);
        if (termsTree != null)
        {
            if (_ascending)
                CreateReader(termsTree.IterateValues<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>(), fieldMeta, min, max, fieldSlice);
            else
                CreateReader(termsTree.IterateValues<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(), fieldMeta, min, max, fieldSlice);
        }
    }

    private void CreateReader<TDirection>(TDirection iterator, FieldMetadata fieldMeta, long min, long max, Slice fieldSlice = default)
        where TDirection : struct, ILookupIterator
    {
        SortingMatch<AllEntriesMatch>.SortedIndexReader<TDirection>.SeekAction seekAction = null;

        if (_seekValue is long longVal &&
            (typeof(TDirection) == typeof(Lookup<Int64LookupKey>.ForwardIterator) ||
             typeof(TDirection) == typeof(Lookup<Int64LookupKey>.BackwardIterator)))
        {
            seekAction = (ref TDirection it) => it.Seek(new Int64LookupKey(longVal));
        }
        else if (_seekValue is double doubleVal &&
            (typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.ForwardIterator) ||
             typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.BackwardIterator)))
        {
            seekAction = (ref TDirection it) => it.Seek(new DoubleLookupKey(doubleVal));
        }
        else if (_seekValue is string strVal &&
            (typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.ForwardIterator) ||
             typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.BackwardIterator)))
        {
            var tree = _searcher.GetTermsFor(fieldSlice);
            if (tree != null)
            {
                var ck = _llt.AcquireCompactKey();
                ck.Set(System.Text.Encoding.UTF8.GetBytes(strVal));
                ck.ChangeDictionary(tree.DictionaryId);
                seekAction = (ref TDirection it) => it.Seek(new CompactTree.CompactKeyLookup(ck));
            }
        }

        var reader = new SortingMatch<AllEntriesMatch>.SortedIndexReader<TDirection>(
            _llt, _searcher, iterator, fieldMeta, min, max, _nullFirst, _ascending, seekAction);

        _readFunc = (Span<long> buf) => reader.Read(buf);
        _readerDisposable = reader;
    }

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException();
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }
    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode("SortedDrivingMatch",
            parameters: new Dictionary<string, string>
            {
                ["Field"] = _fieldName,
                ["Direction"] = _ascending ? "Forward" : "Backward",
                ["BitmapCount"] = _bitmapBuilt ? _bitmap.Count.ToString() : "not built"
            });
    }

    public void Dispose()
    {
        _readerDisposable?.Dispose();
        if (_bitmapBuilt) _bitmap.Dispose();
        _emittedBitmap.Dispose();
        (_rangeMatch as IDisposable)?.Dispose();
    }
}
