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
/// Wraps a SortedIndexReader to produce entry IDs in field-value order (not entry-ID order).
/// Used as the driving match for DirectScanMatch when the ORDER BY field has a range clause.
/// Unlike TermsProviderMatch (which materializes to bitmap and loses sort order), this
/// preserves the tree's term ordering.
/// </summary>
public sealed class SortedDrivingMatch : IQueryMatch, IDisposable
{
    private readonly IndexSearcher _searcher;
    private readonly LowLevelTransaction _llt;
    private readonly string _fieldName;
    private readonly bool _ascending;
    private readonly long _min;
    private readonly long _max;
    private readonly bool _nullFirst;
    private readonly object _seekValue;

    // The actual reader — created lazily on first Fill because we need the generic TDirection
    private IDisposable _readerDisposable;
    private Func<Span<long>, int> _readFunc;
    private bool _initialized;

    // Dedup for multi-value fields
    private RoaringBitmap _emittedBitmap;

    public SortedDrivingMatch(IndexSearcher searcher, string fieldName, bool ascending,
        long min = long.MinValue, long max = long.MaxValue, bool nullFirst = false, object seekValue = null)
    {
        _searcher = searcher;
        _llt = searcher.Transaction.LowLevelTransaction;
        _fieldName = fieldName;
        _ascending = ascending;
        _min = min;
        _max = max;
        _nullFirst = nullFirst;
        _seekValue = seekValue;
        _emittedBitmap = new RoaringBitmap(searcher.Allocator);
    }

    public long Count => -1;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

    public unsafe int Fill(Span<long> matches)
    {
        if (_initialized == false)
        {
            Initialize();
            _initialized = true;
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
                if (_emittedBitmap.Contains(id) == false)
                {
                    _emittedBitmap.Add(id);
                    matches[count++] = id;
                }
            }
        }

        return count;
    }

    private void Initialize()
    {
        // Determine field type and create the appropriate SortedIndexReader
        var fieldMeta = _searcher.FieldMetadataBuilder(_fieldName);
        Slice.From(_llt.Allocator, _fieldName, out var fieldSlice);

        // Try numeric long tree first
        var longFieldMeta = fieldMeta.GetNumericFieldMetadata<long>(_llt.Allocator);
        var longTree = _searcher.GetLongTermsFor(longFieldMeta.FieldName);
        if (longTree != null)
        {
            if (_ascending)
                CreateReader<Lookup<Int64LookupKey>.ForwardIterator>(longTree.Iterate<Lookup<Int64LookupKey>.ForwardIterator>(), longFieldMeta);
            else
                CreateReader<Lookup<Int64LookupKey>.BackwardIterator>(longTree.Iterate<Lookup<Int64LookupKey>.BackwardIterator>(), longFieldMeta);
            return;
        }

        // Try numeric double tree
        var doubleFieldMeta = fieldMeta.GetNumericFieldMetadata<double>(_llt.Allocator);
        var doubleTree = _searcher.GetDoubleTermsFor(doubleFieldMeta.FieldName);
        if (doubleTree != null)
        {
            if (_ascending)
                CreateReader<Lookup<DoubleLookupKey>.ForwardIterator>(doubleTree.Iterate<Lookup<DoubleLookupKey>.ForwardIterator>(), doubleFieldMeta);
            else
                CreateReader<Lookup<DoubleLookupKey>.BackwardIterator>(doubleTree.Iterate<Lookup<DoubleLookupKey>.BackwardIterator>(), doubleFieldMeta);
            return;
        }

        // String tree (CompactTree)
        var termsTree = _searcher.GetTermsFor(fieldSlice);
        if (termsTree != null)
        {
            if (_ascending)
                CreateReader<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>(termsTree.IterateValues<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>(), fieldMeta, fieldSlice);
            else
                CreateReader<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(termsTree.IterateValues<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(), fieldMeta, fieldSlice);
        }
    }

    private void CreateReader<TDirection>(TDirection iterator, FieldMetadata fieldMeta, Slice fieldSlice = default)
        where TDirection : struct, ILookupIterator
    {
        SortingMatch<AllEntriesMatch>.SortedIndexReader<TDirection>.SeekAction seekAction = null;

        // Build seek action from the seek value
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
            var termsTree = _searcher.GetTermsFor(fieldSlice);
            if (termsTree != null)
            {
                var compactKey = _llt.AcquireCompactKey();
                compactKey.Set(System.Text.Encoding.UTF8.GetBytes(strVal));
                compactKey.ChangeDictionary(termsTree.DictionaryId);
                seekAction = (ref TDirection it) => it.Seek(new CompactTree.CompactKeyLookup(compactKey));
            }
        }

        var reader = new SortingMatch<AllEntriesMatch>.SortedIndexReader<TDirection>(
            _llt, _searcher, iterator, fieldMeta, _min, _max, _nullFirst, _ascending, seekAction);

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
                ["Direction"] = _ascending ? "Forward" : "Backward"
            });
    }

    public void Dispose()
    {
        _readerDisposable?.Dispose();
        _emittedBitmap.Dispose();
    }
}
