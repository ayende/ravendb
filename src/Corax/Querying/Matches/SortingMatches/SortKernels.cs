using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Reusable per-type sort kernels extracted from the EntryComparer family.
/// Each kernel fetches secondary values for a group of entry IDs and produces
/// a sorted index array. Callers (SortingMatch comparers, SortedDrivingWithTieBreakMatch)
/// walk the index array to emit results in sorted order.
///
/// All kernels sort ascending. Callers that need descending order iterate the
/// index array from the end (descending-on-read), avoiding delegate-backed
/// comparers entirely.
/// </summary>
internal static class SortKernels
{
    /// <summary>
    /// Resolve the long (Integer/Floating-bits) secondary values for a group of entries without sorting.
    /// Used by bounded top-K selection, which picks survivors via a heap instead of a full sort.
    /// </summary>
    public static void ResolveLongs(
        Lookup<Int64LookupKey> lookup,
        Span<long> entries,
        Span<long> valuesOut,
        long missingValue)
    {
        int n = entries.Length;
        Debug.Assert(valuesOut.Length >= n);
        if (lookup != null)
            lookup.GetFor(entries, valuesOut, missingValue);
        else
            valuesOut.Slice(0, n).Fill(missingValue);
    }

    /// <summary>
    /// Resolve the CompactKey term blobs for a group of entries (Sequence secondary field) without sorting.
    /// Mirrors the resolution half of <see cref="SortBySlice"/>; used by bounded top-K selection.
    /// </summary>
    public static unsafe void ResolveSlices(
        Lookup<Int64LookupKey> lookup,
        LowLevelTransaction llt,
        PageLocator pageLocator,
        Span<long> entries,
        Span<long> termIdsScratch,
        Span<UnmanagedSpan> termsOut,
        long nullTermContainerId,
        long nonExistingTermContainerId)
    {
        int n = entries.Length;
        Debug.Assert(termIdsScratch.Length >= n);
        Debug.Assert(termsOut.Length >= n);
        if (lookup != null)
        {
            lookup.GetFor(entries, termIdsScratch, SortingHelpers.MissingTermId);
            SortingHelpers.ReplaceNullAndNonExistingTermIds(
                termIdsScratch.Slice(0, n), nonExistingTermContainerId, nullTermContainerId, SortingHelpers.MissingTermId);
            Container.GetAllSortedByPage(llt, termIdsScratch.Slice(0, n), termsOut.Slice(0, n), SortingHelpers.MissingTermId, pageLocator);
        }
        else
        {
            termsOut.Slice(0, n).Fill(default);
        }
    }

    /// <summary>
    /// Sort a group of entries by a long (Integer) secondary field.
    /// Populates <paramref name="indexesOut"/> with indices into <paramref name="entries"/>
    /// ordered by the secondary long value (ascending).
    /// </summary>
    /// <param name="lookup">Entries-to-terms lookup for the secondary field (may be null if field doesn't exist).</param>
    /// <param name="entries">Entry IDs to sort.</param>
    /// <param name="valuesScratch">Scratch buffer for secondary values, must be at least <paramref name="entries"/>.Length.</param>
    /// <param name="indexesOut">Output index array; must be padded to a multiple of 8 for SIMD init.
    /// On return, indexesOut[0..entries.Length) contains the ascending sort permutation.</param>
    /// <param name="missingValue">Value used for entries that lack the secondary field.</param>
    public static void SortByLong(
        Lookup<Int64LookupKey> lookup,
        Span<long> entries,
        Span<long> valuesScratch,
        Span<int> indexesOut,
        long missingValue)
    {
        int n = entries.Length;
        Debug.Assert(valuesScratch.Length >= n);
        Debug.Assert(indexesOut.Length >= n);

        if (lookup != null)
            lookup.GetFor(entries, valuesScratch, missingValue);
        else
            valuesScratch.Slice(0, n).Fill(missingValue);

        RoaringBitmap.InitializeIndices(indexesOut, n);
        var vals = valuesScratch.Slice(0, n);
        var idxs = indexesOut.Slice(0, n);
        vals.Sort(idxs);
    }

    /// <summary>
    /// Sort a group of entries by a double (Floating) secondary field.
    /// The lookup stores bit-cast doubles as longs; this kernel reinterprets them
    /// as doubles before sorting (required for correct negative-value ordering).
    /// </summary>
    public static void SortByDouble(
        Lookup<Int64LookupKey> lookup,
        Span<long> entries,
        Span<long> valuesScratch,
        Span<int> indexesOut,
        long missingValueBits)
    {
        int n = entries.Length;
        Debug.Assert(valuesScratch.Length >= n);
        Debug.Assert(indexesOut.Length >= n);

        if (lookup != null)
            lookup.GetFor(entries, valuesScratch, missingValueBits);
        else
            valuesScratch.Slice(0, n).Fill(missingValueBits);

        RoaringBitmap.InitializeIndices(indexesOut, n);

        // Reinterpret as doubles for correct sort order (long bit-sort is wrong for negative doubles).
        var doubleSpan = MemoryMarshal.Cast<long, double>(valuesScratch).Slice(0, n);
        var idxs = indexesOut.Slice(0, n);
        doubleSpan.Sort(idxs);
    }

    /// <summary>
    /// Sort a group of entries by a string/Slice secondary field (CompactKey byte order).
    /// Fetches term container IDs via the lookup, resolves them to CompactKey blobs via
    /// Container.GetAll, then sorts by the CompactKey byte comparison.
    /// </summary>
    /// <param name="lookup">Entries-to-terms lookup for the secondary field (may be null).</param>
    /// <param name="llt">Low-level transaction for container reads.</param>
    /// <param name="pageLocator">Page locator for container reads.</param>
    /// <param name="entries">Entry IDs to sort.</param>
    /// <param name="termIdsScratch">Scratch for term container IDs, length >= entries.Length.</param>
    /// <param name="termsScratch">Scratch for resolved CompactKey blobs, length >= entries.Length.</param>
    /// <param name="indexesOut">Output index array, padded to multiple of 8.</param>
    /// <param name="nullTermContainerId">Container ID for the null term, or SortingHelpers.InvalidTermId.</param>
    /// <param name="nonExistingTermContainerId">Container ID for the non-existing term, or SortingHelpers.InvalidTermId.</param>
    public static unsafe void SortBySlice(
        Lookup<Int64LookupKey> lookup,
        LowLevelTransaction llt,
        PageLocator pageLocator,
        Span<long> entries,
        Span<long> termIdsScratch,
        Span<UnmanagedSpan> termsScratch,
        Span<int> indexesOut,
        long nullTermContainerId,
        long nonExistingTermContainerId)
    {
        int n = entries.Length;
        Debug.Assert(termIdsScratch.Length >= n);
        Debug.Assert(termsScratch.Length >= n);
        Debug.Assert(indexesOut.Length >= n);

        if (lookup != null)
        {
            lookup.GetFor(entries, termIdsScratch, SortingHelpers.MissingTermId);
            SortingHelpers.ReplaceNullAndNonExistingTermIds(
                termIdsScratch.Slice(0, n), nonExistingTermContainerId, nullTermContainerId, SortingHelpers.MissingTermId);
            Container.GetAllSortedByPage(llt, termIdsScratch.Slice(0, n), termsScratch.Slice(0, n), SortingHelpers.MissingTermId, pageLocator);
        }
        else
        {
            // Field doesn't exist; all terms are null (address = null).
            termsScratch.Slice(0, n).Fill(default);
        }

        RoaringBitmap.InitializeIndices(indexesOut, n);

        // Sort by CompactKey byte order. Null entries (Address == null) sort to one end.
        var idxs = indexesOut.Slice(0, n);
        fixed (UnmanagedSpan* termsPtr = termsScratch)
        {
            idxs.Sort(new SliceComparer(termsPtr));
        }
    }

    /// <summary>
    /// IComparer for CompactKey-encoded UnmanagedSpan values, used by <see cref="SortBySlice"/>.
    /// Null entries (Address == null) sort before non-null entries (ascending null-first).
    /// </summary>
    private readonly unsafe struct SliceComparer : IComparer<int>
    {
        private readonly UnmanagedSpan* _terms;

        public SliceComparer(UnmanagedSpan* terms) => _terms = terms;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            ref var xItem = ref _terms[x];
            ref var yItem = ref _terms[y];

            if (yItem.Address == null)
                return xItem.Address == null ? 0 : 1;
            if (xItem.Address == null)
                return -1;

            var cmp = Memory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
            if (cmp != 0)
                return cmp;

            var xBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
            var yBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
            return xBits - yBits;
        }
    }
}
