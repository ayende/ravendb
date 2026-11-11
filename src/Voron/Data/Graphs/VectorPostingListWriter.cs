using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Compression;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Voron.Data.Graphs;

/// <summary>
/// Applies entry additions / removals to the posting list of a vector node. The posting list value is tagged by its two
/// lowest bits, see <see cref="Constants.Graphs.VectorId"/>: a tombstone, a single entry, a small posting list stored as a
/// container item, or a full <see cref="PostingList"/>.
/// </summary>
public unsafe struct VectorPostingListWriter : IDisposable
{
    private readonly LowLevelTransaction _llt;
    private readonly ContainerId _container;
    private FastPForEncoder _pforEncoder;
    private FastPForDecoder _pforDecoder;
    private ContextBoundNativeList<long> _listBuffer;
    private ContextBoundNativeList<byte> _byteBuffer;
    private PostingList _largePostingListSet;

    public VectorPostingListWriter(LowLevelTransaction llt, ContainerId container)
    {
        _llt = llt;
        _container = container;
        _pforEncoder = new FastPForEncoder(llt.Allocator);
        _pforDecoder = new FastPForDecoder(llt.Allocator);
        _listBuffer = new ContextBoundNativeList<long>(llt.Allocator);
        _byteBuffer = new ContextBoundNativeList<byte>(llt.Allocator);
        _byteBuffer.EnsureCapacityFor(128);
        _largePostingListSet = null;
    }

    public long Merge(long postingList, NativeList<long> modifications)
    {
        // Duplicates are expected: a document may hold the same vector several times, and a removal may be recorded for each
        // of them. Vector posting lists carry no frequency, so we only need to know that there was an addition / a removal.
        modifications.Shrink(Sorting.SortAndRemoveDuplicates(modifications.ToSpan()));

        _listBuffer.Clear();
        _listBuffer.AddRange(modifications.ToSpan());

        int currentSize = 0;
        bool hasSmallPostingList = false;
        ContainerEntryId rawPostingListId = new ContainerEntryId(postingList & Constants.Graphs.VectorId.ContainerType);

        switch (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask)
        {
            case Constants.Graphs.VectorId.Tombstone:
                break;
            case Constants.Graphs.VectorId.Single:
                _listBuffer.Add((long)rawPostingListId);
                break;
            case Constants.Graphs.VectorId.SmallPostingList:
                hasSmallPostingList = true;
                Hnsw.SearchState.ReadPostingList(_llt, rawPostingListId, ref _listBuffer, ref _pforDecoder, out currentSize);
                break;
            case Constants.Graphs.VectorId.PostingList:
                return UpdatePostingList(rawPostingListId, in modifications);
        }

        // After the deduplication above, an entry id may show up as: 2x addition + 1x removal (an update, the id remains),
        // 1x addition + 1x removal (a delete, the id goes away) or 1x addition (a new document).
        PostingList.SortModificationsAndRemoveDuplicates(ref _listBuffer);

        if (_listBuffer.Count is 0 or 1)
        {
            if (hasSmallPostingList)
                Container.Delete(_llt, _container, rawPostingListId);

            if (_listBuffer.Count is 0)
                return 0;

            Debug.Assert((_listBuffer[0] & Constants.Graphs.VectorId.PostingList) == 0, "(listBuffer[0] & 0b11) == 0");
            return _listBuffer[0] | Constants.Graphs.VectorId.Single;
        }

        int size = _pforEncoder.Encode(_listBuffer.RawItems, _listBuffer.Count);
        if (size > Container.MaxSizeInsideContainerPage)
        {
            if (hasSmallPostingList)
                Container.Delete(_llt, _container, rawPostingListId);
            return CreateNewPostingList();
        }

        _byteBuffer.EnsureCapacityFor(size + 5);
        var offset = VariableSizeEncoding.Write(_byteBuffer.RawItems, _listBuffer.Count);
        (int itemsCount, int sizeUsed) = _pforEncoder.Write(_byteBuffer.RawItems + offset, _byteBuffer.Capacity - offset);
        _byteBuffer.Count = sizeUsed + offset;
        Debug.Assert(itemsCount == _listBuffer.Count && sizeUsed == size, "itemsCount == listBuffer.Count && sizeUsed == size");
        Span<byte> mutable;
        if (currentSize == _byteBuffer.Count)
        {
            mutable = Container.GetMutable(_llt, rawPostingListId);
        }
        else
        {
            if (hasSmallPostingList)
                Container.Delete(_llt, _container, rawPostingListId);
            rawPostingListId = Container.Allocate(_llt, _container, _byteBuffer.Count, out mutable);
        }

        _byteBuffer.ToSpan().CopyTo(mutable);

        Debug.Assert(((long)rawPostingListId & Constants.Graphs.VectorId.PostingList) == 0, "(rawPostingListId & 0b11) == 0");
        return (long)rawPostingListId | Constants.Graphs.VectorId.SmallPostingList;
    }

    /// <summary>
    /// Releases the storage of a posting list that is no longer referenced.
    /// </summary>
    public void Delete(long postingList)
    {
        var raw = new ContainerEntryId(postingList & Constants.Graphs.VectorId.ContainerType);
        switch (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask)
        {
            case Constants.Graphs.VectorId.SmallPostingList:
                Container.Delete(_llt, _container, raw);
                break;
            case Constants.Graphs.VectorId.PostingList:
                _largePostingListSet ??= _llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
                _largePostingListSet.Remove((long)raw);
                Container.Delete(_llt, _container, raw);
                break;
        }
    }

    private long UpdatePostingList(ContainerEntryId postingListId, in NativeList<long> modifications)
    {
        var setSpace = Container.GetMutable(_llt, postingListId);
        ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

        var lists = stackalloc long*[2];
        var indexes = stackalloc int[2];

        using var _1 = _llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs1);
        using var _2 = _llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs2);
        lists[0] = (long*)bs1.Ptr;
        lists[1] = (long*)bs2.Ptr;

        for (int i = 0; i < modifications.Count; i++)
        {
            var cur = modifications[i];
            var listIdx = cur & 1;
            var curIndex = indexes[listIdx]++;
            lists[listIdx][curIndex] = cur;
        }

        var numberOfEntries = PostingList.Update(_llt, ref postingListState, lists[0], indexes[0],
            lists[1], indexes[1], _pforEncoder, ref _listBuffer, ref _pforDecoder);

        if (numberOfEntries is 0)
        {
            _largePostingListSet ??= _llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
            _largePostingListSet.Remove((long)postingListId);
            Container.Delete(_llt, _container, postingListId);
            return 0;
        }

        return (long)postingListId | Constants.Graphs.VectorId.PostingList;
    }

    private long CreateNewPostingList()
    {
        var setId = Container.Allocate(_llt, _container, sizeof(PostingListState), out var setSpace);

        _largePostingListSet ??= _llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
        _largePostingListSet.Add((long)setId);

        ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
        PostingList.Create(_llt, ref postingListState, _pforEncoder);
        return (long)setId | Constants.Graphs.VectorId.PostingList;
    }

    public void Dispose()
    {
        _listBuffer.Dispose();
        _byteBuffer.Dispose();
        _pforEncoder.Dispose();
        _pforDecoder.Dispose();
    }
}
