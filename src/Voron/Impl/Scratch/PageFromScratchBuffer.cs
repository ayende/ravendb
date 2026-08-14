using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    public sealed class PageFromScratchBufferEqualityComparer : IEqualityComparer<PageFromScratchBuffer>
    {
        public static readonly PageFromScratchBufferEqualityComparer Instance = new PageFromScratchBufferEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(PageFromScratchBuffer x, PageFromScratchBuffer y)
        {
            if (x.File == null || y.File == null)
                return x.File == y.File;

            return x.PositionInScratchBuffer == y.PositionInScratchBuffer && x.Size == y.Size && x.NumberOfPages == y.NumberOfPages && x.File.Number == y.File.Number;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(PageFromScratchBuffer obj)
        {
            if (obj.File == null)
                return 0;

            int v = Hashing.Combine(obj.NumberOfPages, obj.File.Number);
            int w = Hashing.Combine(obj.Size.GetHashCode(), obj.PositionInScratchBuffer.GetHashCode());
            return Hashing.Combine(v, w);
        }
    }

    /// <summary>
    /// A page's location in the scratch buffers, handed out by value.
    ///
    /// This used to be the version-chain node itself, which is why it was a class: it held a reference to
    /// the next older version. The chain now lives in <see cref="ScratchEntry"/> as an index, so this
    /// carries only the payload and can be a struct - there is no per-version object left to allocate or
    /// for the GC to trace.
    ///
    /// <see cref="File"/> being null marks the absence of a page: either a tombstone read out of the
    /// table, or a default value standing in for what used to be a null reference.
    /// </summary>
    public readonly record struct PageFromScratchBuffer(
        ScratchBufferFile File,
        Pager.State State,
        long AllocatedInTransaction,
        long PositionInScratchBuffer,
        long PageNumberInDataFile,
        Page PreviousVersion,
        long Size,
        int NumberOfPages
    )
    {
        // Tombstones are the only versions without a scratch position; the tombstone kind rides on
        // AllocatedInTransaction, which has no journal meaning for them
        internal const long TombstoneTx = -1;
        internal const long SurvivingTombstoneTx = -2;

        public bool IsValid => File != null;

        internal bool IsRemoved => File == null;

        // free-after-flush tombstones only: the matching scratch pool free is not undone by a rollback (RavenDB-27166)
        internal bool SurvivesRollback => AllocatedInTransaction == SurvivingTombstoneTx;

        public unsafe Page ReadPage(LowLevelTransaction tx)
        {
            return new Page(Read(ref tx.PagerTransactionState));
        }

        public unsafe byte* Read(ref Pager.PagerTransactionState txState)
        {
            File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
            return File.Pager.AcquirePagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
        }

        public unsafe byte* ReadRawPagePointer(ref Pager.PagerTransactionState txState)
        {
            File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
            return File.Pager.AcquireRawPagePointer(State, ref txState, PositionInScratchBuffer);
        }

        public unsafe Page ReadNewPage(LowLevelTransaction tx)
        {
            var p = File.Pager.AcquirePagePointerForNewPage(State, ref tx.PagerTransactionState, PositionInScratchBuffer, NumberOfPages);
            p = File.Pager.MakeWritable(State, p);
            return new Page(p);
        }

        public unsafe Page ReadRawPage(LowLevelTransaction tx)
        {
            return new Page(ReadRaw(ref tx.PagerTransactionState));
        }

        public unsafe byte* ReadRaw(ref Pager.PagerTransactionState txState)
        {
            File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
            return File.Pager.AcquireRawPagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
        }

        public unsafe Page ReadWritable(LowLevelTransaction tx)
        {
            return new Page(ReadWritable(ref tx.PagerTransactionState));
        }

        public unsafe byte* ReadWritable(ref Pager.PagerTransactionState txPagerTransactionState)
        {
            var ptr = Read(ref txPagerTransactionState);
            return File.Pager.MakeWritable(State, ptr);
        }

        public unsafe byte* ReadWritableRawPagePointer(ref Pager.PagerTransactionState txPagerTransactionState)
        {
            var ptr = ReadRawPagePointer(ref txPagerTransactionState);
            return File.Pager.MakeWritable(State, ptr);
        }
    }
}
