using System;
using Sparrow;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    public sealed class PageFromScratchBufferEqualityComparer : IEqualityComparer<PageFromScratchBuffer>
    {
        public static readonly PageFromScratchBufferEqualityComparer Instance = new PageFromScratchBufferEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(PageFromScratchBuffer x, PageFromScratchBuffer y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;            

            return x.PositionInScratchBuffer == y.PositionInScratchBuffer && x.Size == y.Size && x.NumberOfPages == y.NumberOfPages && x.File.Number == y.File.Number;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(PageFromScratchBuffer obj)
        {
            int v = Hashing.Combine(obj.NumberOfPages, obj.File.Number);
            int w = Hashing.Combine(obj.Size.GetHashCode(), obj.PositionInScratchBuffer.GetHashCode());
            return Hashing.Combine(v, w);
        }
    }


    public sealed record PageFromScratchBuffer(
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
        // The state of this page in the scratch table - 16 bytes, no padding
        internal struct ScratchTableState
        {
            // next older version of the same page, in descending Seq order
            internal PageFromScratchBuffer Older;

            // Visibility stamp: the ScratchPagesTable publish sequence of the write session that
            // created this node. Distinct from AllocatedInTransaction (journal provenance, used by
            // the flusher) - the sequence advances on every published record, including book-keeping
            // commits that do not consume a transaction id.
            internal long Seq;
        }

        internal ScratchTableState Chain;

        // Tombstones are the only nodes without a scratch position; the tombstone kind rides on
        // AllocatedInTransaction, which has no journal meaning for them
        private const long TombstoneTx = -1;
        private const long SurvivingTombstoneTx = -2;

        internal bool IsRemoved => File == null;

        // free-after-flush tombstones only: the matching scratch pool free is not undone by a rollback (RavenDB-27166)
        internal bool SurvivesRollback => AllocatedInTransaction == SurvivingTombstoneTx;

        internal static PageFromScratchBuffer CreateTombstone(long pageNumberInDataFile, long seq, bool survivesRollback)
        {
            return new PageFromScratchBuffer(null, null, survivesRollback ? SurvivingTombstoneTx : TombstoneTx, -1, pageNumberInDataFile, default, 0, 0)
            {
                Chain = new() { Seq = seq }
            };
        }

        // Instances are version-chain nodes with *reference* identity not structural equality
        public bool Equals(PageFromScratchBuffer other) => ReferenceEquals(this, other);

        public override int GetHashCode() => RuntimeHelpers.GetHashCode(this);

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
