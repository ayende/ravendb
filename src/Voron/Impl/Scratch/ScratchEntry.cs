using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// A version-chain node, stored inline in <see cref="ScratchPagesTable"/>'s entries array.
    ///
    /// This holds no references, so the entries array is not traced by the GC no matter how many pages
    /// the scratch buffers hold - which is the whole point of storing versions here instead of as one
    /// object per version. The two references a version needs (the scratch file and the pager state that
    /// maps it) live in the generation's <see cref="ScratchRef"/> array, addressed by <see cref="RefIndex"/>;
    /// there is one of those per scratch file growth rather than one per page.
    /// </summary>
    internal struct ScratchEntry
    {
        internal long PositionInScratchBuffer;
        internal long PageNumberInDataFile;

        // Journal provenance for the flusher. Tombstones have no journal meaning and ride their kind here
        // (see PageFromScratchBuffer.TombstoneTx / SurvivingTombstoneTx).
        internal long AllocatedInTransaction;

        internal long Size;
        internal Page PreviousVersion;
        internal int NumberOfPages;

        // Index into the generation's ScratchRef array. -1 marks a tombstone: it has no scratch position,
        // so it has no file or pager state either.
        internal int RefIndex;

        // Next older version of the same page, in descending Seq order. -1 ends the chain.
        internal int OlderIndex;

        // Visibility stamp: the publish sequence of the write session that created this version.
        internal long Seq;

        internal readonly bool IsRemoved => RefIndex < 0;
    }

    /// <summary>
    /// The scratch file and pager state a run of versions was allocated against. There is one of these per
    /// scratch file growth - a new <see cref="Pager.State"/> comes from the remap path - so a generation
    /// holds tens of them, not millions.
    ///
    /// Lifetime stays with the GC: a published generation is captured by every snapshot taken against it,
    /// so a state is reachable exactly while some reader may still resolve a version that used it. That
    /// matters because Pager.State unmaps from its finalizer - releasing one early would leave a reader
    /// dereferencing unmapped memory.
    /// </summary>
    internal struct ScratchRef
    {
        internal ScratchBufferFile File;
        internal Pager.State State;
    }
}
