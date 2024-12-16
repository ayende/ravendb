using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;

namespace Voron;

public record EnvironmentStateRecord(
    Pager.State DataPagerState, 
    long TransactionId,
    ImmutableDictionary<long, PageFromScratchBuffer> ScratchPagesTable,
    
    TreeRootHeader Root,
    long NextPageNumber,
    (long Number, long Last4KWritePosition) Journal,
    List<long> SparsePageRanges);

public record SparseRegionsRecord(
    long TransactionId,
    List<(long Start, long Count)> Regions
);

internal sealed class EnvironmentStateRecordHolder
{
    public EnvironmentStateRecord EnvStateRecord;
}
