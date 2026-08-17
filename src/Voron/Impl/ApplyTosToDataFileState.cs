using System;
using System.Collections.Generic;
using Voron.Impl.Scratch;

namespace Voron.Impl;

record ApplyLogsToDataFileState(
    List<PageFromScratchBuffer> Buffers,
    // page number -> the newest flushed transaction that freed it; a version of the page from an older
    // transaction is stale and must not be written, its pages may already belong to another allocation
    Dictionary<long, long> FreedPages,
    List<(long Start, long Count)> SparseRegions,
    EnvironmentStateRecord Record)
{
    public override string ToString()
    {
        return Record.DataPagerState.Pager.FileName;
    }
}
