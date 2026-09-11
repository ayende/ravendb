using System.Threading;
using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public readonly struct CoraxIndexingStats(IndexingStatsScope stats, Index index) : ICoraxIndexingScope<CoraxIndexingStats>
{
    public CoraxIndexingStats For(string name, bool start = true) => new CoraxIndexingStats(stats.For(name, start), index);

    public void Checkpoint(long allocatedUnmanagedBytes, CancellationToken token)
    {
        stats.SetAllocatedUnmanagedBytes(allocatedUnmanagedBytes);

        index?.IndexingCheckpointWithoutCancellation();
        token.ThrowIfCancellationRequested();
    }

    public void Dispose()
    {
        stats?.Dispose();
    }
}
