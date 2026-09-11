using System.Threading;

namespace Voron.Util;

/// <summary>
/// The host's periodic hook into a long-running operation: it reports progress, decides whether the operation
/// should stop, and gets a chance to do bounded work of its own.
/// <para>
/// Always invoked on the thread that owns the operation, at a bounded boundary - never from a worker thread,
/// because a host may complete transactions here and Voron's completion callbacks require the transaction
/// lock's thread. Implemented by a struct and consumed through a generic constraint, so it costs no dispatch
/// and no allocation on the paths it sits in.
/// </para>
/// </summary>
public interface IOperationCheckpoint
{
    void Checkpoint(long allocatedUnmanagedBytes, CancellationToken token);
}

public readonly struct NoOpCheckpoint : IOperationCheckpoint
{
    public void Checkpoint(long allocatedUnmanagedBytes, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
    }
}
