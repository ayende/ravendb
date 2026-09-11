using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Raven.Server.Documents;

internal interface IAsyncCommittedBatch
{
    // journal write was durable (null means completed inline)
    Task DurableCommit { get; }

    // Complete the batch and release its resources, must be called on the tx merger thread
    ExceptionDispatchInfo Complete(Exception priorFailure);
}

internal sealed class AsyncCommitCompletionPump<TBatch>
    where TBatch : class, IAsyncCommittedBatch
{
    private readonly Queue<TBatch> _inFlight = new();
    private ExceptionDispatchInfo _failure;

    private Task _headDurableCommit;

    public bool HasPendingCompletions => _inFlight.Count > 0;

    public bool OldestTransactionIsDurable => _headDurableCommit is { IsCompleted: true };

    public Task OldestDurableCommit => _headDurableCommit;

    private void UpdateHeadDurableCommit() => _headDurableCommit = _inFlight.TryPeek(out var head) ? head.DurableCommit : null;

    public void Release(TBatch batch)
    {
        _inFlight.Enqueue(batch);
        if (_inFlight.Count == 1)
            UpdateHeadDurableCommit();

        // it can be completed inline if the durable commit is already done, otherwise we wait for the ack
        if (batch.DurableCommit is not { IsCompleted: false })
            DrainCompleted();
    }

    public void DrainCompleted()
    {
        while (_inFlight.TryPeek(out var head)) // in commit order, so no tx can be raised before its predecessor
        {
            if (_failure == null && head.DurableCommit is { IsCompleted: false })
                break; // its ack wakes us again

            Complete(_inFlight.Dequeue());
        }

        UpdateHeadDurableCommit();
    }

    public void DrainAll()
    {
        while (_inFlight.Count > 0)
            Complete(_inFlight.Dequeue());

        _headDurableCommit = null;
    }

    private void Complete(TBatch batch)
    {
        try
        {
            _failure ??= batch.Complete(_failure?.SourceException);
        }
        catch (Exception e)
        {
            // Complete is not supposed to throw, but the pump is called from finally blocks and from the
            // middle of an indexing batch - losing the queue here would strand every batch behind this one
            _failure ??= ExceptionDispatchInfo.Capture(e);
        }
    }

    public void ThrowOnFailure() => _failure?.Throw();
}
