using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Sparrow.Server.Platform;

namespace Voron.Impl.Journal;

public class SharedJournalState()
{
    private readonly ConcurrentQueue<WriteAheadJournal.JournalStateRecord> _mergedCommitsQueue = new();
    private readonly List<WriteAheadJournal.JournalStateRecord> _mergedJournalJournalRecordsBuffer = new List<WriteAheadJournal.JournalStateRecord>();
    private readonly List<Pal.journal_entry> _mergedEntriesBuffer = new List<Pal.journal_entry>();
    public bool HasBranchCommits => _mergedCommitsQueue.IsEmpty is false;

    public void Enqueue(WriteAheadJournal.JournalStateRecord record) => _mergedCommitsQueue.Enqueue(record);

    public bool TryDequeue(out WriteAheadJournal.JournalStateRecord record) => _mergedCommitsQueue.TryDequeue(out record);

    public bool IsEmpty => _mergedCommitsQueue.IsEmpty;

    public Span<Pal.journal_entry> Entries => CollectionsMarshal.AsSpan(_mergedEntriesBuffer);
    public List<WriteAheadJournal.JournalStateRecord> JournalRecords => _mergedJournalJournalRecordsBuffer;
    public ConcurrentQueue<WriteAheadJournal.JournalStateRecord> MergedCommitsQueue => _mergedCommitsQueue;

    public void PrepareForCommit(WriteAheadJournal.JournalStateRecord state)
    {
        _mergedJournalJournalRecordsBuffer.Add(state);
        _mergedEntriesBuffer.Add(state.Entry);
    }

    public void PrepareForCommit(Pal.journal_entry entry)
    {
        _mergedEntriesBuffer.Add(entry);
    }

    public void Reset()
    {
        _mergedJournalJournalRecordsBuffer.Clear();
        _mergedEntriesBuffer.Clear();
    }

    public void SetException(Exception e)
    {
        // Only the records merged into the write that failed are casualties of it: part of their
        // data may have reached the device, so their environments cannot be trusted.
        foreach (var record in _mergedJournalJournalRecordsBuffer)
        {
            record.FailCatastrophically(e);
        }

        // Records still queued were never part of that write. We don't fail them catastrophically, but we
        // but we do abort the current transaction, this can then be retried.
        while (_mergedCommitsQueue.TryDequeue(out var rec))
        {
            // a harvested record's lease is retained by the write and returned there; one that was
            // never harvested still owns its own, and nothing else will hand it back
            var lease = rec.Lease;
            rec.Lease = null;
            lease?.Dispose();

            rec.FailLeavingEnvironmentUsable(e);
        }
    }

    public void SetCancel()
    {
        while (_mergedCommitsQueue.TryDequeue(out var rec))
        {
            rec.Cancel();
        }

        foreach (var record in _mergedJournalJournalRecordsBuffer)
        {
            record.Cancel();
        }
    }
}
