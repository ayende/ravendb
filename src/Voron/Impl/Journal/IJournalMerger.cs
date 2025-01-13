namespace Voron.Impl.Journal;

public interface IJournalMerger
{
    bool IsIdle { get; }

    void JournalMergeSubmitted();
}
