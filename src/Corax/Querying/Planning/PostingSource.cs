using Voron.Data.PostingLists;

namespace Corax.Querying.Planning;

/// <summary>Three-way native posting-list source attached to term op.
/// Mirrors the encoding used by <see cref="ITermsProvider.FillPostingListIds"/>:
/// the low 2 bits of a CompactTree value distinguish Single / SmallPostingList /
/// PostingList. Resolved up-front by <c>ResolvePostingSources</c>; consumed by
/// <c>FillBitmapFromPostingSource</c> / <c>AndWithPostingSource</c> /
/// <c>AndNotWithPostingSource</c> at execution time.</summary>
public struct PostingSource
{
    public PostingSourceKind Kind;

    /// <summary>Decoded entry id (Kind == Single) — already passed through
    /// EntryIdEncodings.GetContainerId.</summary>
    public long SingleEntryId;

    /// <summary>Container id for the small posting list (Kind == SmallPostingList) —
    /// pass to <c>Container.Get</c> on the LowLevelTransaction, then decode the
    /// FastPFor buffer.</summary>
    public long SmallPostingListId;

    /// <summary>Iterator over a large posting list (Kind == PostingList).</summary>
    public PostingList.Iterator LargeIterator;
}