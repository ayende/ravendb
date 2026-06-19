using Corax.Indexing;
using Corax.Utils;
using Voron.Data.PostingLists;

namespace Corax.Querying.Planning;

/// <summary>Three-way native posting-list source attached to term op.
/// Mirrors the encoding used by <see cref="ITermsProvider.FillPostingListIds"/>:
/// the low 2 bits of a CompactTree value distinguish Single / SmallPostingList /
/// PostingList. Resolved up-front by <c>ResolveTermSources</c>; consumed by
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

    /// <summary>Decode a raw posting-list ID (with TermIdMask bits) into a
    /// <see cref="PostingSource"/>. Returns Empty when the term doesn't exist (-1).
    /// For PostingList kind, opens a fresh iterator on the underlying set.</summary>
    public static PostingSource Decode(long postingListId, IndexSearcher indexSearcher)
    {
        if (postingListId == -1)
            return default; // Kind == Empty

        var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case TermIdMask.Single:
                return new PostingSource
                {
                    Kind = PostingSourceKind.Single,
                    SingleEntryId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case TermIdMask.SmallPostingList:
                return new PostingSource
                {
                    Kind = PostingSourceKind.SmallPostingList,
                    SmallPostingListId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case TermIdMask.PostingList:
            {
                var postingList = indexSearcher.GetPostingList(postingListId);
                return new PostingSource
                {
                    Kind = PostingSourceKind.PostingList,
                    LargeIterator = postingList.Iterate(),
                };
            }

            default:
                return default;
        }
    }
}