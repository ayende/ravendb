namespace Corax.Querying.Planning;

public enum PostingSourceKind : byte
{
    /// <summary>The term does not exist in the index (or the field has no compact tree).
    /// Dispatcher primitives no-op on Empty for Or-shaped ops, and clear the bitmap
    /// for And-shaped ops.</summary>
    Empty,
    Single,
    SmallPostingList,
    PostingList,
    /// <summary>Sentinel used for AllIn's null-term slot when HasNullTerm=false.
    /// AND-shaped ops treat this as a universal pass-through (no-op), preserving the
    /// bitmap unchanged. This allows the AND range loop to always cover
    /// <c>inTermCount</c> slots (advancing the cursor past the null slot) without
    /// clearing the bitmap when no null term was requested.</summary>
    All,
}