using System;

namespace Corax.Indexing;
// container ids are guaranteed to be aligned on 
// 4 bytes boundary, we're using this to store metadata
// about the data
[Flags]
public enum TermIdMask : long
{
    Single = 0,

    EnsureIsSingleMask = 0b11,

    SmallPostingList = 1,
    PostingList = 2,

    // The three values above cover three of the four low-2-bit patterns; 0b11 is never produced for a real
    // term. Callers repurpose it as a synthetic "this is not a real posting list" marker — the concrete meaning
    // (e.g. an empty source, or a universal/all source) is defined at the call site by the exact sentinel id.
    NotARealValue = 0b11,
}
