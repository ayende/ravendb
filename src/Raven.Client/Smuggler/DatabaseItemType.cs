using System;

namespace Raven.Client.Smuggler
{
    [Flags]
    public enum DatabaseItemType
    {
        None = 0,

        Documents = 1 << 0,
        RevisionDocuments = 1 << 1,
        Indexes = 1 << 2,
        Transformers = 1 << 3,
        Identities = 1 << 4,

        RemoveAnalyzers = 1 << 7,
    }
}