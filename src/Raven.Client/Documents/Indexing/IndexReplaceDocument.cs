using System;

namespace Raven.Client.Documents.Indexing
{
    public class IndexReplaceDocument
    {
        public string IndexToReplace { get; set; }

        public long? MinimumEtagBeforeReplace { get; set; }

        public DateTime? ReplaceTimeUtc { get; set; }
    }
}
