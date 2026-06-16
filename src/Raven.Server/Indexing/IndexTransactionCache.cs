using System;
using System.Collections.Generic;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Graphs;

namespace Raven.Server.Indexing
{
    public sealed class IndexTransactionCache
    {
        public sealed class CollectionEtags
        {
            public long LastIndexedEtag;
            public long LastProcessedTombstoneEtag;
            public long LastProcessedTimeSeriesDeletedRangeEtag;
            public Dictionary<string, ReferenceCollectionEtags> LastReferencedEtags;
            public ReferenceCollectionEtags LastReferencedEtagsForCompareExchange;
        }

        public sealed class ReferenceCollectionEtags
        {
            public long LastEtag;
            public long LastProcessedTombstoneEtag;
        }

        public sealed class DirectoryFiles
        {
            public Dictionary<string, Tree.ChunkDetails[]> ChunksByName = new Dictionary<string, Tree.ChunkDetails[]>(StringComparer.OrdinalIgnoreCase);

            public Dictionary<string, InlineFileLocation> InlinesByName = new Dictionary<string, InlineFileLocation>(StringComparer.OrdinalIgnoreCase);
        }
        
        public readonly record struct InlineFileLocation(long PageNumber, int DataOffsetInPage, int DataSize);

        public Dictionary<string, DirectoryFiles> DirectoriesByName = new Dictionary<string, DirectoryFiles>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, CollectionEtags> Collections = new Dictionary<string, CollectionEtags>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Per-field HNSW vector caches keyed by field name. Populated by Corax indexes only
        /// (null or empty for Lucene indexes and for Corax indexes without vector fields).
        /// Attached to a transaction via
        /// <see cref="Voron.Impl.LowLevelTransaction.ImmutableExternalState"/>, so a read tx
        /// observes the same cache instance that was current at the moment its transaction was
        /// created. Each cache instance is long-lived (one per index field) and survives across
        /// commits; the dictionary only changes when a vector field is added or removed.
        /// </summary>
        public Dictionary<Slice, HnswIndexCache> VectorNodeCaches;

        /// <summary>
        /// Names of fields that have at least one entry with multiple terms (i.e. are NOT strictly
        /// single-valued), as recorded at write time in the <c>MultipleTermsInField</c> tree. Populated by
        /// Corax indexes only. The set is consumed by the structural plan key so the per-field single/multi
        /// state segregates plan templates. It is <c>null</c> when the field count exceeds the cap (an index
        /// with many multi-valued dynamic fields) — callers then fall back to a live per-call tree lookup.
        /// </summary>
        public HashSet<string> FieldsWithMultipleTerms;
    }
}
