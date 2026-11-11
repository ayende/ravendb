using System;
using System.Collections.Generic;
using System.Threading;
using Sparrow.Server;

namespace Voron.Data.Graphs;

public interface IVectorIndexer : IDisposable
{
    bool IsCommited { get; }

    int AmountOfModifiedVectorsInTransaction { get; }

    /// <summary>
    /// Node ids whose persisted record changed in the last commit, used to update the in memory node caches.
    /// </summary>
    HashSet<long> DirtyNodeIds { get; }

    int MaxConcurrentBatches { get; set; }

    ByteString Register(long entryId, ReadOnlySpan<byte> vector);

    void Remove(long entryId, ReadOnlySpan<byte> vectorHash);

    void Commit(CancellationToken token);
}
