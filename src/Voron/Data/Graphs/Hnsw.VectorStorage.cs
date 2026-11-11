using System;
using System.Diagnostics;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Impl;
using Sparrow;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    internal static ContainerEntryId RegisterVector(LowLevelTransaction llt, ContainerId globalVectorsContainerId, int vectorBatchSizeInPages,
        int vectorSizeBytes, ref ContainerId lastUsedContainerId, ref byte vectorBatchIndex, ReadOnlySpan<byte> vector)
    {
        if (lastUsedContainerId.IsEmpty)
        {
            if (vectorBatchSizeInPages is 1)
            {
                var vectorId = Container.Allocate(llt, globalVectorsContainerId, vector.Length, out var singleVectorStorage);
                vector.CopyTo(singleVectorStorage);
                return vectorId;
            }

            var sizeInBytes = vectorBatchSizeInPages * Constants.Storage.PageSize - PageHeader.SizeOf;
            var batchId = Container.Allocate(llt, globalVectorsContainerId, sizeInBytes, out var vectorStorage);

            Debug.Assert(vectorStorage.Length / vectorSizeBytes <= byte.MaxValue, "vectorStorage.Length / vectorSizeBytes <= byte.MaxValue");
            Debug.Assert(((long)batchId & 0xFFF) == 0, "We allocate > 1 page, so we get the full page container id");
            lastUsedContainerId = new ContainerId((long)batchId);
            vectorBatchIndex = 1;
            vector.CopyTo(vectorStorage);
            return GetVectorId(new ContainerId((long)batchId), 0);
        }

        var span = Container.GetMutable(llt, new ContainerEntryId((long)lastUsedContainerId));
        var count = vectorBatchIndex++;
        Debug.Assert(count * vector.Length < span.Length, "count * vector.Length < span.Length");
        var offset = count * vector.Length;
        vector.CopyTo(span[offset..]);
        offset += vector.Length;
        var id = GetVectorId(lastUsedContainerId, count);
        if (offset + vector.Length > span.Length)
        {
            // no more room for the _next_ vector
            lastUsedContainerId = new ContainerId(0);
            vectorBatchIndex = 0;
        }

        return id;

        static ContainerEntryId GetVectorId(ContainerId containerId, int index)
        {
            Debug.Assert(((long)containerId & Constants.Graphs.VectorId.EnsureIsSingleMask) == 0, $"Container id {containerId}");
            //container id | index    | marker
            return new ContainerEntryId((long)containerId | (uint)(index << 1) | Constants.Graphs.VectorStorage.VectorContainerInternalIndexer);
        }
    }

    internal static UnmanagedSpan ReadVector(LowLevelTransaction llt, long vectorId, int vectorSizeBytes)
    {
        if ((vectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
        {
            Container.Get(llt, new ContainerEntryId(vectorId), out var item);
            Debug.Assert(vectorSizeBytes == item.Length, "vectorSizeBytes == item.Length");
            return new UnmanagedSpan(item.Address, item.Length);
        }

        var count = (byte)(vectorId >> 1);
        Container.Get(llt, new ContainerEntryId(vectorId & Node.VectorIdMask), out var container);
        var offset = count * vectorSizeBytes;
        Debug.Assert(offset >= 0 && offset + vectorSizeBytes <= container.Length, "offset >= 0 && offset + vectorSizeBytes <= container.Length");
        return new UnmanagedSpan(container.Address + offset, vectorSizeBytes);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal static long GetVectorContainerId(long vectorId) =>
        (vectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0 ? vectorId : vectorId & Node.VectorIdMask;

    internal static UnmanagedSpan SliceVector(UnmanagedSpan container, long vectorId, int vectorSizeBytes)
    {
        if ((vectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
            return container;
        var offset = (byte)(vectorId >> 1) * vectorSizeBytes;
        return new UnmanagedSpan(container.Address + offset, vectorSizeBytes);
    }
}
