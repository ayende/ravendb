﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class NestedMapResultsSection : IDisposable
    {
        private static readonly int SizeOfResultHeader = sizeof(ResultHeader);
        
        private readonly TransactionOperationContext _indexContext;
        private readonly AllocatedMemoryData _inMemorySectionBuffer;
        private readonly Dictionary<long, BlittableJsonReaderObject> _mapResults = new Dictionary<long, BlittableJsonReaderObject>();

        private int _dataSize;

        public NestedMapResultsSection(byte* ptr, int size, TransactionOperationContext indexContext)
        {
            _indexContext = indexContext;

            // need to have a copy because pointer can become invalid after defragmentation of a related page

            _inMemorySectionBuffer = _indexContext.GetMemory(size);

            var inMemoryPtr = (byte*)_inMemorySectionBuffer.Address;

            Memory.Copy(inMemoryPtr, ptr, size);

            var readPtr = inMemoryPtr;

            while (readPtr - inMemoryPtr < size)
            {
                var resultPtr = (ResultHeader*)readPtr;

                var blittableJsonReaderObject = new BlittableJsonReaderObject(readPtr + SizeOfResultHeader, resultPtr->Size, _indexContext);

                _mapResults.Add(resultPtr->Id, blittableJsonReaderObject);

                _dataSize += resultPtr->Size;

                readPtr += resultPtr->Size + SizeOfResultHeader;
            }
        }

        public NestedMapResultsSection(TransactionOperationContext indexContext)
        {
            _indexContext = indexContext;
        }

        public int Size => _dataSize + _mapResults.Count*SizeOfResultHeader;

        public void Add(long id, BlittableJsonReaderObject result, bool isUpdate)
        {
            if (isUpdate)
                _dataSize -= _mapResults[id].Size;

            _mapResults[id] = result;

            _dataSize += result.Size;
        }

        public Dictionary<long, BlittableJsonReaderObject> GetResults()
        {
            return _mapResults;
        }

        public void Dispose()
        {
            foreach (var result in _mapResults)
            {
                result.Value.Dispose();
            }
        }

        public void Delete(long id)
        {
            _dataSize -= _mapResults[id].Size;

            _mapResults.Remove(id);
        }

        public void CopyTo(byte* ptr)
        {
            foreach (var result in _mapResults)
            {
                var header = (ResultHeader*)ptr;

                header->Id = result.Key;
                header->Size = (ushort) result.Value.Size;

                ptr += SizeOfResultHeader;

                result.Value.CopyTo(ptr);

                ptr += result.Value.Size;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct ResultHeader
        {
            [FieldOffset(0)]
            public long Id;

            [FieldOffset(8)]
            public ushort Size;
        }
    }
}