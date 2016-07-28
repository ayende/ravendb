﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Json;
using Voron;
using Sparrow;

namespace Raven.Server.ServerWide.Context
{
    public class TransactionOperationContext : TransactionOperationContext<RavenTransaction>
    {
        private readonly StorageEnvironment _environment;

        public TransactionOperationContext(UnmanagedBuffersPool pool, StorageEnvironment environment)
            : base(pool)
        {
            _environment = environment;
        }

        protected override RavenTransaction CreateReadTransaction(ByteStringContext context)
        {
            return new RavenTransaction(_environment.ReadTransaction(context));
        }

        protected override RavenTransaction CreateWriteTransaction(ByteStringContext context)
        {
            return new RavenTransaction(_environment.WriteTransaction(context));
        }
    }

    public abstract class TransactionOperationContext<TTransaction> : JsonOperationContext
        where TTransaction : RavenTransaction
    {
        private List<GCHandle> _pinnedObjects;

        public readonly ByteStringContext Allocator;
        public TTransaction Transaction;

        protected TransactionOperationContext(UnmanagedBuffersPool pool)
            : base(pool)
        {
            Allocator = new ByteStringContext();
        }

        public RavenTransaction OpenReadTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
                throw new InvalidOperationException("Transaction is already opened");

            Transaction = CreateReadTransaction(Allocator);

            return Transaction;
        }

        protected abstract TTransaction CreateReadTransaction(ByteStringContext allocator);

        protected abstract TTransaction CreateWriteTransaction(ByteStringContext allocator);

        public virtual RavenTransaction OpenWriteTransaction()
        {
            if (Transaction != null && Transaction.Disposed == false)
            {
                throw new InvalidOperationException("Transaction is already opened");
            }

            Transaction = CreateWriteTransaction(Allocator);

            return Transaction;
        }

        public IntPtr PinObjectAndGetAddress(object obj)
        {
            var handle = GCHandle.Alloc(obj, GCHandleType.Pinned);

             if (_pinnedObjects == null)
                _pinnedObjects = new List<GCHandle>();

             _pinnedObjects.Add(handle);

            return handle.AddrOfPinnedObject();
        }

        public override void Dispose()
        {
            base.Dispose();

            Allocator?.Dispose();

            if (_pinnedObjects != null)
            {
                foreach (var pinnedObject in _pinnedObjects)
                {
                    pinnedObject.Free();
                }
            }
        }

        public override void Reset()
        {
            base.Reset();

            Transaction?.Dispose();            
            Transaction = null;
        }        
    }
}