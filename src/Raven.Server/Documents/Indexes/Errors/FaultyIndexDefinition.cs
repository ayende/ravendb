using System;
using Raven.Client.Documents.Indexing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyIndexDefinition : IndexDefinitionBase
    {
        public FaultyIndexDefinition(string name, string[] collections, IndexLockMode lockMode, IndexField[] mapFields)
            : base(name, collections, lockMode, mapFields)
        {
        }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        protected override IndexDefinition CreateIndexDefinition()
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        public override bool Equals(IndexDefinition indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }
    }
}