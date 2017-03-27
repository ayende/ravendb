﻿using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndex : MapIndexBase<AutoMapIndexDefinition>
    {
        private AutoMapIndex(AutoMapIndexDefinition definition)
            : base(IndexType.AutoMap, definition)
        {
        }

        public static AutoMapIndex CreateNew(AutoMapIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapIndex(definition);
            instance.Initialize(documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static AutoMapIndex Open(StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = AutoMapIndexDefinition.Load(environment);
            var instance = new AutoMapIndex(definition);
            instance.Initialize(environment, documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            return new AutoIndexDocsEnumerator(documents, stats);
        }

        public override void Update(IndexDefinitionBase definition, IndexingConfiguration configuration)
        {
            throw new NotSupportedException($"{Type} index does not support updating it's definition and configuration.");
        }
    }
}