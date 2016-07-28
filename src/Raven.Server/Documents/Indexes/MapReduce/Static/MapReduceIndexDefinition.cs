﻿using System.Collections.Generic;
using Raven.Client.Documents.Indexing;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : StaticMapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, string[] collections, string[] outputFields, string[] groupByFields)
            : base(definition, collections, outputFields)
        {
            GroupByFields = new HashSet<string>(groupByFields);
        }

        public HashSet<string> GroupByFields { get; private set; }
    }
}