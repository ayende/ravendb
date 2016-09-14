﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Documents.Queries.Sorting;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMapping
    {
        private static readonly CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

        public string ForCollection { get; private set; }

        public DynamicSortInfo[] SortDescriptors { get; private set; } = new DynamicSortInfo[0];

        public DynamicQueryMappingItem[] MapFields { get; private set; } = new DynamicQueryMappingItem[0];

        public string[] GroupByFields { get; private set; } = new string[0];

        public string[] HighlightedFields { get; private set; }

        public bool IsMapReduce { get; private set; }

        public IndexDefinitionBase CreateAutoIndexDefinition()
        {
            if (IsMapReduce == false)
            {
                return new AutoMapIndexDefinition(ForCollection, MapFields.Select(field =>
                    new IndexField
                    {
                        Name = field.Name,
                        Storage = FieldStorage.No,
                        SortOption = SortDescriptors.FirstOrDefault(x => field.Name.Equals(x.Name))?.FieldType,
                        Highlighted = HighlightedFields.Any(x => field.Name.Equals(x))
                    }).ToArray());
            }

            if (MapFields.Length == 0)
                throw new InvalidOperationException("Invalid dynamic map-reduce query mapping. There is no aggregation specified.");

            if (GroupByFields.Length == 0)
                throw new InvalidOperationException("Invalid dynamic map-reduce query mapping. There is no group by field specified.");

            return new AutoMapReduceIndexDefinition(new[] { ForCollection }, MapFields.Select(field =>
                    new IndexField
                    {
                        Name = field.Name,
                        Storage = FieldStorage.Yes,
                        MapReduceOperation = field.MapReduceOperation,
                        SortOption = SortDescriptors.FirstOrDefault(x => field.Name.Equals(x.Name))?.FieldType,
                    }).ToArray(),
                    GroupByFields.Select(field =>
                    new IndexField
                    {
                        Name = field,
                        Storage = FieldStorage.Yes,
                        SortOption = SortDescriptors.FirstOrDefault(x => field.Equals(x.Name))?.FieldType,
                    }).ToArray());
        }

        public void ExtendMappingBasedOn(IndexDefinitionBase definitionOfExistingIndex)
        {
            Debug.Assert(definitionOfExistingIndex is AutoMapIndexDefinition || definitionOfExistingIndex is AutoMapReduceIndexDefinition, "We can only support auto-indexes.");

            var extendedMapFields = new List<DynamicQueryMappingItem>(MapFields);
            var extendedSortDescriptors = new List<DynamicSortInfo>(SortDescriptors);

            foreach (var field in definitionOfExistingIndex.MapFields.Values)
            {
                if (extendedMapFields.Any(x => x.Name.Equals(field.Name, StringComparison.OrdinalIgnoreCase)) == false)
                {
                    extendedMapFields.Add(new DynamicQueryMappingItem(field.Name, field.MapReduceOperation));
                }

                if (extendedSortDescriptors.Any(x => x.Name.Equals(field.Name, StringComparison.OrdinalIgnoreCase)) == false && field.SortOption != null)
                {
                    extendedSortDescriptors.Add(new DynamicSortInfo
                    {
                        Name = field.Name,
                        FieldType = field.SortOption.Value
                    });
                }
            }

            //TODO arek - HighlightedFields

            MapFields = extendedMapFields.ToArray();
            SortDescriptors = extendedSortDescriptors.ToArray();
        }

        public static DynamicQueryMapping Create(string entityName, IndexQueryServerSide query)
        {
            var result = new DynamicQueryMapping
            {
                ForCollection = entityName,
            };

            IEnumerable<DynamicQueryMappingItem> dynamicMapFields;
            string[] numericFields;

            if (query.DynamicMapReduceFields == null)
            {
                // auto map query

                var fields = SimpleQueryParser.GetFieldsForDynamicQuery(query); // TODO arek - not sure if we really need a Tuple<string, string> here

                if (query.SortedFields != null)
                {
                    foreach (var sortedField in query.SortedFields)
                    {
                        var field = sortedField.Field;

                        if (field == Constants.Indexing.Fields.IndexFieldScoreName)
                            continue;

                        if (field.StartsWith(Constants.Indexing.Fields.RandomFieldName) ||
                            field.StartsWith(Constants.Indexing.Fields.CustomSortFieldName))
                            continue;

                        if (InvariantCompare.IsPrefix(field, Constants.Indexing.Fields.AlphaNumericFieldName, CompareOptions.None))
                        {
                            field = SortFieldHelper.ExtractName(field);
                        }

                        if (InvariantCompare.IsSuffix(field, Constants.Indexing.Fields.RangeFieldSuffix, CompareOptions.None))
                            field = field.Substring(0, field.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length);

                        fields.Add(Tuple.Create(SimpleQueryParser.TranslateField(field), field));
                    }
                }

                dynamicMapFields = fields.Select(x => new DynamicQueryMappingItem(x.Item1.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix) ? x.Item1.Substring(0, x.Item1.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length) : x.Item1, FieldMapReduceOperation.None));

                numericFields = fields.Where(x => x.Item1.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix)).Select(x => x.Item1).Distinct().ToArray();
            }
            else
            {
                // dynamic map-reduce query

                result.IsMapReduce = true;

                dynamicMapFields = query.DynamicMapReduceFields.Where(x => x.IsGroupBy == false).Select(x => new DynamicQueryMappingItem(x.Name, x.OperationType));

                result.GroupByFields = query.DynamicMapReduceFields.Where(x => x.IsGroupBy).Select(x => x.Name).ToArray();

                numericFields = null;
            }

            result.MapFields = dynamicMapFields
                .Where(x => x.Name != Constants.Indexing.Fields.DocumentIdFieldName)
                .OrderByDescending(x => x.Name.Length)
                .ToArray();

            result.SortDescriptors = GetSortInfo(query.SortedFields, numericFields);

            result.HighlightedFields = query.HighlightedFields.EmptyIfNull().Select(x => x.Field).ToArray();

            return result;
        }

        private static DynamicSortInfo[] GetSortInfo(SortedField[] sortedFields, string[] numericFields)
        {
            var sortInfo = new List<DynamicSortInfo>();

            if (numericFields != null && numericFields.Length > 0)
            {
                foreach (var key in numericFields)
                {
                    if (key == Constants.Indexing.Fields.IndexFieldScoreName)
                        continue;

                    if (InvariantCompare.IsPrefix(key, Constants.Indexing.Fields.RandomFieldName, CompareOptions.None))
                        continue;

                    sortInfo.Add(new DynamicSortInfo
                    {
                        Name = key.Substring(0, key.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length),
                        FieldType = SortOptions.NumericDefault
                    });
                }
            }

            if (sortedFields != null && sortedFields.Length > 0)
            {
                foreach (var sortOptions in sortedFields)
                {
                    var key = sortOptions.Field;

                    if (key == Constants.Indexing.Fields.IndexFieldScoreName)
                        continue;

                    if (InvariantCompare.IsPrefix(key, Constants.Indexing.Fields.RandomFieldName, CompareOptions.None))
                        continue;

                    if (InvariantCompare.IsSuffix(key, Constants.Indexing.Fields.RangeFieldSuffix, CompareOptions.None))
                    {
                        sortInfo.Add(new DynamicSortInfo
                        {
                            Name = key.Substring(0, key.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length),
                            FieldType = SortOptions.NumericDefault
                        });
                    }
                    else
                    {
                        sortInfo.Add(new DynamicSortInfo
                        {
                            Name = key,
                            FieldType = SortOptions.String
                        });
                    }
                }
            }

            return sortInfo.ToArray();
        }
    }
}