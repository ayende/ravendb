﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using System.Linq;
using Raven.Server.Documents.Includes;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private static readonly ConcurrentDictionary<Type, PropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, PropertyAccessor>();

        private readonly IndexingFunc _transformer;
        private readonly DocumentsOperationContext _context;

        public TransformationScope(IndexingFunc transformer, BlittableJsonReaderObject transformerParameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, DocumentsOperationContext context)
        {
            _transformer = transformer;
            _context = context;
            CurrentTransformationScope.Current = new CurrentTransformationScope(transformerParameters, include, documentsStorage, context);
        }

        public void Dispose()
        {
            CurrentTransformationScope.Current = null;
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            var docsEnumerator = new StaticIndexDocsEnumerator(documents, _transformer, null, StaticIndexDocsEnumerator.EnumerationType.Transformer);

            IEnumerable transformedResults;
            while (docsEnumerator.MoveNext(out transformedResults))
            {
                using (docsEnumerator.Current.Data)
                {
                    var values = new DynamicJsonArray();
                    var result = new DynamicJsonValue
                    {
                        ["$values"] = values
                    };

                    PropertyAccessor accessor = null;
                    foreach (var transformedResult in transformedResults)
                    {
                        if (accessor == null)
                            accessor = GetPropertyAccessor(transformedResult);

                        var value = new DynamicJsonValue();
                        foreach (var property in accessor.Properties)
                        {
                            var propertyValue = property.Value(transformedResult);
                            var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                            if (propertyValueAsEnumerable != null && AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(propertyValue))
                            {
                                value[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(ConvertType));
                                continue;
                            }

                            value[property.Key] = ConvertType(propertyValue);
                        }

                        values.Add(value);
                    }

                    var document = new Document
                    {
                        //Key = docsEnumerator.Current.Key,
                        Data = _context.ReadObject(result, docsEnumerator.Current.Key ?? string.Empty),
                        Etag = docsEnumerator.Current.Etag,
                        StorageId = docsEnumerator.Current.StorageId
                    };

                    yield return document;
                }
            }
        }

        private static object ConvertType(object value)
        {
            if (value == null)
                return null;

            var dynamicDocument = value as DynamicBlittableJson;
            if (dynamicDocument != null)
                return dynamicDocument.BlittableJson;

            var transformerParameter = value as TransformerParameter;
            if (transformerParameter != null)
                return transformerParameter.OriginalValue;

            if (value is string)
                return value;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value;

            if (value is DateTime)
                return value;

            var inner = new DynamicJsonValue();
            var accessor = GetPropertyAccessor(value);

            foreach (var property in accessor.Properties)
            {
                var propertyValue = property.Value(value);
                var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                if (propertyValueAsEnumerable != null && AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(propertyValue))
                {
                    inner[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(ConvertType));
                    continue;
                }

                inner[property.Key] = ConvertType(propertyValue);
            }

            return inner;
        }

        private static PropertyAccessor GetPropertyAccessor(object value)
        {
            var type = value.GetType();
            return PropertyAccessorCache.GetOrAdd(type, PropertyAccessor.Create);
        }
    }
}