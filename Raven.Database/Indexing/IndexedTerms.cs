﻿// -----------------------------------------------------------------------
//  <copyright file="CachedIndexedTerms.cs" company="Hibernating Rhinos LTD"> 
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Lucene.Net.Util;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
    using System.Collections;

    public static class IndexedTerms
    {
        public static void ReadEntriesForFieldsFromTermVectors(
            IndexSearcherHolder.IndexSearcherHoldingState state,
            HashSet<string> fieldsToRead,
            HashSet<int> docIds,
            Func<string,string, double> convert,
            Action<string, string, double, int> onTermFound)
        {
            var reader = state.IndexSearcher.IndexReader;

            foreach (var docId in docIds)
            {
                foreach (var field in fieldsToRead)
                {
                    var termFreqVector = reader.GetTermFreqVector(docId, field);
                    if (termFreqVector == null)
                        continue;

                    foreach (var term in termFreqVector.GetTerms())
                    {
                        if (LowPrecisionNumber(field, term))
                            continue;
                        onTermFound(field, term, convert(field, term), docId);
                    }
                }
            }
        }

        public static void ReadEntriesForFields(
                IndexSearcherHolder.IndexSearcherHoldingState state,
                HashSet<string> fieldsToRead,
                HashSet<int> docIds,
                Func<string, string, double> convert,
                Action<string, string, double, int> onTermFound)
        {
            state.Lock.EnterReadLock();
            try
            {
                EnsureFieldsAreInCache(state, fieldsToRead, docIds);

                foreach (var field in fieldsToRead)
                {
                    foreach (var docId in docIds)
                    {
                        foreach (var val in state.GetFromCache(field, docId))
                        {
                            onTermFound(val.Term.Field, val.Term.Text, convert(val.Term.Field, val.Term.Text), docId);
                        }
                    }
                }
            }
            finally
            {
                if (state.Lock.IsReadLockHeld)
                    state.Lock.ExitReadLock();
            }
        }

        public static void ReadEntriesForFieldsFromTermVectors(
            IndexSearcherHolder.IndexSearcherHoldingState state,
            HashSet<string> fieldsToRead,
            HashSet<int> docIds,
            Action<string,string, int> onTermFound)
        {
            var reader = state.IndexSearcher.IndexReader;

            foreach (var docId in docIds)
            {
                foreach (var field in fieldsToRead)
                {
                    var termFreqVector = reader.GetTermFreqVector(docId, field);
                    if (termFreqVector == null)
                        continue;

                    foreach (var term in termFreqVector.GetTerms())
                    {
                        if(LowPrecisionNumber(field, term))
                            continue;
                        onTermFound(field, term, docId);
                    }
                }
            }
        }

        public static void ReadEntriesForFields(
              IndexSearcherHolder.IndexSearcherHoldingState state,
              HashSet<string> fieldsToRead,
              HashSet<int> docIds,
              Action<string, string, int> onTermFound)
        {
            state.Lock.EnterReadLock();
            try
            {
                EnsureFieldsAreInCache(state, fieldsToRead, docIds);

                foreach (var field in fieldsToRead)
                {
                    foreach (var docId in docIds)
                    {
                        foreach (var val in state.GetFromCache(field, docId))
                        {
                            onTermFound(val.Term.Field, val.Term.Text, docId);
                        }
                    }
                }
            }
            finally
            {
                if (state.Lock.IsReadLockHeld)
                    state.Lock.ExitReadLock();
            }
        }

        //public static void PreFillCache(IndexSearcherHolder.IndexSearcherHoldingState state, string[] fieldsToRead,
        //    IndexReader reader)
        //{
        //    state.Lock.EnterWriteLock();
        //    try
        //    {
        //        if (fieldsToRead.All(state.IsInCache))
        //            return;
        //        FillCache(state, fieldsToRead, reader);
        //    }
        //    finally
        //    {
        //        state.Lock.ExitWriteLock();
        //    }
        //}

        private static void EnsureFieldsAreInCache(IndexSearcherHolder.IndexSearcherHoldingState state, HashSet<string> fieldsToRead, HashSet<int> docIds)
        {
            if (fieldsToRead.All(field => docIds.All(docId => state.IsInCache(field, docId))))
                return;

            state.Lock.ExitReadLock();
            state.Lock.EnterWriteLock();
            try
            {
                var segments = new HashSet<object>();
                foreach (var docId in fieldsToRead.SelectMany(field => docIds.Where(docId => state.IsInCache(field, docId) == false)))
                    segments.Add(state.GetCachedSegmentReaderFieldCacheKey(docId));

                foreach (var segment in segments)
                    FillCache(state, fieldsToRead, state.GetCachedSegmentReaderByFieldCacheKey(segment));
            }
            finally
            {
                state.Lock.ExitWriteLock();
            }
            state.Lock.EnterReadLock();
        }

        private static void FillCache(IndexSearcherHolder.IndexSearcherHoldingState state, IEnumerable<string> fieldsToRead, IndexSearcherHolder.IndexSearcherHoldingState.SegmentReaderWithMetaInformation reader)
        {
            var numDocs = reader.NumDocs();

            foreach (var field in fieldsToRead)
            {
                var items = new LinkedList<IndexSearcherHolder.IndexSearcherHoldingState.CacheVal>[numDocs];

                using (var termDocs = reader.TermDocs())
                {
                    using (var termEnum = reader.Terms(new Term(field)))
                    {
                        do
                        {
                            if (termEnum.Term == null || field != termEnum.Term.Field)
                                break;

                            Term term = termEnum.Term;
                            if (LowPrecisionNumber(term.Field, term.Text))
                                continue;

                            var totalDocCountIncludedDeletes = termEnum.DocFreq();
                            termDocs.Seek(termEnum.Term);

                            while (termDocs.Next() && totalDocCountIncludedDeletes > 0)
                            {
                                totalDocCountIncludedDeletes -= 1;
                                if (reader.IsDeleted(termDocs.Doc))
                                    continue;

                                if (items[termDocs.Doc] == null)
                                    items[termDocs.Doc] = new LinkedList<IndexSearcherHolder.IndexSearcherHoldingState.CacheVal>();

                                items[termDocs.Doc].AddLast(new IndexSearcherHolder.IndexSearcherHoldingState.CacheVal
                                {
                                    Term = termEnum.Term
                                });
                            }
                        } while (termEnum.Next());
                    }
                }

                state.SetInCache(field, reader.FieldCacheKey, items);
            }
        }

        private static bool LowPrecisionNumber(string field, string val)
        {
            if (field.EndsWith("_Range") == false)
                return false;

            if (string.IsNullOrEmpty(val))
                return false;

            return val[0] - NumericUtils.SHIFT_START_INT != 0 &&
                   val[0] - NumericUtils.SHIFT_START_LONG != 0;
        }

        public static RavenJObject[] ReadAllEntriesFromIndex(IndexReader reader)
        {
            if (reader.MaxDoc > 128 * 1024)
            {
                throw new InvalidOperationException("Refusing to extract all index entires from an index with " + reader.MaxDoc +
                                                    " entries, because of the probable time / memory costs associated with that." +
                                                    Environment.NewLine +
                                                    "Viewing Index Entries are a debug tool, and should not be used on indexes of this size. You might want to try Luke, instead.");
            }
            var results = new RavenJObject[reader.MaxDoc];
            using (var termDocs = reader.TermDocs())
            using (var termEnum = reader.Terms())
            {
                while (termEnum.Next())
                {
                    var term = termEnum.Term;
                    if (term == null)
                        break;

                    var text = term.Text;

                    termDocs.Seek(termEnum);
                    for (int i = 0; i < termEnum.DocFreq() && termDocs.Next(); i++)
                    {
                        RavenJObject result = results[termDocs.Doc];
                        if (result == null)
                            results[termDocs.Doc] = result = new RavenJObject();
                        var propertyName = term.Field;
                        if (propertyName.EndsWith("_ConvertToJson") ||
                            propertyName.EndsWith("_IsArray"))
                            continue;
                        if (result.ContainsKey(propertyName))
                        {
                            switch (result[propertyName].Type)
                            {
                                case JTokenType.Array:
                                    ((RavenJArray)result[propertyName]).Add(text);
                                    break;
                                case JTokenType.String:
                                    result[propertyName] = new RavenJArray
									{
										result[propertyName],
										text
									};
                                    break;
                                default:
                                    throw new ArgumentException("No idea how to handle " + result[propertyName].Type);
                            }
                        }
                        else
                        {
                            result[propertyName] = text;
                        }
                    }
                }
            }
            return results;
        }

    }
}