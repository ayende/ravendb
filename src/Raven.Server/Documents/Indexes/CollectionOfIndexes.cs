using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Jint.Parser.Ast;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Sparrow.Collections;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfIndexes : IEnumerable<CollectionOfIndexes.IndexPair>
    {
        public class IndexPair
        {
            private Index _current;
            private Index _sideBySide;
            public Index Current{get { return _current; } set { _current = value; }}
            public Index SideBySide { get { return _sideBySide; } set { _sideBySide = value; } }
        }
        private readonly ConcurrentDictionary<long, Index> _indexesById = new ConcurrentDictionary<long, Index>();
        private readonly ConcurrentDictionary<string, IndexPair> _indexesByName = new ConcurrentDictionary<string, IndexPair>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string,IndexPair>> _indexesByCollection = new ConcurrentDictionary<string, ConcurrentDictionary<string, IndexPair>>();

        public void AddNewIndex(Index index)
        {
            bool indexExists = _indexesByName.ContainsKey(index.Name);
            Debug.Assert(indexExists == false, $"Index {index.Name} already exists. Should not happen");
            
            var indexPair = new IndexPair {
                Current = index
            };

            _indexesByName[index.Name] = indexPair;

            _indexesById.TryAdd(index.Etag, index);


            foreach (var collection in index.Definition.Collections)
            {
                var indexesOnCollection = _indexesByCollection.GetOrAdd(collection, s => new ConcurrentDictionary<string, IndexPair>());

                indexExists = indexesOnCollection.ContainsKey(index.Name);
                Debug.Assert(indexExists == false);
                indexesOnCollection.TryAdd(index.Name,indexPair);
            }
        }

        public void SetSideBySideIndex(Index index)
        {
            IndexPair indexPair;
            bool indexExists = _indexesByName.TryGetValue(index.Name, out indexPair);
            Debug.Assert(indexExists, $"Index {index.Name} does not exists. Should not happen");
            
            indexPair.SideBySide = index;
            _indexesById.TryAdd(index.Etag, index);
        }

        public void RenameIndex(Index index, string oldName, string newName)
        {
            // todo: not sure how to implement that yet
            throw new NotImplementedException();
            //_indexesByName.AddOrUpdate(newName, index, (key, oldValue) => index);
            //Index _;
            //_indexesByName.TryRemove(oldName, out _);
        }

        public bool TryGetByName(string name, out IndexPair indexpair)
        {
            return _indexesByName.TryGetValue(name, out indexpair);
        }

        public IEnumerable<IndexPair> GetForCollection(string collection)
        {
            ConcurrentDictionary<string,IndexPair> indexes;

            if (_indexesByCollection.TryGetValue(collection, out indexes) == false)
                return Enumerable.Empty<IndexPair>();

            return indexes.Values;
        }
        
        public IEnumerator<IndexPair> GetEnumerator()
        {
            return _indexesByName.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _indexesByName.Count;

        public bool TryGetById(long id, out Index index)
        {
            return _indexesById.TryGetValue(id, out index);
        }

        public bool TryRemoveById(long etag, out Index index)
        {
            var result = _indexesById.TryRemove(etag, out index);
            if (result == false)
                return false;
            
            if (_indexesByName.TryGetValue(index.Name, out IndexPair indexPair))
            {
                if (indexPair.SideBySide == null && indexPair.Current?.Etag == etag)
                {
                    Debug.Assert(indexPair.Current.Etag == index.Etag);
                    _indexesByName.TryRemove(index.Name, out indexPair);
                    foreach (var collection in indexPair.Current.Collections)
                    {
                        ConcurrentDictionary<string, IndexPair> indexes;
                        if (_indexesByCollection.TryGetValue(collection, out indexes)==false)
                            continue;

                        indexes.TryRemove(index.Name, out IndexPair pair);
                    }
                }
                else
                {
                    if (indexPair.Current.Etag == index.Etag)
                    {
                        // todo: this code path may be redundant, or maybe illegal?
                        indexPair.Current= indexPair.SideBySide;
                        indexPair.Current.IsSideBySide = false;
                    }
                    else
                    {
                        Debug.Assert(indexPair.SideBySide?.Etag == index.Etag);
                        indexPair.SideBySide = null;
                        
                    }
                }
            }

            return true;
        }

        public bool TryRemoveByName(string id, out IndexPair pair )
        {
            if (_indexesByName.TryRemove(id, out pair))
            {
                Index index;
                if (pair.SideBySide != null)
                    TryRemoveById(pair.SideBySide.Etag, index: out index);
                TryRemoveById(pair.Current.Etag, out index);
                return true;
            }
            return false;
        }
    }
}