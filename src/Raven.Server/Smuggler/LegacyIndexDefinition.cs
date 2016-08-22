using System;
using System.Collections.Generic;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Server.Smuggler
{
    /// <summary>
    /// A definition of a RavenIndex
    /// </summary>
    internal class LegacyIndexDefinition
    {
        /// <summary>
        /// Index identifier (internal).
        /// </summary>
        public int IndexId { get; set; }

        /// <summary>
        /// This is the means by which the outside world refers to this index definition
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Index lock mode:
        /// <para>- Unlock - all index definition changes acceptable</para>
        /// <para>- LockedIgnore - all index definition changes will be ignored, only log entry will be created</para>
        /// <para>- LockedError - all index definition changes will raise exception</para>
        /// <para>- SideBySide - all index definition changes will raise exception except when updated by a side by side index</para>
        /// </summary>
        public IndexLockMode LockMode { get; set; }

        /// <summary>
        /// Index version, used in index replication in order to identify if two indexes are indeed the same.
        /// </summary>
        public int? IndexVersion { get; set; }

        /// <summary>
        /// Index map function, if there is only one
        /// </summary>
        /// <remarks>
        /// This property only exists for backward compatibility purposes
        /// </remarks>
        public string Map
        {
            get { return Maps.FirstOrDefault(); }
            set
            {
                if (Maps.Count != 0)
                {
                    Maps.Remove(Maps.First());
                }
                Maps.Add(value);
            }
        }

        /// <summary>
        /// All the map functions for this index
        /// </summary>
        public HashSet<string> Maps
        {
            get { return _maps ?? (_maps = new HashSet<string>()); }
            set { _maps = value; }
        }

        /// <summary>
        /// Index reduce function
        /// </summary>
        public string Reduce { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance is map reduce index definition
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
        /// </value>
        public bool IsMapReduce => string.IsNullOrEmpty(Reduce) == false;

        /// <summary>
        /// Internal use only.
        /// </summary>
        public bool IsCompiled { get; set; }

        /// <summary>
        /// Index field storage settings.
        /// </summary>
        public Dictionary<string, FieldStorage> Stores
        {
            get { return _stores ?? (_stores = new Dictionary<string, FieldStorage>()); }
            set { _stores = value; }
        }

        /// <summary>
        /// Index field indexing settings.
        /// </summary>
        public Dictionary<string, FieldIndexing> Indexes
        {
            get { return _indexes ?? (_indexes = new Dictionary<string, FieldIndexing>()); }
            set { _indexes = value; }
        }

        /// <summary>
        /// Index field sorting settings.
        /// </summary>
        public Dictionary<string, LegacySortOptions> SortOptions
        {
            get { return _sortOptions ?? (_sortOptions = new Dictionary<string, LegacySortOptions>()); }
            set { _sortOptions = value; }
        }

        /// <summary>
        /// Index field analyzer settings.
        /// </summary>
        public Dictionary<string, string> Analyzers
        {
            get { return _analyzers ?? (_analyzers = new Dictionary<string, string>()); }
            set { _analyzers = value; }
        }

        /// <summary>
        /// List of queryable fields in index.
        /// </summary>
        public List<string> Fields
        {
            get { return _fields ?? (_fields = new List<string>()); }
            set { _fields = value; }
        }

        /// <summary>
        /// Index field suggestion settings.
        /// </summary>
        [Obsolete("Use SuggestionsOptions")]
        public Dictionary<string, SuggestionOptions> Suggestions
        {
            get
            {
                if (SuggestionsOptions == null || SuggestionsOptions.Count == 0)
                    return null;

                return SuggestionsOptions.ToDictionary(x => x, x => new SuggestionOptions());
            }
            set
            {
                if (value == null)
                    return;
                SuggestionsOptions = value.Keys.ToHashSet();
            }
        }

        public HashSet<string> SuggestionsOptions
        {
            get { return _suggestionsOptions ?? (_suggestionsOptions = new HashSet<string>()); }
            set { _suggestionsOptions = value; }
        }

        /// <summary>
        /// Index field term vector settings.
        /// </summary>
        public Dictionary<string, FieldTermVector> TermVectors
        {
            get { return _termVectors ?? (_termVectors = new Dictionary<string, FieldTermVector>()); }
            set { _termVectors = value; }
        }

        /// <summary>
        /// Index field spatial settings.
        /// </summary>
        public Dictionary<string, SpatialOptions> SpatialIndexes
        {
            get { return _spatialIndexes ?? (_spatialIndexes = new Dictionary<string, SpatialOptions>()); }
            set { _spatialIndexes = value; }
        }

        /// <summary>
        /// Internal map of field names to expressions generating them
        /// Only relevant for auto indexes and only used internally
        /// </summary>
        public Dictionary<string, string> InternalFieldsMapping
        {
            get { return _internalFieldsMapping ?? (_internalFieldsMapping = new Dictionary<string, string>()); }
            set { _internalFieldsMapping = value; }
        }

        /// <summary>
        /// Index specific setting that limits the number of map outputs that an index is allowed to create for a one source document. If a map operation applied to
        /// the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document 
        /// will be skipped and the appropriate error message will be added to the indexing errors.
        /// <para>Default value: null means that the global value from Raven configuration will be taken to detect if number of outputs was exceeded.</para>
        /// </summary>
        public int? MaxIndexOutputsPerDocument { get; set; }

        private static int DictionaryHashCode<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> x)
        {
            int result = 0;
            foreach (var kvp in x)
            {
                result = (result * 397) ^ kvp.Key.GetHashCode();
                result = (result * 397) ^ (!Equals(kvp.Value, default(TValue)) ? kvp.Value.GetHashCode() : 0);
            }
            return result;
        }

        private static int SetHashCode<TKey>(IEnumerable<TKey> x)
        {
            int result = 0;
            foreach (var kvp in x)
            {
                result = (result * 397) ^ kvp.GetHashCode();
            }
            return result;
        }

        [JsonIgnore]
        private byte[] _cachedHashCodeAsBytes;
        [JsonIgnore]
        private HashSet<string> _maps;
        [JsonIgnore]
        private Dictionary<string, FieldStorage> _stores;
        [JsonIgnore]
        private Dictionary<string, FieldIndexing> _indexes;
        [JsonIgnore]
        private Dictionary<string, LegacySortOptions> _sortOptions;
        [JsonIgnore]
        private Dictionary<string, string> _analyzers;
        [JsonIgnore]
        private List<string> _fields;
        [JsonIgnore]
        private Dictionary<string, FieldTermVector> _termVectors;
        [JsonIgnore]
        private Dictionary<string, SpatialOptions> _spatialIndexes;
        [JsonIgnore]
        private Dictionary<string, string> _internalFieldsMapping;
        [JsonIgnore]
        private HashSet<string> _suggestionsOptions;

        /// <summary>
        /// Provide a cached version of the index hash code, which is used when generating
        /// the index etag. 
        /// It isn't really useful for anything else, in particular, we cache that because
        /// we want to avoid calculating the cost of doing this over and over again on each 
        /// query.
        /// </summary>
        public byte[] GetIndexHash()
        {
            if (_cachedHashCodeAsBytes != null)
                return _cachedHashCodeAsBytes;

            _cachedHashCodeAsBytes = BitConverter.GetBytes(GetHashCode());
            return _cachedHashCodeAsBytes;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                int result = Maps.Where(x => x != null).Aggregate(0, (acc, val) => acc * 397 ^ val.GetHashCode());
                result = (result * 397) ^ Maps.Count;
                result = (result * 397) ^ (Reduce?.GetHashCode() ?? 0);
                result = (result * 397) ^ DictionaryHashCode(Stores);
                result = (result * 397) ^ DictionaryHashCode(Indexes);
                result = (result * 397) ^ DictionaryHashCode(Analyzers);
                result = (result * 397) ^ DictionaryHashCode(SortOptions);
                result = (result * 397) ^ SetHashCode(SuggestionsOptions);
                result = (result * 397) ^ DictionaryHashCode(TermVectors);
                result = (result * 397) ^ DictionaryHashCode(SpatialIndexes);
                return result;
            }
        }

        public string Type
        {
            get
            {
                var name = Name ?? string.Empty;
                if (name.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
                    return "Auto";
                if (IsCompiled)
                    return "Compiled";
                if (IsMapReduce)
                    return "MapReduce";
                return "Map";
            }
        }

        /// <summary>
        /// Prevent index from being kept in memory. Default: false
        /// </summary>
        public bool DisableInMemoryIndexing { get; set; }

        /// <summary>
        /// Whatever this is a temporary test only index
        /// </summary>
        public bool IsTestIndex { get; set; }

        /// <summary>
        /// Whatever this is a side by side index
        /// </summary>
        public bool IsSideBySideIndex { get; set; }

        public override string ToString()
        {
            return Name ?? Map;
        }

        public enum LegacySortOptions
        {
            None = 0,
            String = 3,
            Int = 4,
            Float = 5,
            Long = 6,
            Double = 7,
            Short = 8,
            Custom = 9,
            Byte = 10,
            StringVal = 11
        }
    }
}