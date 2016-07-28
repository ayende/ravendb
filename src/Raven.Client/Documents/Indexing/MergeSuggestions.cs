using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Indexing
{
    public class MergeSuggestions
    {
        public IndexDefinition MergedIndex = new IndexDefinition();  //propose for new index with all it's properties

        // start MergedIndex != null
        public List<string> CanMerge = new List<string>();  // index names

        public string Collection = String.Empty; // the collection that is being merged
        // end MergedIndex != null

        // start MergedIndex == null
        public List<string> CanDelete = new List<string>();  // index names

        public string SurpassingIndex = string.Empty;
        // end MergedIndex == null
    }
}
