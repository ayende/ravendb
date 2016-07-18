using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Util;

namespace Raven.Server.Documents
{
    public class HugeDocuments
    {
        private readonly SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, int> _hugeDocs;

        public HugeDocuments(int maxWarnSize)
        {
            _hugeDocs = new SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, int>(maxWarnSize);
        }

        public void Add(string id, int size)
        {
            _hugeDocs.Set(new Tuple<string, DateTime>(id, DateTime.Now), size);
        }

        public int GetSize(Tuple<string, DateTime> key)
        {
            int size;
            _hugeDocs.TryGetValue(key, out size);
            return size;
        }

        public SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, int> GetHugeDocuments()
        {
            return _hugeDocs;
        } 
    }
}