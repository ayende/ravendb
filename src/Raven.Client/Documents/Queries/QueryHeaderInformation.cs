using System;

namespace Raven.Client.Documents.Queries
{
    public class QueryHeaderInformation
    {
        public string Index { get; set; }
        public bool IsStale { get; set; }
        public DateTime IndexTimestamp { get; set; }
        public int TotalResults { get; set; }
        public long? ResultEtag { get; set; }
        public long? IndexEtag { get; set; }
    }
}
