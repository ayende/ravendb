using System.Collections.Generic;
using Voron;
using Voron.Data.Graphs;

namespace Corax.Querying;

// The Corax-facing view of the per-transaction index cache (implemented by Raven.Server's IndexTransactionCache).
// Lets an IndexSearcher pick up everything it needs from a single object instead of unpacking each field at the
// call site. The concrete type carries more (collections, directory files) that Corax has no business seeing.
public interface ICoraxTransactionCache
{
    Dictionary<Slice, HnswIndexCache> VectorNodeCaches { get; }

    HashSet<string> FieldsWithMultipleTerms { get; }
}
