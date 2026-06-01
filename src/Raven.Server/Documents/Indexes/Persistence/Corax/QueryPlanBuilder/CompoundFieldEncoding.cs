using Corax.Querying.Planning;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal struct CompoundFieldEncoding
{
    public PackedParam Packed;
    public Slice Analyzed;
    public int SourceSlot;
    public int Size;
}
