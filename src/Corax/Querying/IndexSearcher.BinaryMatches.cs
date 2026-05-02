using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CombinedMatch And<TInner, TOuter>(in TInner innerSet, in TOuter outerSet, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        return CombinedMatch.And(innerSet, outerSet);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CombinedMatch Or<TInner, TOuter>(in TInner innerSet, in TOuter outerSet, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        return CombinedMatch.Or(innerSet, outerSet);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch AndNot<TInner, TOuter>(in TInner innerSet, in TOuter outerSet, in CancellationToken token = default)
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        return AndNotMatch.Create(AndNotMatch<TInner, TOuter>.Create(this, innerSet, outerSet, token));
    }
}
