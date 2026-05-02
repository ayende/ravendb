using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SpatialMatch;

namespace Corax.Querying;

public partial class IndexSearcher
{
    private static class QueryBuilderHelper
    {
        public enum QueryType
        {
            TermMatch,
            CombinedMatch,
            MultiTermMatch,
            AndNotMatch,
            BoostingMatch,
            SpatialMatchNoBoosting,
            SpatialMatchHasBoosting,
            MultiUnaryMatch,
            CompiledQueryMatch,
            IQueryMatch
        }

        public static QueryType GetQueryType<T>(in T match)
        {
            var type = match.GetType();
            if (type == typeof(TermMatch))
                return QueryType.TermMatch;

            if (type == typeof(CombinedMatch))
                return QueryType.CombinedMatch;

            if (type == typeof(MultiTermMatch))
                return QueryType.MultiTermMatch;

            if (type == typeof(AndNotMatch))
                return QueryType.AndNotMatch;

            if (type == typeof(BoostingMatch))
                return QueryType.BoostingMatch;

            if (type == typeof(SpatialMatch<NoBoosting>))
                return QueryType.SpatialMatchNoBoosting;

            if (type == typeof(SpatialMatch<HasBoosting>))
                return QueryType.SpatialMatchHasBoosting;

            if (type == typeof(MultiUnaryMatch))
                return QueryType.MultiUnaryMatch;

            if (type == typeof(CompiledQueryMatch))
                return QueryType.CompiledQueryMatch;

            return QueryType.IQueryMatch;
        }
    }
}
