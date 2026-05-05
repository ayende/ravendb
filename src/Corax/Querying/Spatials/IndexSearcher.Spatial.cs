using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SpatialMatch;
using Spatial4n.Shapes;
using SpatialContext = Spatial4n.Context.SpatialContext;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch SpatialQuery(in FieldMetadata field, double error, IShape shape, SpatialContext spatialContext, Utils.Spatial.SpatialRelation spatialRelation, bool isNegated = false, in CancellationToken token = default)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty.
            return TermMatch.CreateEmpty(this, Allocator);
        }

        IQueryMatch match = field.HasBoost
            ? new SpatialMatch<HasBoosting>(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation, token)
            : new SpatialMatch<NoBoosting>(this, _transaction.Allocator, spatialContext, field, shape, terms, error, spatialRelation, token);
        if (isNegated)
        {
            // Negated spatial query: all entries except those matching the spatial condition
            // Build bitmap from all entries, then AND NOT the spatial match
            var allEntriesBitmap = new Matches.BitmapMatch(Allocator);
            var allEntries = AllEntries();
            Primitives.QueryPrimitives.FillFromMatch(allEntries, ref allEntriesBitmap.BitmapState, Allocator);

            var tempBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);
            Primitives.QueryPrimitives.AndNotWithMatch(match, ref allEntriesBitmap.BitmapState, ref tempBitmapData, Allocator);
            tempBitmapData.Dispose();

            return allEntriesBitmap;
        }

        return match;
    }
}
