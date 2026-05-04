using System;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying;

/// <summary>
/// Temporary stubs for deleted test-only infrastructure.
/// These will be removed once SearchQuery is refactored to use QueryPlanBuilder.
/// </summary>
public partial class IndexSearcher
{
    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch AllInQuery(in FieldMetadata field, System.Collections.Generic.HashSet<(string Term, bool Exact)> allInTerms, in CancellationToken token = default)
        => throw new NotImplementedException("AllInQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch AllInQuery(in FieldMetadata field, System.Collections.Generic.HashSet<(string Term, bool Exact)> allInTerms, CancellationToken cancellationToken = default)
        => throw new NotImplementedException("AllInQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch AllInQuery(in FieldMetadata field, System.Collections.Generic.HashSet<Voron.Slice> allInTerms, in CancellationToken token = default)
        => throw new NotImplementedException("AllInQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch AllInQuery(in FieldMetadata field, System.Collections.Generic.HashSet<Voron.Slice> allInTerms, CancellationToken cancellationToken = default)
        => throw new NotImplementedException("AllInQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch InQuery(string field, System.Collections.Generic.List<string> inTerms)
        => throw new NotImplementedException("InQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch InQuery(in FieldMetadata field, System.Collections.Generic.List<string> inTerms)
        => throw new NotImplementedException("InQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch InQuery(in FieldMetadata field, System.Collections.Generic.List<(string Term, bool Exact)> inTerms, in CancellationToken token = default)
        => throw new NotImplementedException("InQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use QueryPlanBuilder instead")]
    public IQueryMatch InQuery(in FieldMetadata field, System.Collections.Generic.List<Voron.Slice> inTerms, in CancellationToken token = default)
        => throw new NotImplementedException("InQuery has been deleted. Use QueryPlanBuilder instead.");

    [Obsolete("Deleted - use bitmap AND instead")]
    public IQueryMatch AndNot(IQueryMatch inner, IQueryMatch outer, in CancellationToken token = default)
        => throw new NotImplementedException("AndNot has been deleted. Use bitmap operations instead.");
}
