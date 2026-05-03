using System;
using System.Threading;

namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    public QueryILEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }
    public int Ordering { get; init; }

    /// <summary>Packed parameter type signature (2 bits per param: 0=long, 1=double, 2=string).
    /// For ≤ 16 typed scan predicates this is the exact identity. For more, it carries the
    /// 2-bit-per-kind packing of the FIRST 16 predicates and acts as a lossy hash; in that
    /// case <see cref="FullKinds"/> is non-null and disambiguates via SequenceEqual on the
    /// PlanCache chain walk.</summary>
    public int TypeSignature { get; init; }

    /// <summary>Full per-predicate kind vector. Populated only when there are more than 16
    /// typed scan predicates (where <see cref="TypeSignature"/>'s int packing becomes a
    /// hash rather than an identity). Null in the common case so the hot path doesn't pay
    /// the byte[] allocation.</summary>
    public byte[] FullKinds { get; init; }

    /// <summary>Chain pointer for hash-collision disambiguation in PlanCache. Two plans
    /// share a slot when their int <see cref="TypeSignature"/> values collide but their
    /// <see cref="FullKinds"/> differ — only possible when paramCount &gt; 16. Null in the
    /// common case (chain length 1).</summary>
    public CompiledPlan Next;

    /// <summary>Provider that generates the EXPLAIN pseudocode on first access. Lazily
    /// materialized — Inspect() / EXPLAIN diagnostics are the only consumers, so the
    /// majority of plans never pay the string-build cost. Set once at compile time and
    /// captures the QueryPlan; the first .ExplainSource read replaces it with the
    /// materialized string.</summary>
    public Func<string> ExplainSourceProvider { get; init; }

    private string _explainSource;

    /// <summary>EXPLAIN pseudocode for this plan. First read materializes via
    /// <see cref="ExplainSourceProvider"/> and caches; subsequent reads return the
    /// cached value. Returns "" when no provider was set.</summary>
    public string ExplainSource
    {
        get
        {
            var cached = Volatile.Read(ref _explainSource);
            if (cached != null)
                return cached;
            var provider = ExplainSourceProvider;
            var generated = provider != null ? provider() : "";
            // First-write-wins; concurrent reads each compute but only one's result sticks.
            // The provider is pure (depends only on the plan), so any winner is correct.
            Interlocked.CompareExchange(ref _explainSource, generated, null);
            return Volatile.Read(ref _explainSource);
        }
    }
}
