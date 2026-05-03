using System;
using System.Threading;

namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    public QueryILEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }
    public int Ordering { get; init; }

    /// <summary>Packed parameter type signature (2 bits per param: 0=long, 1=double, 2=string).
    /// Different types produce different IL (different comparison instructions).</summary>
    public int TypeSignature { get; init; }

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
