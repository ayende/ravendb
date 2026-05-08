using System;

namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    /// <summary>IL-emitted delegate that executes the posting-list scan plan.
    /// Takes a <see cref="QueryScanContext"/> by ref and fills / intersects the
    /// bitmap slots according to the compiled <see cref="PlanOp"/> sequence.</summary>
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

    /// <summary>EXPLAIN pseudocode for this plan. Generated in the same pass as
    /// the IL emission so they cannot drift out of sync.</summary>
    public string ExplainSource { get; init; }
}
