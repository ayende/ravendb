using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace Corax.Querying.Planning;

/// <summary>
/// 256-bit SHA-256 digest of a query's canonical plan-cache key serialization.
///
/// Replaces the four fixed-width key fields (ordering, type-signature, full-kinds, when-flags)
/// that previously disambiguated compiled plans. Those fields each carried a hard ceiling
/// (10 clauses for ordering, 16 params for the packed signature, 32 WHEN clauses); folding
/// them into a single digest removes all of those limits — any new key dimension becomes one
/// more <see cref="PlanCacheKeyBuilder.Append(int)"/> call with no storage-size impact.
///
/// SHA-256 is used (not a faster non-cryptographic hash) because RQL is client-controlled:
/// only a cryptographic hash gives collision RESISTANCE against an adversary who can craft
/// query text. With 256 bits and at most 32 plans per query, the birthday collision
/// probability is on the order of 1e-75 — far below the rate of an undetected hardware fault,
/// so the full digest IS the plan identity. No byte-for-byte revalidation of the underlying
/// serialization is needed on a match.
/// </summary>
public readonly struct PlanCacheKeyHash : IEquatable<PlanCacheKeyHash>
{
    private readonly ulong _w0;
    private readonly ulong _w1;
    private readonly ulong _w2;
    private readonly ulong _w3;

    private PlanCacheKeyHash(ulong w0, ulong w1, ulong w2, ulong w3)
    {
        _w0 = w0;
        _w1 = w1;
        _w2 = w2;
        _w3 = w3;
    }

    /// <summary>Pack a 32-byte SHA-256 digest into four little-endian 64-bit words.</summary>
    public static PlanCacheKeyHash FromDigest(ReadOnlySpan<byte> digest)
    {
        return new PlanCacheKeyHash(
            BinaryPrimitives.ReadUInt64LittleEndian(digest),
            BinaryPrimitives.ReadUInt64LittleEndian(digest.Slice(8)),
            BinaryPrimitives.ReadUInt64LittleEndian(digest.Slice(16)),
            BinaryPrimitives.ReadUInt64LittleEndian(digest.Slice(24)));
    }

    /// <summary>Low 64 bits of the digest — used as the SIMD pre-filter key in
    /// <see cref="PlanCache"/>. A lane hit is confirmed with a full 256-bit <see cref="Equals(PlanCacheKeyHash)"/>.</summary>
    public long Lo
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => unchecked((long)_w0);
    }

    public bool Equals(PlanCacheKeyHash other)
        => _w0 == other._w0 && _w1 == other._w1 && _w2 == other._w2 && _w3 == other._w3;

    public override bool Equals(object obj) => obj is PlanCacheKeyHash other && Equals(other);

    public override int GetHashCode() => _w0.GetHashCode();
}

/// <summary>
/// Builds the canonical byte serialization that <see cref="PlanCacheKeyHash"/> digests.
/// Appends are fixed-width and little-endian, so the same <see cref="Append(int)"/> sequence
/// must be issued (in the same order) on both the cache-probe and cache-store paths — there is
/// no length-prefixing safety net beyond the caller's discipline because the digest is the only
/// thing compared.
///
/// The scratch buffer is caller-provided (stackalloc); 128 bytes covers the overwhelming
/// majority of queries. Larger keys (very many clauses/parameters) spill to a pooled array,
/// which <see cref="ToHash"/> returns. Allocation only happens on spill.
/// </summary>
public ref struct PlanCacheKeyBuilder
{
    private Span<byte> _buffer;
    private byte[] _rented;
    private int _position;

    public PlanCacheKeyBuilder(Span<byte> scratch)
    {
        _buffer = scratch;
        _rented = null;
        _position = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Span<byte> Reserve(int count)
    {
        if (_position + count > _buffer.Length)
            Grow(count);

        Span<byte> slot = _buffer.Slice(_position, count);
        _position += count;
        return slot;
    }

    private void Grow(int count)
    {
        int required = _position + count;
        int newSize = Math.Max(_buffer.Length * 2, required);
        byte[] newRented = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.Slice(0, _position).CopyTo(newRented);
        if (_rented != null)
            ArrayPool<byte>.Shared.Return(_rented);
        _rented = newRented;
        _buffer = newRented;
    }

    public void Append(byte value)
    {
        Reserve(1)[0] = value;
    }

    public void Append(ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(Reserve(sizeof(ushort)), value);
    }

    public void Append(int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(Reserve(sizeof(int)), value);
    }

    public void Append(long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(Reserve(sizeof(long)), value);
    }

    public void Append(ReadOnlySpan<byte> value)
    {
        value.CopyTo(Reserve(value.Length));
    }

    /// <summary>Digest everything appended so far. Returns any pooled spill buffer to the pool.</summary>
    public PlanCacheKeyHash ToHash()
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(_buffer.Slice(0, _position), digest);
        PlanCacheKeyHash hash = PlanCacheKeyHash.FromDigest(digest);

        if (_rented != null)
        {
            ArrayPool<byte>.Shared.Return(_rented);
            _rented = null;
        }

        return hash;
    }
}
