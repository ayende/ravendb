#nullable enable

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Security.Cryptography;

namespace Corax.Querying.Planning;

public ref struct PlanCacheKeyBuilder(Span<byte> scratch)
{
    private Span<byte> _buffer = scratch;
    private byte[]? _rented = null;
    private int _bytePosition = 0;

    private ulong _bitAccumulator = 0;
    private int _bitCount = 0;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(int value, int bits)
    {
        Debug.Assert(
            bits is > 0 and < 32 && // cannot send 32 bits (it's an int, would be negative)
            (uint)value >>> bits == 0
        );
        int freeBits = 64 - _bitCount;
        if (bits >= freeBits)
        {
            _bitAccumulator |= (ulong)value << _bitCount;
            value >>>= freeBits;
            bits -= freeBits;
            AppendBitsToBuffer(8);
        }

        _bitAccumulator |= (ulong)value << _bitCount;
        _bitCount += bits;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AppendBitsToBuffer(int bytes)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteUInt64LittleEndian(_buffer[_bytePosition..], _bitAccumulator);
        _bytePosition += bytes;
        _bitAccumulator = 0;
        _bitCount = 0;
      
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity(int count)
    {
        if (_bytePosition + count > _buffer.Length)
            Grow(count);
    }

    private void Grow(int count)
    {
        int required = _bytePosition + count;
        int newSize = Math.Max(_buffer.Length * 2, required);
        byte[] newRented = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer[.._bytePosition].CopyTo(newRented);
        if (_rented != null)
            ArrayPool<byte>.Shared.Return(_rented);
        _rented = newRented;
        _buffer = newRented;
    }

    public Vector256<long> ToHash()
    {
        if (_bitCount > 0)
            AppendBitsToBuffer((_bitCount + 7) / 8);

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(_buffer[.._bytePosition], digest);
        
        if (_rented != null)
        {
            ArrayPool<byte>.Shared.Return(_rented);
            _rented = null;
        }

        return Vector256.Create(MemoryMarshal.Cast<byte, long>(digest));
    }
}
