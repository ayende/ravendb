using System;
using System.Buffers;
using Sparrow.Server;

namespace Corax.Utils;

public struct VectorValue : IDisposable
{
    private readonly IDisposable _memoryScope;
    private readonly Memory<byte> _memory;
    private int _length;
    public int Length => _length;

    // Source (pre-packing) dimension count when known; 0 means unknown. Bit-packed Binary embeddings lose the
    // exact dimension count (ceil(dims/8) is not injective), so callers that still hold the source element count
    // record it here to allow an exact dimensionality check later.
    private int _sourceDimensions;
    public int SourceDimensions => _sourceDimensions;

    public readonly bool IsNull;
    public static readonly VectorValue Null = new(true);

    public ReadOnlySpan<byte> GetEmbedding()
    {
        return _memory.Span.Slice(0, _length);
    }

    public Memory<byte> GetEmbeddingMemory()
    {
        return _memory.Slice(0, _length);
    }

    public VectorValue()
    {
    }
    
    private VectorValue(bool isNull)
    {
        IsNull = isNull;
    }

    public VectorValue(IDisposable memoryScope, Memory<byte> embedding, int? length = null)
    {
        _memoryScope = memoryScope;
        _memory = embedding;
        _length = length ?? embedding.Length;
    }

    public void OverrideLength(int len) => _length = len;

    public void SetSourceDimensions(int dimensions) => _sourceDimensions = dimensions;

    public void Dispose()
    {
        _memoryScope?.Dispose();
    }
}
