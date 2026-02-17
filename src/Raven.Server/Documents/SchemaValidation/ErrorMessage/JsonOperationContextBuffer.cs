using System;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.ErrorMessage;

public class JsonOperationContextBuffer<T>(JsonOperationContext context) : AbstractBuffer<T>
{
    private AllocatedMemoryData _buffer;

    protected override unsafe Span<T> BufferAsSpan()
    {
        return _buffer == null ? Span<T>.Empty : new Span<T>(_buffer.Address, _buffer.SizeInBytes / Unsafe.SizeOf<T>());
    }

    public override void CheckAndGrow(int minRequired)
    {
        minRequired *= Unsafe.SizeOf<T>();
        if (_buffer == null)
        {
            _buffer = context.GetMemory(minRequired);
            return;
        }

        minRequired = Length * Unsafe.SizeOf<T>() + minRequired;
        if (minRequired <= _buffer.SizeInBytes)
            return;
        
        if (context.GrowAllocation(_buffer, minRequired))
            return;

        var newBuffer = context.GetMemory(minRequired);
        _buffer.AsSpan().CopyTo(newBuffer.AsSpan());
        context.ReturnMemory(_buffer);
        _buffer = newBuffer;
    }

    public override void Dispose()
    {
        if (_buffer != null)
            context.ReturnMemory(_buffer);
    }

    public unsafe int Append(int alreadySeen, UnmanagedWriteBuffer buffer)
    {
        var toAdd = Length - alreadySeen;
        CheckAndGrow(toAdd);
        buffer.CopyTo(alreadySeen, _buffer.Address);
        Length += toAdd;
        return toAdd;
    }

    public Memory<byte> AsMemory() => _buffer.AsMemory()[..Length];
}
