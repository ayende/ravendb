using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Json;

namespace Tryouts.Corax.Bitmaps
{
    public struct PackedBitmapMultiSequenceBuilder : IDisposable
    {
        private readonly JsonOperationContext _ctx;
        private PackedBitmapBuilder _builder;
        private PackedBitmapReader _previous;
        private ulong _last;

        public ulong NumberOfSetBits;

        public PackedBitmapMultiSequenceBuilder(JsonOperationContext ctx)
        {
            _ctx = ctx;
            _builder = new PackedBitmapBuilder(ctx);
            _previous = new PackedBitmapReader(); // initially empty
            _last = 0;
            NumberOfSetBits = 0;
        }

        public void Set(ulong pos)
        {
            NumberOfSetBits++;
            if (_last < pos)
            {
                _last = pos;
                _builder.Set(pos);
                return;
            }
            UnlikelyMergeBitmaps(pos);
        }

        private void UnlikelyMergeBitmaps(ulong pos)
        {
            _last = pos;
            _builder.Complete(out var current);
            MergeBitmaps(current);
            _builder = new PackedBitmapBuilder(_ctx);
            _builder.Set(pos);
        }

        private void MergeBitmaps(PackedBitmapReader current)
        {
            using (_previous)
            {
                if (_previous.SizeInBytes == 0)
                {
                    _previous = current;
                }
                else
                {
                    try
                    {
                        PackedBitmapReader.Or(_ctx, ref _previous, ref current, out _previous);
                    }
                    finally
                    {
                        current.Dispose();
                    }
                }
            }
        }

        public void Dispose()
        {
            _builder.Dispose();
        }

        internal void Complete(out PackedBitmapReader results)
        {
            _builder.Complete(out var temp);
            MergeBitmaps(temp);
            results = _previous;
        }
    }
}
