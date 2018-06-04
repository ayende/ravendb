using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using Sparrow;

namespace Voron.Data.PostingList
{
    public unsafe struct PostingListBuffer
    {
        public ByteStringContext<ByteStringMemoryCache>.InternalScope Scope;
        public ByteString Buffer;
        public long Start;
        public long Last;
        public int Size;
        public int Used;
        public bool HasModifications;

        public long ComputeLast()
        {
            byte* buffer = Buffer.Ptr;
            byte* end = buffer + Used;
            long value = Start;
            while (buffer < end)
            {
                long delta = ReadVariableSizeLong(ref buffer);
                value += delta;
            }

            return value;
        }

        public bool TryAppend(long num)
        {
            if (num < Last)
                ThrowInvalidAppend(num);

            var buffer = stackalloc byte[9]; // max size
            int numberSize = WriteVariableSizeLong(num - Last, buffer);
            if (numberSize + Used > Size)
                return false; // won't fit

            HasModifications = true;
            Unsafe.CopyBlock(Buffer.Ptr + Used, buffer, (uint)numberSize);
            Used += numberSize;
            Last = num;
            return true;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeInt(ref byte* buffer)
        {
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            // we assume that the value shouldn't be zero very often
            // because then we'll always take 5 bytes to store it

            int count = 0;
            byte shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = *(buffer++);
              

                count |= (b & 0x7F) << shift;
                shift += 7;                
            }
            while ((b & 0x80) != 0);

            return count;

            Error:
            ThrowInvalidShift();            

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadVariableSizeLong(ref byte* buffer)
        {
            // The high bit 
            // of the byte when on means to continue reading more bytes.

            ulong count = 0;
            byte shift = 0;
            byte b;
            do
            {
                if (shift == 70)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = *buffer++;
                count |= (ulong)(b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);

            // good handling for negative values via:
            // http://code.google.com/apis/protocolbuffers/docs/encoding.html#types

            return (long)(count >> 1) ^ -(long)(count & 1);

            Error:
            ThrowInvalidShift();
            return -1;
        }

        private static void ThrowInvalidShift()
        {
            throw new FormatException("Bad variable size int");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int WriteVariableSizeLong(long value, byte* buffer)
        {
            // see zig zap trick here:
            // https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
            // for negative values

            int count = 0;
            ulong v = (ulong)((value << 1) ^ (value >> 63));
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }

            buffer[count++] = (byte)v;
            return count;
        }

        private void ThrowInvalidAppend(long num)
        {
            throw new ArgumentException($"Appending to posting list must be larger than the previous, but Last was {Last} and now got {num}");
        }

        public bool Delete(long val)
        {
            var currentBufferPos = Buffer.Ptr;
            var end = Buffer.Ptr + Used;
            byte* previousBufferPos;
            var current = Start;
            long delta = 0;
            do
            {
                previousBufferPos = currentBufferPos;
                delta = ReadVariableSizeLong(ref currentBufferPos);
                current += delta;
            } while (current < val && currentBufferPos < end);

            if (current != val)
                return false; // value not found, nothing to do

            HasModifications = true;
            if (currentBufferPos - Buffer.Ptr == Used)
            {
                // just need to shrink
                Used -= (int)(currentBufferPos - previousBufferPos);
                return true;
            }

            // need to actually move the data
            var nextDelta = ReadVariableSizeLong(ref currentBufferPos);
            var fixedDelta = delta + nextDelta;
            var amountToMove = Used - (int)(currentBufferPos - Buffer.Ptr);
            // Note that here we _know_ that the new delta size can never be
            // more than the size of both deltas, so it is safe to overwrite
            var size = WriteVariableSizeLong(fixedDelta, previousBufferPos);
            UnmanagedMemory.Move(previousBufferPos + size, currentBufferPos, amountToMove);
            Used -= (int)(currentBufferPos - previousBufferPos - size);
            HasModifications = true;
            return true;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            var currentBufferPos = Buffer.Ptr;
            var end = currentBufferPos + Used;
            var current = Start;
            while (currentBufferPos < end)
            {
                var delta = ReadVariableSizeLong(ref currentBufferPos);
                current += delta;
                sb.AppendFormat("{0:#,#}, ", current).AppendLine();
            }
            return sb.ToString();
        }
    }
}
