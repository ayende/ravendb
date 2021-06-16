using System;

namespace Voron.Data.Sets
{
    public static class ZigZag
    {
        public static int Encode(Span<byte> buffer, long value)
        {
            ulong uv = (ulong)((value << 1) ^ (value >> 63));
            return Encode7Bits(buffer, uv);
        }
            
        public int Encode7Bits(Span<byte> buffer, ulong uv)
        {
            var len = 0;
            while (uv > 0x7Fu)
            {
                buffer[len++] = ((byte)((uint)uv | ~0x7Fu));
                uv >>= 7;
            }
            buffer[len++] = ((byte)uv);
            return len;
        }

        public static long Decode(ref Span<byte> buffer)
        {
            ulong result = Decode7Bits(ref buffer);
            return (long)((result & 1) != 0 ? (result >> 1) - 1 : (result >> 1));
        }
            
        public static ulong Decode7Bits(ref Span<byte> buffer)
        {
            ulong result = 0;
            byte byteReadJustNow;
            var length = 0;

            const int maxBytesWithoutOverflow = 9;
            for (int shift = 0; shift < maxBytesWithoutOverflow * 7; shift += 7)
            {
                byteReadJustNow = buffer[length++];
                result |= (byteReadJustNow & 0x7Ful) << shift;

                if (byteReadJustNow <= 0x7Fu)
                {
                    buffer = buffer.Slice(length);
                    return result;
                }
            }

            byteReadJustNow = buffer[length];
            if (byteReadJustNow > 0b_1u)
            {
                throw new ArgumentOutOfRangeException("result", "Bad var int value");
            }

            result |= (ulong)byteReadJustNow << (maxBytesWithoutOverflow * 7);
            buffer = buffer.Slice(length);
            return result;
        }
    }
}
