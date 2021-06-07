using System;
using System.Diagnostics;
using System.Numerics;

namespace Tryouts
{
    
    /*
   * See Binary Packing (2.6) here: https://arxiv.org/pdf/1209.2137.pdf
   * for the baseline idea on the format. 
   * 
   * The output represent a bit stream with format that is a sequence of header | data pairs.
   * The header is defined as: 
   * 
   * * 0b00  (9 bits) - fixed header marker - followed by 3 bits value indicating the number of numbers following
   *                  * 0b00 - 1 value
   *                  * 0b01 - 32 values
   *                  * 0b10 - 64 values
   *                  * 0b11 - 128 values
   *            then 5 bits value (B bits per number)
   * * 0b01   (15 bits) - variable header marker - followed by 8 bits value (number of items) and 5 bits value (B bits per number)
   * * 0b10   (15 bits) - repeated header marker - followed by 8 bits value (number of repetitions) and 5 bits value (B bits per number) 
   * * 0b11   - reserved
   */
    public ref struct PForEncoder
    {
        public const int BufferLen = 128;

        private readonly Span<byte> _output;
        private int _bufPos, _bitPos;
        private readonly int _maxNumOfBits;
        private readonly Span<uint> _deltasBuffer;
        private int _prev;
        private bool _first;

        public int SizeInBytes;

        public PForEncoder(Span<byte> output, Span<uint> scratchBuffer)
        {
            _output = output;
            _bufPos = 0;
            _bitPos = 0;
            _maxNumOfBits = output.Length * 8;
            _prev = 0;
            _first = true;
            SizeInBytes = -1;
            _deltasBuffer = scratchBuffer.Slice(0, BufferLen);
        }

        public bool TryAdd(int value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException();
            if (!_first && _prev > value)
                throw new ArgumentOutOfRangeException();

            if (_first)
            {
                _deltasBuffer[_bufPos++] = (uint)value;
                _prev = value;
                _first = false;
                return TryFlush();
            }

            if (_bufPos < BufferLen)
            {
                var diff = value - _prev;
                _prev = value;
                _deltasBuffer[_bufPos++] = (uint)diff;
                return true;
            }
            if (TryFlush() == false)
                return false;
            return TryAdd(value);
        }

        public bool TryClose()
        {
            var result = TryFlush() &&
                         TryPushBits(0b11, 2) &&
                         // last value in the compressed range, aligned to the last 4 bytes
                         TryPushBits((ulong)_prev, 32 + (_bitPos % 8));
            
            
            SizeInBytes = (_bitPos + 7) / 8;
            _bitPos = int.MaxValue;
            return result;
        }

        private bool TryFlush(Span<uint> buffer)
        {
            if (buffer.Length == 0)
                return true;
            if (buffer.Length == 1)
            {
                var bits = 32 - BitOperations.LeadingZeroCount(buffer[0]);
                Debug.Assert(bits < 32); // we never encode 0
                ulong header = 0b00_00_00000ul | (uint)bits;
                return TryPushBits(header, 9) && TryPushBits(buffer[0], bits);
            }
            var (maxBits, identicalPrefix) = Analyze(buffer);
            Debug.Assert(identicalPrefix <= 256);
            if (identicalPrefix > 5) // enough to warrant a repeating header to save space
            {
                ulong header = 0b10_00000000_00000ul | (uint)identicalPrefix << 5 | (uint)maxBits;
                if (TryPushBits(header, 15) == false ||
                    TryPushBits(buffer[0], maxBits) == false)
                    return false;
                return TryFlush(buffer.Slice(identicalPrefix));
            }
            ulong fixedSizeMarker = buffer.Length switch
            {
                32 => 0b01,
                64 => 0b10,
                128 => 0b11,
                _ => 0
            };
            if (fixedSizeMarker != 0)
            {
                var half = buffer.Length / 2;
                // now need to figure out optimal partitioning scheme
                int first = MaxBits(buffer.Slice(0, half)),
                    second = MaxBits(buffer.Slice(half));

                if (first != second) // better to output them as two segments, then
                {

                    return TryFlush(buffer.Slice(0, half)) &&
                           TryFlush(buffer.Slice(half));
                }
                ulong header = 0b00_00_00000ul | fixedSizeMarker << 5 | (uint)maxBits;
                if (TryPushBits(header, 9) == false)
                    return false;
                return TryPushValues(buffer, maxBits);
            }
            else
            {
                ulong header = 0b01_0000_0000_00000ul | (ulong)buffer.Length << 5 | (uint)maxBits;
                if (TryPushBits(header, 15) == false)
                    return false;
                return TryPushValues(buffer, maxBits);
            }
        }

        private bool TryPushValues(Span<uint> buffer, int maxBits)
        {
            for (int i = 0; i < buffer.Length; i++)
            {
                if (TryPushBits(buffer[i], maxBits) == false)
                {
                    return false;
                }
            }
            return true;
        }

        private bool TryFlush()
        {
            var oldPos = _bufPos;
            _bufPos = 0;
            return TryFlush(_deltasBuffer.Slice(0, oldPos));
        }

        private static int MaxBits(Span<uint> buffer)
        {
            uint mask = 0;
            for (int i = 0; i < buffer.Length; i++)
            {
                mask |= buffer[i];
            }
            var maxBits = 32 - BitOperations.LeadingZeroCount(mask);
            Debug.Assert(maxBits < 32); // we never encode 0
            return maxBits;
        }

        private static (int MaxNumberOfBits, int IdenticalPrefixLegnth) Analyze(Span<uint> buffer)
        {
            uint mask = buffer[0];
            int identicalPrefix = 1;
            bool identical = true;
            for (int i = 1; i < buffer.Length; i++)
            {
                mask |= buffer[i];
                if (identical)
                {
                    if (buffer[i] == buffer[0])
                    {
                        identicalPrefix++;
                    }
                    else
                    {
                        identical = false;
                    }
                }
            }
            var maxBits = 32 - BitOperations.LeadingZeroCount(mask);
            Debug.Assert(maxBits < 32); // we never encode 0
            return (maxBits, identicalPrefix);
        }

        // see: 
        // https://github.com/facebookarchive/beringei/blob/75c3002b179d99c8709323d605e7d4b53484035c/beringei/lib/BitUtil.cpp#L17
        public bool TryPushBits(ulong value, int bitsInValue)
        {
            if (_bitPos + bitsInValue > _maxNumOfBits)
            {
                return false;
            }

            int bitsAvailable = (_bitPos & 0x7) != 0 ? 8 - (_bitPos & 0x7) : 0;
            var bytePos = _bitPos / 8;
            _bitPos += bitsInValue;
            if (bitsInValue <= bitsAvailable)
            {
                // Everything fits inside the last byte
                _output[bytePos] += (byte)(value << bitsAvailable - bitsInValue);
                return true;
            }

            int bitsLeft = bitsInValue;
            if (bitsAvailable > 0)
            {
                // Fill up the last byte
                _output[bytePos] += (byte)(value >> bitsInValue - bitsAvailable);
                bitsLeft -= bitsAvailable;
                bytePos++;
            }

            while (bitsLeft >= 8)
            {
                // Enough bits for a dedicated byte
                _output[bytePos] = (byte)(value >> bitsLeft - 8 & 0xFF);
                bitsLeft -= 8;
                bytePos++;
            }

            if (bitsLeft != 0)
            {
                // Start a new byte with the rest of the bits
                _output[bytePos] += (byte)((value & (1U << bitsLeft) - 1) << 8 - bitsLeft);
            }
            return true;
        }
    }
}
