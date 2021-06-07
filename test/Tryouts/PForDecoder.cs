using System;
using System.IO;

namespace Tryouts
{
    public ref struct PForDecoder
    {
        private readonly Span<byte> _input;
        private int _bitPos, _maxBits;
        private int _prevValue;
        private readonly Span<int> _nums;

        public PForDecoder(Span<byte> input, Span<int> scratch)
        {
            _input = input;
            _bitPos = 0;
            _maxBits = input.Length * 8;
            _prevValue = 0;
            _nums = scratch.Slice(0, PForEncoder.BufferLen);
        }

        public Span<int> TryDecode()
        {
            var bits = Read(2);
            switch (bits)
            {
                case 0b00: // fixed header
                    bits = Read(7);
                    int numOfValues = (bits >> 5) switch
                    {
                        0b000 => 1,
                        0b001 => 32,
                        0b010 => 64,
                        0b011 => 128,
                        _ => throw new ArgumentOutOfRangeException((bits >> 5) + " isn't a valid number of items for fixed header")
                    };
                    return ReadNumbers((int)(0x1F & bits), numOfValues);
                case 0b01: // variable size
                    bits = Read(13);
                    return ReadNumbers((int)(0x1F & bits), (int)(bits >> 5));
                case 0b10: // repeated header
                    bits = Read(13);
                    int numOfRepeatedValues = (int)(bits >> 5);
                    int numOfBits = (int)(0x1F & bits);
                    var repeatedDelta = (int)Read(numOfBits);
                    for (int i = 0; i < numOfRepeatedValues; i++)
                    {
                        _prevValue += repeatedDelta;
                        _nums[i] = _prevValue;
                    }
                    return _nums.Slice(0, numOfRepeatedValues);
                case 0b11:
                    return Span<int>.Empty;
                default:
                    throw new ArgumentOutOfRangeException(bits + " isn't a valid header marker");
            }
        }

        private Span<int> ReadNumbers(int numOfBits, int numOfValues)
        {
            if (numOfBits == 0)
                return Span<int>.Empty;
            for (int i = 0; i < numOfValues; i++)
            {
                var v = Read(numOfBits);
                _prevValue += (int)v;
                _nums[i] = _prevValue;
            }
            return _nums.Slice(0, numOfValues);
        }

        private ulong Read(int bitsToRead)
        {
            int end = _bitPos + bitsToRead;
            if (end > _maxBits)
                throw new EndOfStreamException();

            ulong value = 0; ;
            while (_bitPos < end)
            {
                value <<= 1;
                ulong bit = (ulong)(_input[_bitPos >> 3] >> 7 - (_bitPos & 0x7) & 1);
                value += bit;
                _bitPos++;
            }
            return value;
        }
    }
}
