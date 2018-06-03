namespace Ewah
{

/*
 * Copyright 2012, Kemal Erdogan, Daniel Lemire and Ciaran Jessup
 * Licensed under APL 2.0.
 */
 
    /// <summary>
    /// Mostly for internal use.
    /// </summary>
    public sealed class RunningLengthWord
    {
        #region Constants

        /// <summary>
        /// largest number of dirty words in a run
        /// </summary>
        public const long LargestLiteralCount = (1L << LiteralBits) - 1;

        /// <summary>
        /// largest number of clean words in a run 
        /// </summary>
        public const long LargestRunningLengthCount = (1L << RunningLengthBits) - 1;

        /// <summary>
        /// number of bits dedicated to marking  of the running length of clean words
        /// </summary>
        public const int RunningLengthBits = 32;

        private const int LiteralBits = 64 - 1 - RunningLengthBits;
        private const long NotRunningLengthPlusRunningBit = ~RunningLengthPlusRunningBit;
        private const long NotShiftedLargestRunningLengthCount = ~ShiftedLargestRunningLengthCount;

        private const long RunningLengthPlusRunningBit = (1L << (RunningLengthBits + 1)) - 1;
        private const long ShiftedLargestRunningLengthCount = LargestRunningLengthCount << 1;

        #endregion

        #region Fields

        /// <summary>
        /// The array of words. 
        /// </summary>
        public long[] ArrayOfWords;

        /// <summary>
        /// The Position in array. 
        /// </summary>
        public int Position;

        #endregion

        #region C'tors

        /// <summary>
        /// Instantiates a new running length word
        /// </summary>
        /// <param name="a">an array of 64-bit words</param>
        /// <param name="p">p Position in the array where the running length word is located</param>
        internal RunningLengthWord(long[] a, int p)
        {
            ArrayOfWords = a;
            Position = p;
        }

        #endregion

        #region Instance Properties

        /// <summary>
        /// Return the size in uncompressed words represented by
        /// this running length word.
        /// </summary>
        /// <returns></returns>
        public long Count
        {
            get { return RunningLength + NumberOfLiteralWords; }
        }

        /// <summary>
        /// the number of literal words
        /// </summary>
        public long NumberOfLiteralWords
        {
            get { return (long) (((ulong) ArrayOfWords[Position]) >> (1 + RunningLengthBits)); }
            set
            {
                ArrayOfWords[Position] |= NotRunningLengthPlusRunningBit;
                ArrayOfWords[Position] &= (value << (RunningLengthBits + 1))
                                          | RunningLengthPlusRunningBit;
            }
        }

        /// <summary>
        /// the running bit
        /// </summary>
        public bool RunningBit
        {
            get { return (ArrayOfWords[Position] & 1) != 0; }
            set
            {
                if (value)
                {
                    ArrayOfWords[Position] |= 1L;
                }
                else
                {
                    ArrayOfWords[Position] &= ~1L;
                }
            }
        }

        /// <summary>
        /// the running length
        /// </summary>
        public long RunningLength
        {
            get { return (long) ((((ulong) ArrayOfWords[Position]) >> 1) & LargestRunningLengthCount); }
            set
            {
                ArrayOfWords[Position] |= ShiftedLargestRunningLengthCount;
                ArrayOfWords[Position] &= (value << 1)
                                          | NotShiftedLargestRunningLengthCount;
            }
        }

        #endregion

        #region ICloneable Members

        public object Clone()
        {
            var clone = new RunningLengthWord(ArrayOfWords,Position);
            return clone;
        }

        #endregion
        #region Instance Methods

        public override string ToString()
        {
            return "running bit = " + RunningBit + " running length = "
                   + RunningLength + " number of lit. words "
                   + NumberOfLiteralWords;
        }

        #endregion
    }
}
