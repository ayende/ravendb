namespace Ewah
{
/*
 * Copyright 2012, Kemal Erdogan, Daniel Lemire and Ciaran Jessup
 * Licensed under APL 2.0.
 */


    /// <summary>
    /// Mostly for internal use. Similar to RunningLengthWord, but can
    /// be modified without access to the array, and has faster access.
    /// </summary>
    internal sealed class BufferedRunningLengthWord
    {
        #region Fields

        /// <summary>
        /// how many dirty words have we read so far?
        /// </summary>
        public int DirtyWordOffset;

        /// <summary>
        /// The Number of literal words
        /// </summary>
        public int NumberOfLiteralWords;

        /// <summary>
        /// The Running bit
        /// </summary>
        public bool RunningBit;

        /// <summary>
        /// The Running length
        /// </summary>
        public long RunningLength;

        #endregion

        #region C'tors

        /// <summary>
        /// Instantiates a new buffered running length word
        /// </summary>
        /// <param name="rlw">the rlw</param>
        public BufferedRunningLengthWord(RunningLengthWord rlw)
            : this(rlw.ArrayOfWords[rlw.Position])
        {
        }

        /// <summary>
        /// Instantiates a new buffered running length word
        /// </summary>
        /// <param name="a">the word</param>
        public BufferedRunningLengthWord(long a)
        {
            NumberOfLiteralWords = (int) (((ulong) a) >> (1 + RunningLengthWord.RunningLengthBits));
            RunningBit = (a & 1) != 0;
            RunningLength = (int) ((((ulong) a) >> 1) & RunningLengthWord.LargestRunningLengthCount);
        }

        #endregion

        #region Instance Properties

        /// <summary>
        /// Size in uncompressed words
        /// </summary>
        public long Count
        {
            get { return RunningLength + NumberOfLiteralWords; }
        }

        #endregion

        #region Instance Methods

        public override string ToString()
        {
            return "running bit = " + RunningBit + " running length = "
                   + RunningLength + " number of lit. words "
                   + NumberOfLiteralWords;
        }

        /// <summary>
        /// Discard first words
        /// </summary>
        /// <param name="x"></param>
        public void DiscardFirstWords(long x)
        {
            if (RunningLength >= x)
            {
                RunningLength -= x;
                return;
            }
            x -= RunningLength;
            RunningLength = 0;
            DirtyWordOffset += (int) x;
            NumberOfLiteralWords -= (int) x;
        }

        /// <summary>
        /// Reset the values of this running length word so that it has the same values
        /// as the other running length word.
        /// </summary>
        /// <param name="rlw">the other running length word </param>
        public void Reset(RunningLengthWord rlw)
        {
            Reset(rlw.ArrayOfWords[rlw.Position]);
        }

        /// <summary>
        /// Reset the values using the provided word.
        /// </summary>
        /// <param name="a">the word</param>
        public void Reset(long a)
        {
            NumberOfLiteralWords = (int) (((ulong) a) >> (1 + RunningLengthWord.RunningLengthBits));
            RunningBit = (a & 1) != 0;
            RunningLength = (int) ((((ulong) a) >> 1) & RunningLengthWord.LargestRunningLengthCount);
            DirtyWordOffset = 0;
        }

        #endregion
    }
}