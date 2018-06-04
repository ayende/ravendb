using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Ewah
{
    /*
     * Copyright 2012, Kemal Erdogan, Daniel Lemire and Ciaran Jessup
     * Licensed under APL 2.0.
     */


    /// <summary>
    /// <p>This implements the patent-free(1) EWAH scheme. Roughly speaking, it is a
    /// 64-bit variant of the BBC compression scheme used by Oracle for its bitmap
    /// indexes.</p>
    /// 
    /// <p>The objective of this compression type is to provide some compression, while
    /// reducing as much as possible the CPU cycle usage.</p>
    /// 
    /// 
    /// <p>This implementation being 64-bit, it assumes a 64-bit CPU together with a
    /// 64-bit .NET runtime. This same code on a 32-bit machine may not be as
    /// fast.</p>
    /// 
    /// <p>For more details, see the following paper:</p>
    /// 
    /// <ul><li>Daniel Lemire, Owen Kaser, Kamel Aouiche, Sorting improves word-aligned
    /// bitmap indexes. Data & Knowledge Engineering 69 (1), pages 3-28, 2010.
    /// http://arxiv.org/abs/0901.3751</li>
    /// </ul>
    /// 
    /// <p>It was first described by Wu et al. and named WBC:</p>
    /// 
    /// <ul><li>K. Wu, E. J. Otoo, A. Shoshani, H. Nordberg, Notes on design and
    /// implementation of compressed bit vectors, Tech. Rep. LBNL/PUB-3161, Lawrence
    /// Berkeley National Laboratory, available from http://crd.lbl.
    /// gov/~kewu/ps/PUB-3161.html (2001).</li>
    /// </ul>
    /// 
    /// <p>We can view this scheme as a 64-bit equivalent to the 
    /// Oracle bitmap compression scheme:</p>
    /// <ul><li>G. Antoshenkov, Byte-Aligned Bitmap Compression, DCC'95, 1995.</li></ul>
    /// 
    /// <p>1- The author (D. Lemire) does not know of any patent infringed by the
    /// following implementation. However, similar schemes, like WAH are covered by
    /// patents.</p>
    /// 
    /// <para>Ported to C# by Kemal Erdogan</para>
    /// </summary>
    [Serializable]
    public sealed class EwahCompressedBitArray :IEnumerable<int>
    {
        #region Constants

        /// <summary>
        /// the number of bits in a long
        /// </summary>
        public const int WordInBits = 64;

        /// <summary>
        /// default memory allocation when the object is constructed.
        /// </summary>
        private const int DefaultBufferSize = 4;

        #endregion


        #region Fields

        /// <summary>
        /// current (last) running length word
        /// </summary>
        internal RunningLengthWord _Rlw;

        internal int _ActualSizeInWords = 1;

        /// <summary>
        /// The buffer (array of 64-bit words)
        /// </summary>
        internal long[] _Buffer;

        #endregion

        #region C'tors

        /// <summary>
        /// Creates an empty bitmap (no bit set to true).
        /// </summary>
        public EwahCompressedBitArray()
        {
            _Buffer = new long[DefaultBufferSize];
            _Rlw = new RunningLengthWord(_Buffer, 0);
        }

        /// <summary>
        /// Sets explicitly the buffer size (in 64-bit words). The initial memory usage
        /// will be "buffersize * 64". For large poorly compressible bitmaps, using
        /// large values may improve performance.
        /// </summary>
        /// <param name="buffersize">buffersize number of 64-bit words reserved when the object is created</param>
        public EwahCompressedBitArray(int buffersize)
        {
            _Buffer = new long[buffersize];
            _Rlw = new RunningLengthWord(_Buffer, 0);
        }

      
        #endregion

        #region Instance Properties

        /// <summary>
        /// The size in bits of the *uncompressed* bitmap represented by this
        /// compressed bitmap. Initially, the SizeInBits is zero. It is extended
        /// automatically when you set bits to true.
        /// </summary>
        public int SizeInBits { get; set; }

        /// <summary>
        /// Report the *compressed* size of the bitmap (equivalent to memory usage,
        /// after accounting for some overhead).
        /// </summary>
        public int SizeInBytes
        {
            get { return _ActualSizeInWords * 8; }
        }

        #endregion

        #region Instance Methods

        /// <summary>
        /// Check to see whether the two compressed bitmaps contain the same data
        /// (effectively check whether the cardinality of a XOR is == 0.
        /// </summary>
        /// <param name="o">the other bitmap</param>
        /// <returns></returns>
        public override bool Equals(Object o)
        {
            var other = o as EwahCompressedBitArray;
            if (other != null)
            {
                return this.Xor(other).GetCardinality() == 0; // could be more efficient
            }
            return false;
        }

        /// <summary>
        /// Returns a customized hash code (based on Karp-Rabin).
        /// Naturally, if the bitmaps are equal, they will hash to the same value.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            long karprabin = 0;
            const int B = 31;
            EwahEnumerator i = this.GetEwahEnumerator();
            while (i.HasNext())
            {
                i.Next();
                if (i._Rlw.RunningBit == true)
                {
                    karprabin += B * karprabin
                               + (i._Rlw.RunningLength & ((1L << 32) - 1));
                    karprabin += B * karprabin + (long)(((ulong)i._Rlw.RunningLength) >> 32);
                }
                int dw = i.DirtyWords;
                long numLiteralWords = i._Rlw.NumberOfLiteralWords;
                long buf;
                for (int k = 0; k < numLiteralWords; ++k)
                {
                    buf = this._Buffer[dw + k];
                    karprabin += B * karprabin + (buf & ((1L << 32) - 1));
                    karprabin += B * karprabin + (long)(((ulong)buf) >> 32);
                }
            }
            return (int)karprabin;
        }

        /// <summary>
        /// A string describing the bitmap
        /// </summary>
        /// <returns>the description string</returns>
        public override string ToString()
        {
            var ans = new StringBuilder("{");


            IEnumerator<int> it = ((IEnumerable<int>)this).GetEnumerator();

            if (it.MoveNext())
                while (true)
                {
                    ans.Append(it.Current);
                    var b = it.MoveNext();
                    if (b)
                        ans.Append(",");
                    else
                        break;
                }
            ans.Append("}");
            return ans.ToString();
        }

        /// <summary>
        /// Adding words directly to the bitmap (for expert use).
        /// 
        /// This is normally how you add data to the array. So you add bits in streams
        /// of 8*8 bits.
        /// </summary>
        /// <param name="newdata">the word</param>
        /// <returns>the number of words added to the buffer</returns>
        public int Add(long newdata)
        {
            return Add(newdata, WordInBits);
        }

        /// <summary>
        /// Adding words directly to the bitmap (for expert use).
        /// </summary>
        /// <param name="newdata">the word</param>
        /// <param name="bitsthatmatter">the number of significant bits (by default it should be 64)</param>
        /// <returns>the number of words added to the buffer</returns>
        public int Add(long newdata, int bitsthatmatter)
        {
            SizeInBits += bitsthatmatter;
            if (newdata == 0)
            {
                return AddEmptyWord(false);
            }
            if (newdata == ~0L)
            {
                return AddEmptyWord(true);
            }
            return AddLiteralWord(newdata);
        }

        /// <summary>
        /// For experts: You want to add many
        /// zeroes or ones? This is the method you use.
        /// </summary>
        /// <param name="v">the bool value</param>
        /// <param name="number">the number</param>
        /// <returns>the number of words added to the buffer</returns>
        public int AddStreamOfEmptyWords(bool v, long number)
        {
            if (number == 0)
            {
                return 0;
            }
            bool noliteralword = (_Rlw.NumberOfLiteralWords == 0);
            long runlen = _Rlw.RunningLength;
            if ((noliteralword) && (runlen == 0))
            {
                _Rlw.RunningBit = v;
            }
            int wordsadded = 0;
            if ((noliteralword) && (_Rlw.RunningBit == v)
                && (runlen < RunningLengthWord.LargestRunningLengthCount))
            {
                long whatwecanadd = number < RunningLengthWord.LargestRunningLengthCount
                                    - runlen
                                        ? number
                                        : RunningLengthWord.LargestRunningLengthCount
                                          - runlen;
                _Rlw.RunningLength = runlen + whatwecanadd;
                SizeInBits += (int)whatwecanadd * WordInBits;
                if (number - whatwecanadd > 0)
                {
                    wordsadded += AddStreamOfEmptyWords(v, number - whatwecanadd);
                }
            }
            else
            {
                PushBack(0);
                ++wordsadded;
                _Rlw.Position = _ActualSizeInWords - 1;
                long whatwecanadd = number < RunningLengthWord.LargestRunningLengthCount
                                        ? number
                                        : RunningLengthWord.LargestRunningLengthCount;
                _Rlw.RunningBit = v;
                _Rlw.RunningLength = whatwecanadd;
                SizeInBits += (int)whatwecanadd * WordInBits;
                if (number - whatwecanadd > 0)
                {
                    wordsadded += AddStreamOfEmptyWords(v, number - whatwecanadd);
                }
            }
            return wordsadded;
        }

        /// <summary>
        /// Returns a new compressed bitmap containing the bitwise AND values of the
        /// current bitmap with some other bitmap.
        /// 
        /// The running time is proportional to the sum of the compressed sizes (as
        /// reported by <ref>SizeInBytes</ref>).
        /// 
        /// </summary>
        /// <param name="a">the other bitmap</param>
        /// <returns>the EWAH compressed bitmap</returns>
        public EwahCompressedBitArray And(EwahCompressedBitArray a)
        {
            var container = new EwahCompressedBitArray();
            container
                .Reserve(_ActualSizeInWords > a._ActualSizeInWords
                             ? _ActualSizeInWords
                             : a._ActualSizeInWords);
            EwahEnumerator i = a.GetEwahEnumerator();
            EwahEnumerator j = GetEwahEnumerator();
            if (!(i.HasNext() && j.HasNext()))
            {
                // this never happens...
                container.SizeInBits = SizeInBits;
                return container;
            }
            // at this point, this is safe:
            var rlwi = new BufferedRunningLengthWord(i.Next());
            var rlwj = new BufferedRunningLengthWord(j.Next());
            while (true)
            {
                bool iIsPrey = rlwi.Count < rlwj.Count;
                BufferedRunningLengthWord prey = iIsPrey ? rlwi : rlwj;
                BufferedRunningLengthWord predator = iIsPrey ? rlwj : rlwi;
                long predatorrl;
                long tobediscarded;
                if (prey.RunningBit == false)
                {
                    container.AddStreamOfEmptyWords(false, prey.RunningLength);
                    predator.DiscardFirstWords(prey.RunningLength);
                    prey.RunningLength = 0;
                }
                else
                {
                    // we have a stream of 1x11
                    predatorrl = predator.RunningLength;
                    long preyrl = prey.RunningLength;
                    tobediscarded = (predatorrl >= preyrl) ? preyrl : predatorrl;
                    container
                        .AddStreamOfEmptyWords(predator.RunningBit, tobediscarded);
                    int dwPredator = predator.DirtyWordOffset
                                     + (iIsPrey ? j.DirtyWords : i.DirtyWords);
                    container.AddStreamOfDirtyWords(iIsPrey ? j.Buffer : i.Buffer,
                                                    dwPredator,
                                                    preyrl - tobediscarded);
                    predator.DiscardFirstWords(preyrl);
                    prey.RunningLength = 0;
                }
                predatorrl = predator.RunningLength;
                long nbreDirtyPrey;
                if (predatorrl > 0)
                {
                    if (predator.RunningBit == false)
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                        container.AddStreamOfEmptyWords(false, tobediscarded);
                    }
                    else
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        int dwPrey = prey.DirtyWordOffset
                                     + (iIsPrey ? i.DirtyWords : j.DirtyWords);
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        container.AddStreamOfDirtyWords(iIsPrey ? i.Buffer : j.Buffer,
                                                        dwPrey,
                                                        tobediscarded);
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                    }
                }
                // all that is left to do now is to AND the dirty words
                nbreDirtyPrey = prey.NumberOfLiteralWords;
                if (nbreDirtyPrey > 0)
                {
                    for (int k = 0; k < nbreDirtyPrey; ++k)
                    {
                        if (iIsPrey)
                        {
                            container.Add(i.Buffer[prey.DirtyWordOffset + i.DirtyWords + k]
                                          & j.Buffer[predator.DirtyWordOffset + j.DirtyWords + k]);
                        }
                        else
                        {
                            container.Add(i.Buffer[predator.DirtyWordOffset + i.DirtyWords
                                                   + k]
                                          & j.Buffer[prey.DirtyWordOffset + j.DirtyWords + k]);
                        }
                    }
                    predator.DiscardFirstWords(nbreDirtyPrey);
                }
                if (iIsPrey)
                {
                    if (!i.HasNext())
                    {
                        rlwi = null;
                        break;
                    }
                    rlwi.Reset(i.Next());
                }
                else
                {
                    if (!j.HasNext())
                    {
                        rlwj = null;
                        break;
                    }
                    rlwj.Reset(j.Next());
                }
            }
            if (rlwi != null)
            {
                DischargeAsEmpty(rlwi, i, container);
            }
            if (rlwj != null)
            {
                DischargeAsEmpty(rlwj, j, container);
            }
            container.SizeInBits = Math.Max(SizeInBits, a.SizeInBits);
            return container;
        }

        /// <summary>
        /// Returns a new compressed bitmap containing the bitwise AND NOT values of
        /// the current bitmap with some other bitmap.
        /// 
        /// The running time is proportional to the sum of the compressed sizes (as 
        /// reported by <ref>SizeInBytes</ref>).
        /// </summary>
        /// <param name="a">the other bitmap</param>
        /// <returns>the EWAH compressed bitmap</returns>
        public EwahCompressedBitArray AndNot(EwahCompressedBitArray a)
        {
            var container = new EwahCompressedBitArray();
            container
                .Reserve(_ActualSizeInWords > a._ActualSizeInWords
                             ? _ActualSizeInWords
                             : a._ActualSizeInWords);
            EwahEnumerator i = a.GetEwahEnumerator();
            EwahEnumerator j = GetEwahEnumerator();
            if (!(i.HasNext() && j.HasNext()))
            {
                // this never happens...
                container.SizeInBits = SizeInBits;
                return container;
            }
            // at this point, this is safe:
            var rlwi = new BufferedRunningLengthWord(i.Next());
            rlwi.RunningBit = !rlwi.RunningBit;
            var rlwj = new BufferedRunningLengthWord(j.Next());
            while (true)
            {
                bool iIsPrey = rlwi.Count < rlwj.Count;
                BufferedRunningLengthWord prey = iIsPrey ? rlwi : rlwj;
                BufferedRunningLengthWord predator = iIsPrey ? rlwj : rlwi;

                long predatorrl;
                long tobediscarded;
                if (prey.RunningBit == false)
                {
                    container.AddStreamOfEmptyWords(false, prey.RunningLength);
                    predator.DiscardFirstWords(prey.RunningLength);
                    prey.RunningLength = 0;
                }
                else
                {
                    // we have a stream of 1x11
                    predatorrl = predator.RunningLength;
                    long preyrl = prey.RunningLength;
                    tobediscarded = (predatorrl >= preyrl) ? preyrl : predatorrl;
                    container
                        .AddStreamOfEmptyWords(predator.RunningBit, tobediscarded);
                    int dwPredator = predator.DirtyWordOffset
                                     + (iIsPrey ? j.DirtyWords : i.DirtyWords);
                    if (iIsPrey)
                    {
                        container.AddStreamOfDirtyWords(j.Buffer,
                                                        dwPredator,
                                                        preyrl
                                                        - tobediscarded);
                    }
                    else
                    {
                        container.AddStreamOfNegatedDirtyWords(i.Buffer,
                                                               dwPredator,
                                                               preyrl - tobediscarded);
                    }
                    predator.DiscardFirstWords(preyrl);
                    prey.RunningLength = 0;
                }
                predatorrl = predator.RunningLength;
                long nbreDirtyPrey;
                if (predatorrl > 0)
                {
                    if (predator.RunningBit == false)
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                        container.AddStreamOfEmptyWords(false, tobediscarded);
                    }
                    else
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        int dwPrey = prey.DirtyWordOffset
                                     + (iIsPrey ? i.DirtyWords : j.DirtyWords);
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        if (iIsPrey)
                        {
                            container.AddStreamOfNegatedDirtyWords(i.Buffer,
                                                                   dwPrey,
                                                                   tobediscarded);
                        }
                        else
                        {
                            container.AddStreamOfDirtyWords(j.Buffer, dwPrey, tobediscarded);
                        }
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                    }
                }
                // all that is left to do now is to AND the dirty words
                nbreDirtyPrey = prey.NumberOfLiteralWords;
                if (nbreDirtyPrey > 0)
                {
                    for (int k = 0; k < nbreDirtyPrey; ++k)
                    {
                        if (iIsPrey)
                        {
                            container.Add((~i.Buffer[prey.DirtyWordOffset + i.DirtyWords
                                                     + k])
                                          & j.Buffer[predator.DirtyWordOffset + j.DirtyWords + k]);
                        }
                        else
                        {
                            container.Add((~i.Buffer[predator.DirtyWordOffset
                                                     + i.DirtyWords + k])
                                          & j.Buffer[prey.DirtyWordOffset + j.DirtyWords + k]);
                        }
                    }
                    predator.DiscardFirstWords(nbreDirtyPrey);
                }
                if (iIsPrey)
                {
                    if (!i.HasNext())
                    {
                        rlwi = null;
                        break;
                    }
                    rlwi.Reset(i.Next());
                    rlwi.RunningBit = !rlwi.RunningBit;
                }
                else
                {
                    if (!j.HasNext())
                    {
                        rlwj = null;
                        break;
                    }
                    rlwj.Reset(j.Next());
                }
            }
            if (rlwi != null)
            {
                DischargeAsEmpty(rlwi, i, container);
            }
            if (rlwj != null)
            {
                Discharge(rlwj, j, container);
            }
            container.SizeInBits = Math.Max(SizeInBits, a.SizeInBits);
            return container;
        }

        /// <summary>
        /// reports the number of bits set to true. Running time is proportional to
        /// compressed size (as reported by <ref>SizeInBytes</ref>).
        /// </summary>
        /// <returns>the number of bits set to true</returns>
        public ulong GetCardinality()
        {
            ulong counter = 0;
            var i = new EwahEnumerator(_Buffer, _ActualSizeInWords);
            while (i.HasNext())
            {
                RunningLengthWord localrlw = i.Next();

                if (localrlw.RunningBit)
                {
                    counter += (ulong)(WordInBits * localrlw.RunningLength);
                }
                for (int j = 0; j < localrlw.NumberOfLiteralWords; ++j)
                {
                    long data = i.Buffer[i.DirtyWords + j];
                    counter += BitCount((ulong)data);
                }
            }
            return counter;
        }

        /// <summary>
        /// get the locations of the true values as one vector. (may use more memory
        /// than <ref>GetEnumerator()</ref>
        /// 
        /// 
        /// </summary>
        /// <returns></returns>
        public List<int> GetPositions()
        {
            var v = new List<int>();
            var i = new EwahEnumerator(_Buffer, _ActualSizeInWords);
            int pos = 0;
            while (i.HasNext())
            {
                RunningLengthWord localrlw = i.Next();
                if (localrlw.RunningBit)
                {
                    for (int j = 0; j < localrlw.RunningLength; ++j)
                    {
                        for (int c = 0; c < WordInBits; ++c)
                        {
                            v.Add(pos++);
                        }
                    }
                }
                else
                {
                    pos += WordInBits * (int)localrlw.RunningLength;
                }

                for (int j = 0; j < localrlw.NumberOfLiteralWords; ++j)
                {
                    long data = i.Buffer[i.DirtyWords + j];
                    for (int c = 0; c < WordInBits; ++c)
                    {
                        if (((1L << c) & data) != 0)
                        {
                            v.Add(pos);
                        }
                        ++pos;
                    }
                }
            }
            while ((v.Count > 0)
                   && (v[v.Count - 1] >= SizeInBits))
            {
                v.Remove(v.Count - 1);
            }
            return v;
        }

        /// <summary>
        /// Return true if the two EwahCompressedBitArray have both at least one
        /// true bit in the same Position. Equivalently, you could call "And"
        /// and check whether there is a set bit, but intersects will run faster
        /// if you don't need the result of the "and" operation.
        /// </summary>
        /// <param name="a"></param>
        /// <returns></returns>
        public bool Intersects(EwahCompressedBitArray a)
        {
            EwahEnumerator i = a.GetEwahEnumerator();
            EwahEnumerator j = GetEwahEnumerator();
            if (!(i.HasNext() && j.HasNext()))
            {
                return false;
            }
            // at this point, this is safe:
            var rlwi = new BufferedRunningLengthWord(i.Next());
            var rlwj = new BufferedRunningLengthWord(j.Next());
            while (true)
            {
                bool iIsPrey = rlwi.Count < rlwj.Count;
                BufferedRunningLengthWord prey = iIsPrey ? rlwi : rlwj;
                BufferedRunningLengthWord predator = iIsPrey ? rlwj : rlwi;
                long predatorrl;
                long tobediscarded;
                if (prey.RunningBit == false)
                {
                    predator.DiscardFirstWords(prey.RunningLength);
                    prey.RunningLength = 0;
                }
                else
                {
                    // we have a stream of 1x11
                    predatorrl = predator.RunningLength;
                    long preyrl = prey.RunningLength;
                    tobediscarded = (predatorrl >= preyrl) ? preyrl : predatorrl;
                    if (predator.RunningBit) return true;
                    if (preyrl - tobediscarded > 0) return true;
                    predator.DiscardFirstWords(preyrl);
                    prey.RunningLength = 0;
                }
                predatorrl = predator.RunningLength;
                long nbreDirtyPrey;
                if (predatorrl > 0)
                {
                    if (predator.RunningBit == false)
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                    }
                    else
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        if (tobediscarded > 0) return true;
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                    }
                }
                // all that is left to do now is to AND the dirty words
                nbreDirtyPrey = prey.NumberOfLiteralWords;

                if (nbreDirtyPrey > 0)
                {
                    for (int k = 0; k < nbreDirtyPrey; ++k)
                    {
                        if (iIsPrey)
                        {
                            if ((i.Buffer[prey.DirtyWordOffset + i.DirtyWords + k]
                                          & j.Buffer[predator.DirtyWordOffset + j.DirtyWords + k]) != 0)
                                return true;
                        }
                        else
                        {
                            if ((i.Buffer[predator.DirtyWordOffset + i.DirtyWords
                                                   + k]
                                          & j.Buffer[prey.DirtyWordOffset + j.DirtyWords + k]) != 0)
                                return true;
                        }
                    }
                    predator.DiscardFirstWords(nbreDirtyPrey);
                }
                if (iIsPrey)
                {
                    if (!i.HasNext())
                    {
                        rlwi = null;
                        break;
                    }
                    rlwi.Reset(i.Next());
                }
                else
                {
                    if (!j.HasNext())
                    {
                        rlwj = null;
                        break;
                    }
                    rlwj.Reset(j.Next());
                }
            }
            return false;
        }

        /// <summary>
        ///  Negate (bitwise) the current bitmap. To get a negated copy, do
        ///  ((EwahCompressedBitArray) mybitmap.Clone()).not();
        ///  
        ///  The running time is proportional to the compressed size (as reported by
        ///  <ref>SizeInBytes</ref>).
        /// </summary>
        public void Not()
        {

            var i = new EwahEnumerator(_Buffer, _ActualSizeInWords);
            if (!i.HasNext())
            {
                return;
            }
            while (true)
            {

                RunningLengthWord rlw1 = i.Next();

                rlw1.RunningBit = !rlw1.RunningBit;
                for (int j = 0; j < rlw1.NumberOfLiteralWords; ++j)
                {
                    i.Buffer[i.DirtyWords + j] = ~i.Buffer[i.DirtyWords + j];
                }
                if (!i.HasNext())
                {

                    int usedbitsinlast = SizeInBits % WordInBits;

                    if (usedbitsinlast == 0)
                        return;

                    if (rlw1.NumberOfLiteralWords == 0)
                    {
                        if ((rlw1.RunningLength > 0) && (rlw1.RunningBit))
                        {
                            rlw1.RunningLength = rlw1.RunningLength - 1;
                            AddLiteralWord((long)((~0UL) >> (WordInBits - usedbitsinlast)));
                        }
                        return;
                    }
                    i.Buffer[i.DirtyWords + rlw1.NumberOfLiteralWords - 1] &= (long)((~0UL) >>
                                                                               (WordInBits - usedbitsinlast));

                    return;
                }

            }
        }

        /// <summary>
        /// Returns a new compressed bitmap containing the bitwise OR values of the
        /// current bitmap with some other bitmap.
        /// 
        /// The running time is proportional to the sum of the compressed sizes (as
        /// reported by <ref>SizeInBytes</ref>).
        /// </summary>
        /// <param name="a">the other bitmap</param>
        /// <returns>the EWAH compressed bitmap</returns>
        public EwahCompressedBitArray Or(EwahCompressedBitArray a)
        {
            var container = new EwahCompressedBitArray();
            container.Reserve(_ActualSizeInWords + a._ActualSizeInWords);
            EwahEnumerator i = a.GetEwahEnumerator();
            EwahEnumerator j = GetEwahEnumerator();
            if (!(i.HasNext() && j.HasNext()))
            {
                // this never happens...
                container.SizeInBits = SizeInBits;
                return container;
            }
            // at this point, this is safe:
            var rlwi = new BufferedRunningLengthWord(i.Next());
            var rlwj = new BufferedRunningLengthWord(j.Next());
            // RunningLength;
            while (true)
            {
                bool iIsPrey = rlwi.Count < rlwj.Count;
                BufferedRunningLengthWord prey = iIsPrey ? rlwi : rlwj;
                BufferedRunningLengthWord predator = iIsPrey ? rlwj : rlwi;
                long predatorrl;
                long tobediscarded;
                if (prey.RunningBit == false)
                {
                    predatorrl = predator.RunningLength;
                    long preyrl = prey.RunningLength;
                    tobediscarded = (predatorrl >= preyrl) ? preyrl : predatorrl;
                    container
                        .AddStreamOfEmptyWords(predator.RunningBit, tobediscarded);
                    long dwPredator = predator.DirtyWordOffset
                                      + (iIsPrey ? j.DirtyWords : i.DirtyWords);
                    container.AddStreamOfDirtyWords(iIsPrey ? j.Buffer : i.Buffer,
                                                    dwPredator,
                                                    preyrl - tobediscarded);
                    predator.DiscardFirstWords(preyrl);
                    prey.DiscardFirstWords(preyrl);
                    prey.RunningLength = 0;
                }
                else
                {
                    // we have a stream of 1x11
                    container.AddStreamOfEmptyWords(true, prey.RunningLength);
                    predator.DiscardFirstWords(prey.RunningLength);
                    prey.RunningLength = 0;
                }
                predatorrl = predator.RunningLength;
                long nbreDirtyPrey;
                if (predatorrl > 0)
                {
                    if (predator.RunningBit == false)
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        long dwPrey = prey.DirtyWordOffset
                                      + (iIsPrey ? i.DirtyWords : j.DirtyWords);
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                        container.AddStreamOfDirtyWords(iIsPrey ? i.Buffer : j.Buffer,
                                                        dwPrey,
                                                        tobediscarded);
                    }
                    else
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        container.AddStreamOfEmptyWords(true, tobediscarded);
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                    }
                }
                // all that is left to do now is to OR the dirty words
                nbreDirtyPrey = prey.NumberOfLiteralWords;
                if (nbreDirtyPrey > 0)
                {
                    for (int k = 0; k < nbreDirtyPrey; ++k)
                    {
                        if (iIsPrey)
                        {
                            container.Add(i.Buffer[prey.DirtyWordOffset + i.DirtyWords + k]
                                          | j.Buffer[predator.DirtyWordOffset + j.DirtyWords + k]);
                        }
                        else
                        {
                            container.Add(i.Buffer[predator.DirtyWordOffset + i.DirtyWords
                                                   + k]
                                          | j.Buffer[prey.DirtyWordOffset + j.DirtyWords + k]);
                        }
                    }
                    predator.DiscardFirstWords(nbreDirtyPrey);
                }
                if (iIsPrey)
                {
                    if (!i.HasNext())
                    {
                        rlwi = null;
                        break;
                    }
                    rlwi.Reset(i.Next()); // = new
                    // BufferedRunningLengthWord(i.Next());
                }
                else
                {
                    if (!j.HasNext())
                    {
                        rlwj = null;
                        break;
                    }
                    rlwj.Reset(j.Next()); // = new
                    // BufferedRunningLengthWord(
                    // j.Next());
                }
            }
            if (rlwi != null)
            {
                Discharge(rlwi, i, container);
            }
            if (rlwj != null)
            {
                Discharge(rlwj, j, container);
            }
            container.SizeInBits = Math.Max(SizeInBits, a.SizeInBits);
            return container;
        }

        /// <summary>
        /// set the bit at Position i to true, the bits must be set in increasing
        /// order. For example, Set(15) and then Set(7) will fail. You must do Set(7)
        /// and then Set(15).
        /// </summary>
        /// <param name="i">the index</param>
        /// <returns>true if the value was set (always true when i>= SizeInBits)</returns>
        public bool Set(int i)
        {
            if (i < SizeInBits)
            {
                return false;
            }
            // must I complete a word?
            if ((SizeInBits % WordInBits) != 0)
            {
                int possiblesizeinbits = (SizeInBits / WordInBits) * WordInBits + WordInBits;
                if (possiblesizeinbits < i + 1)
                {
                    SizeInBits = possiblesizeinbits;
                }
            }
            AddStreamOfEmptyWords(false, (i / WordInBits) - SizeInBits / WordInBits);
            int bittoflip = i - (SizeInBits / WordInBits * WordInBits);
            // next, we set the bit
            if ((_Rlw.NumberOfLiteralWords == 0)
                || ((SizeInBits - 1) / WordInBits < i / WordInBits))
            {
                long newdata = 1L << bittoflip;
                AddLiteralWord(newdata);
            }
            else
            {
                _Buffer[_ActualSizeInWords - 1] |= 1L << bittoflip;
                // check if we just completed a stream of 1s
                if (_Buffer[_ActualSizeInWords - 1] == ~0L)
                {
                    // we remove the last dirty word
                    _Buffer[_ActualSizeInWords - 1] = 0L;
                    --_ActualSizeInWords;
                    _Rlw
                        .NumberOfLiteralWords = _Rlw.NumberOfLiteralWords - 1;
                    // next we add one clean word
                    AddEmptyWord(true);
                }
            }
            SizeInBits = i + 1;
            return true;
        }

        /// <summary>
        /// Change the reported size in bits of the *uncompressed* bitmap represented
        /// by this compressed bitmap. It is not possible to reduce the SizeInBits, but
        /// it can be extended. The new bits are set to false or true depending on the
        /// value of defaultvalue. 
        /// </summary>
        /// <param name="size">the size in bits</param>
        /// <param name="defaultvalue">the default bool value</param>
        /// <returns>true if the update was possible</returns>
        public bool SetSizeInBits(int size, bool defaultvalue)
        {
            if (size < SizeInBits)
            {
                return false;
            }
            if (defaultvalue == false)
            {

                int currentLeftover = SizeInBits % WordInBits;
                int finalLeftover = size % WordInBits;
                AddStreamOfEmptyWords(false, (size / WordInBits) - SizeInBits
                  / WordInBits + (finalLeftover != 0 ? 1 : 0)
                  + (currentLeftover != 0 ? -1 : 0));
            }
            else
            {
                // next bit could be optimized
                while (((SizeInBits % WordInBits) != 0) && (SizeInBits < size))
                {
                    Set(SizeInBits);
                }

                AddStreamOfEmptyWords(defaultvalue, (size / WordInBits)
                  - SizeInBits / WordInBits);

                // next bit could be optimized
                while (SizeInBits < size)
                {
                    Set(SizeInBits);
                }
            }
            SizeInBits = size;
            return true;
        }
        /// <summary>
        /// Sets the internal buffer to the minimum possible size required to contain
        /// the current bitarray.
        ///
        /// This method is useful when dealing with static bitmasks, if it is called
        /// after the final bit has been set, some memory can be free-ed.
        ///
        /// Please note, the next bit set after a call to shrink will cause the memory
        /// usage of the bit-array to double.
        /// </summary>
        public void Shrink()
        {
            Array.Resize(ref _Buffer, _ActualSizeInWords);
            _Rlw.ArrayOfWords = _Buffer;
        }

        /// <summary>
        /// A more detailed string describing the bitmap (useful for debugging).
        /// </summary>
        /// <returns>detailed debug string</returns>
        public string ToDebugString()
        {
            string ans = " EwahCompressedBitArray, size in bits = " + SizeInBits
                         + " size in words = " + _ActualSizeInWords + "\n";
            var i = new EwahEnumerator(_Buffer, _ActualSizeInWords);
            while (i.HasNext())
            {
                RunningLengthWord localrlw = i.Next();
                if (localrlw.RunningBit)
                {
                    ans += localrlw.RunningLength + " 1x11\n";
                }
                else
                {
                    ans += localrlw.RunningLength + " 0x00\n";
                }
                ans += localrlw.NumberOfLiteralWords + " dirties\n";
                for (int j = 0; j < localrlw.NumberOfLiteralWords; ++j)
                {
                    long data = i.Buffer[i.DirtyWords + j];
                    ans += "\t" + data + "\n";
                }
            }
            return ans;
        }




        /// <summary>
        /// Returns a new compressed bitmap containing the bitwise XOR values of the
        /// current bitmap with some other bitmap.
        /// 
        /// The running time is proportional to the sum of the compressed sizes (as
        /// reported by <ref>SizeInBytes</ref>).
        /// 
        /// </summary>
        /// <param name="a">the other bitmap</param>
        /// <returns>the EWAH compressed bitmap</returns>
        public EwahCompressedBitArray Xor(EwahCompressedBitArray a)
        {
            var container = new EwahCompressedBitArray();
            container.Reserve(_ActualSizeInWords + a._ActualSizeInWords);
            EwahEnumerator i = a.GetEwahEnumerator();
            EwahEnumerator j = GetEwahEnumerator();
            if (!(i.HasNext() && j.HasNext()))
            {
                // this never happens...
                container.SizeInBits = SizeInBits;
                return container;
            }
            // at this point, this is safe:
            var rlwi = new BufferedRunningLengthWord(i.Next());
            var rlwj = new BufferedRunningLengthWord(j.Next());
            while (true)
            {
                bool iIsPrey = rlwi.Count < rlwj.Count;
                BufferedRunningLengthWord prey = iIsPrey ? rlwi : rlwj;
                BufferedRunningLengthWord predator = iIsPrey ? rlwj : rlwi;
                long predatorrl;
                long preyrl;
                long tobediscarded;

                if (prey.RunningBit == false)
                {
                    predatorrl = predator.RunningLength;
                    preyrl = prey.RunningLength;
                    tobediscarded = (predatorrl >= preyrl) ? preyrl : predatorrl;
                    container
                        .AddStreamOfEmptyWords(predator.RunningBit, tobediscarded);
                    long dwPredator = predator.DirtyWordOffset
                                      + (iIsPrey ? j.DirtyWords : i.DirtyWords);
                    container.AddStreamOfDirtyWords(iIsPrey ? j.Buffer : i.Buffer,
                                                    dwPredator,
                                                    preyrl - tobediscarded);
                    predator.DiscardFirstWords(preyrl);
                    prey.DiscardFirstWords(preyrl);
                }
                else
                {
                    // we have a stream of 1x11
                    predatorrl = predator.RunningLength;
                    preyrl = prey.RunningLength;
                    tobediscarded = (predatorrl >= preyrl) ? preyrl : predatorrl;
                    container.AddStreamOfEmptyWords(!predator.RunningBit,
                                                    tobediscarded);
                    int dwPredator = predator.DirtyWordOffset
                                     + (iIsPrey ? j.DirtyWords : i.DirtyWords);
                    long[] buf = iIsPrey ? j.Buffer : i.Buffer;
                    for (int k = 0; k < preyrl - tobediscarded; ++k)
                    {
                        container.Add(~buf[k + dwPredator]);
                    }
                    predator.DiscardFirstWords(preyrl);
                    prey.DiscardFirstWords(preyrl);
                }
                predatorrl = predator.RunningLength;
                long nbreDirtyPrey;
                if (predatorrl > 0)
                {
                    if (predator.RunningBit == false)
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        long dwPrey = prey.DirtyWordOffset
                                      + (iIsPrey ? i.DirtyWords : j.DirtyWords);
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                        container.AddStreamOfDirtyWords(iIsPrey ? i.Buffer : j.Buffer,
                                                        dwPrey,
                                                        tobediscarded);
                    }
                    else
                    {
                        nbreDirtyPrey = prey.NumberOfLiteralWords;
                        tobediscarded = (predatorrl >= nbreDirtyPrey)
                                            ? nbreDirtyPrey
                                            : predatorrl;
                        int dwPrey = prey.DirtyWordOffset
                                     + (iIsPrey ? i.DirtyWords : j.DirtyWords);
                        predator.DiscardFirstWords(tobediscarded);
                        prey.DiscardFirstWords(tobediscarded);
                        long[] buf = iIsPrey ? i.Buffer : j.Buffer;
                        for (int k = 0; k < tobediscarded; ++k)
                        {
                            container.Add(~buf[k + dwPrey]);
                        }
                    }
                }
                // all that is left to do now is to AND the dirty words
                nbreDirtyPrey = prey.NumberOfLiteralWords;
                if (nbreDirtyPrey > 0)
                {
                    for (int k = 0; k < nbreDirtyPrey; ++k)
                    {
                        if (iIsPrey)
                        {
                            container.Add(i.Buffer[prey.DirtyWordOffset + i.DirtyWords + k]
                                          ^ j.Buffer[predator.DirtyWordOffset + j.DirtyWords + k]);
                        }
                        else
                        {
                            container.Add(i.Buffer[predator.DirtyWordOffset + i.DirtyWords
                                                   + k]
                                          ^ j.Buffer[prey.DirtyWordOffset + j.DirtyWords + k]);
                        }
                    }
                    predator.DiscardFirstWords(nbreDirtyPrey);
                }
                if (iIsPrey)
                {
                    if (!i.HasNext())
                    {
                        rlwi = null;
                        break;
                    }
                    rlwi.Reset(i.Next());
                }
                else
                {
                    if (!j.HasNext())
                    {
                        rlwj = null;
                        break;
                    }
                    rlwj.Reset(j.Next());
                }
            }
            if (rlwi != null)
            {
                Discharge(rlwi, i, container);
            }
            if (rlwj != null)
            {
                Discharge(rlwj, j, container);
            }
            container.SizeInBits = Math.Max(SizeInBits, a.SizeInBits);
            return container;
        }

        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="v">the bool value</param>
        /// <returns>the storage cost of the addition</returns>
        private int AddEmptyWord(bool v)
        {
            bool noliteralword = (_Rlw.NumberOfLiteralWords == 0);
            long runlen = _Rlw.RunningLength;
            if ((noliteralword) && (runlen == 0))
            {
                _Rlw.RunningBit = v;
            }
            if ((noliteralword) && (_Rlw.RunningBit == v)
                && (runlen < RunningLengthWord.LargestRunningLengthCount))
            {
                _Rlw.RunningLength = runlen + 1;
                return 0;
            }
            PushBack(0);
            _Rlw.Position = _ActualSizeInWords - 1;
            _Rlw.RunningBit = v;
            _Rlw.RunningLength = 1;
            return 1;
        }

        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="newdata">the dirty word</param>
        /// <returns>the storage cost of the addition</returns>
        private int AddLiteralWord(long newdata)
        {
            long numbersofar = _Rlw.NumberOfLiteralWords;
            if (numbersofar >= RunningLengthWord.LargestLiteralCount)
            {
                PushBack(0);
                _Rlw.Position = _ActualSizeInWords - 1;
                _Rlw.NumberOfLiteralWords = 1;
                PushBack(newdata);
                return 2;
            }
            _Rlw.NumberOfLiteralWords = (int)numbersofar + 1;
            PushBack(newdata);
            return 1;
        }

        /// <summary>
        /// if you have several dirty words to copy over, this might be faster.
        /// </summary>
        /// <param name="data">the dirty words</param>
        /// <param name="start">the starting point in the array</param>
        /// <param name="number">the number of dirty words to add</param>
        /// <returns>how many (compressed) words were added to the bitmap</returns>
        private long AddStreamOfDirtyWords(long[] data,
                                           long start,
                                           long number)
        {
            if (number == 0)
            {
                return 0;
            }
            long numberOfLiteralWords = _Rlw.NumberOfLiteralWords;
            long whatwecanadd = number < RunningLengthWord.LargestLiteralCount
                                - numberOfLiteralWords
                                    ? number
                                    : RunningLengthWord.LargestLiteralCount
                                      - numberOfLiteralWords;
            _Rlw.NumberOfLiteralWords = (int)(numberOfLiteralWords + whatwecanadd);
            long leftovernumber = number - whatwecanadd;
            PushBack(data, (int)start, (int)whatwecanadd);
            SizeInBits += (int)whatwecanadd * WordInBits;
            long wordsadded = whatwecanadd;
            if (leftovernumber > 0)
            {
                PushBack(0);
                _Rlw.Position = _ActualSizeInWords - 1;
                ++wordsadded;
                wordsadded += AddStreamOfDirtyWords(data,
                                                    start + whatwecanadd,
                                                    leftovernumber);
            }
            return wordsadded;
        }

        /// <summary>
        /// Same as <ref>addStreamOfDirtyWords</ref>, but the words are negated.
        /// </summary>
        /// <param name="data">the dirty words</param>
        /// <param name="start">start the starting point in the array</param>
        /// <param name="number">the number of dirty words to add</param>
        /// <returns>how many (compressed) words were added to the bitmap</returns>
        private long AddStreamOfNegatedDirtyWords(long[] data,
                                                  long start,
                                                  long number)
        {
            if (number == 0)
            {
                return 0;
            }
            long numberOfLiteralWords = _Rlw.NumberOfLiteralWords;
            long whatwecanadd = number < RunningLengthWord.LargestLiteralCount
                                - numberOfLiteralWords
                                    ? number
                                    : RunningLengthWord.LargestLiteralCount
                                      - numberOfLiteralWords;
            _Rlw.NumberOfLiteralWords = (int)(numberOfLiteralWords + whatwecanadd);
            long leftovernumber = number - whatwecanadd;
            NegativePushBack(data, (int)start, (int)whatwecanadd);
            SizeInBits += (int)whatwecanadd * WordInBits;
            long wordsadded = whatwecanadd;
            if (leftovernumber > 0)
            {
                PushBack(0);
                _Rlw.Position = _ActualSizeInWords - 1;
                ++wordsadded;
                wordsadded += AddStreamOfNegatedDirtyWords(data,
                                                    start + whatwecanadd,
                                                    leftovernumber);
            }
            return wordsadded;
        }

        /// <summary>
        /// Gets an EwahEnumerator over the data. This is a customized
        /// enumerator which iterates over run length word. For experts only. 
        /// </summary>
        /// <returns>the EwahEnumerator</returns>
        private EwahEnumerator GetEwahEnumerator()
        {
            return new EwahEnumerator(_Buffer, _ActualSizeInWords);
        }

        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="data">the array of words to be added</param>
        /// <param name="start">the starting point</param>
        /// <param name="number">the number of words to add</param>
        private void NegativePushBack(long[] data,
                                      int start,
                                      int number)
        {
            while (_ActualSizeInWords + number >= _Buffer.Length)
            {
                Array.Resize(ref _Buffer, _Buffer.Length * 2);
                _Rlw.ArrayOfWords = _Buffer;
            }
            for (int k = 0; k < number; ++k)
            {
                _Buffer[_ActualSizeInWords + k] = ~data[start + k];
            }
            _ActualSizeInWords += number;
        }

        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="data">the word to be added</param>
        private void PushBack(long data)
        {
            if (_ActualSizeInWords == _Buffer.Length)
            {
                Array.Resize(ref _Buffer, _Buffer.Length * 2);
                _Rlw.ArrayOfWords = _Buffer;
            }
            _Buffer[_ActualSizeInWords++] = data;
        }

        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="data">the array of words to be added</param>
        /// <param name="start">the starting point</param>
        /// <param name="number">the number of words to add</param>
        private void PushBack(long[] data, int start, int number)
        {
            while (_ActualSizeInWords + number >= _Buffer.Length)
            {
                Array.Resize(ref _Buffer, _Buffer.Length * 2);
                _Rlw.ArrayOfWords = _Buffer;
            }
            Array.Copy(data, start, _Buffer, _ActualSizeInWords, number);
            _ActualSizeInWords += number;
        }

        /// <summary>
        /// For internal use (trading off memory for speed).
        /// </summary>
        /// <param name="size">the number of words to allocate</param>
        /// <returns>True if the operation was a success</returns>
        private bool Reserve(int size)
        {
            if (size > _Buffer.Length)
            {
                Array.Resize(ref _Buffer, size);
                _Rlw.ArrayOfWords = _Buffer;
                return true;
            }
            return false;
        }

        #endregion

        #region IEnumerable<int> Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Iterator over the set bits (this is what most people will want to use to
        /// browse the content). The location of the set bits is returned, in 
        /// increasing order.
        /// </summary>
        /// <returns>the int enumerator</returns>
        public IEnumerator<int> GetEnumerator()
        {
            return new IntIteratorImpl(new EwahEnumerator(_Buffer, _ActualSizeInWords), WordInBits);
        }

        #endregion

        #region Class Methods

        public static EwahCompressedBitArray BitmapOf(params int[] setbits)
        {
            EwahCompressedBitArray a = new EwahCompressedBitArray();
            foreach (int k in setbits)
                a.Set(k);
            return a;
        }
        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="initialWord">the initial word</param>
        /// <param name="enumerator">the enumerator</param>
        /// <param name="container">the container</param>
        private static void Discharge(BufferedRunningLengthWord initialWord,
                                      EwahEnumerator enumerator,
                                      EwahCompressedBitArray container)
        {
            BufferedRunningLengthWord runningLengthWord = initialWord;
            for (; ;)
            {
                long runningLength = runningLengthWord.RunningLength;
                container.AddStreamOfEmptyWords(runningLengthWord.RunningBit,
                                                runningLength);
                container.AddStreamOfDirtyWords(enumerator.Buffer,
                                                enumerator.DirtyWords
                                                + runningLengthWord.DirtyWordOffset,
                                                runningLengthWord.NumberOfLiteralWords);
                if (!enumerator.HasNext())
                {
                    break;
                }
                runningLengthWord = new BufferedRunningLengthWord(enumerator.Next());
            }
        }

        /// <summary>
        /// Counts the number of set (1) bits.
        /// </summary>
        /// <param name="v">the value to be processed</param>        
        public static UInt64 BitCount(UInt64 v)
        {
            const UInt64 MaskMult = 0x0101010101010101;
            const UInt64 mask1h = (~0UL) / 3 << 1;
            const UInt64 mask2l = (~0UL) / 5;
            const UInt64 mask4l = (~0UL) / 17;
            v -= (mask1h & v) >> 1;
            v = (v & mask2l) + ((v >> 2) & mask2l);
            v += v >> 4;
            v &= mask4l;
            return (v * MaskMult) >> 56;
        }

        /// <summary>
        /// For internal use.
        /// </summary>
        /// <param name="initialWord">the initial word</param>
        /// <param name="enumerator">the enumerator</param>
        /// <param name="container">the container</param>
        private static void DischargeAsEmpty(BufferedRunningLengthWord initialWord,
                                             EwahEnumerator enumerator,
                                             EwahCompressedBitArray container)
        {
            BufferedRunningLengthWord runningLengthWord = initialWord;
            for (; ;)
            {
                long runningLength = runningLengthWord.RunningLength;
                container.AddStreamOfEmptyWords(false,
                                                runningLength + runningLengthWord.NumberOfLiteralWords);
                if (!enumerator.HasNext())
                {
                    break;
                }
                runningLengthWord = new BufferedRunningLengthWord(enumerator.Next());
            }
        }

        #endregion

        #region Nested type: IntIteratorImpl

        private sealed class IntIteratorImpl : IEnumerator<int>
        {
            #region Constants

            private const int InitCapacity = 512;

            #endregion

            #region Readonly & Static Fields

            private readonly EwahEnumerator _EwahEnumerator;
            private readonly int _WordInBits;

            #endregion

            #region Fields

            private int _BufferPos;
            private int _Current = -1;
            private int[] _LocalBuffer = new int[InitCapacity];
            private int _LocalBufferSize;
            private RunningLengthWord _LocalRlw;
            private int _Pos;

            #endregion

            #region C'tors

            public IntIteratorImpl(EwahEnumerator ewahEnumerator, int wordInBits)
            {
                _EwahEnumerator = ewahEnumerator;
                _WordInBits = wordInBits;
            }

            #endregion

            #region Instance Methods

            private void Add(int val)
            {
                ++_LocalBufferSize;
                while (_LocalBufferSize > _LocalBuffer.Length)
                {
                    Array.Resize(ref _LocalBuffer, _LocalBuffer.Length * 2);
                }
                _LocalBuffer[_LocalBufferSize - 1] = val;
            }

            private bool HasNext()
            {
                while (_LocalBufferSize == 0)
                {
                    if (!LoadNextRle())
                    {
                        return false;
                    }
                    LoadBuffer();
                }
                return true;
            }

            private void LoadBuffer()
            {
                _BufferPos = 0;
                _LocalBufferSize = 0;
                if (_LocalRlw.RunningBit)
                {
                    for (int j = 0; j < _LocalRlw.RunningLength; ++j)
                    {
                        for (int c = 0; c < _WordInBits; ++c)
                        {
                            Add(_Pos++);
                        }
                    }
                }
                else
                {
                    _Pos += (int)(_WordInBits * _LocalRlw.RunningLength);
                }
                for (int j = 0; j < _LocalRlw.NumberOfLiteralWords; ++j)
                {
                    long data = _EwahEnumerator.Buffer[_EwahEnumerator.DirtyWords + j];
                    for (int c = 0; c < _WordInBits; ++c)
                    {
                        if (((1L << c) & data) != 0)
                        {
                            Add(_Pos);
                        }
                        ++_Pos;
                    }
                }
            }

            private bool LoadNextRle()
            {
                while (_EwahEnumerator.HasNext())
                {
                    _LocalRlw = _EwahEnumerator.Next();
                    return true;
                }
                return false;
            }

            private int Next()
            {
                int answer = _LocalBuffer[_BufferPos++];
                if (_LocalBufferSize == _BufferPos)
                {
                    _LocalBufferSize = 0;
                }
                return answer;
            }

            #endregion

            #region IEnumerator<int> Members

            public int Current
            {
                get { return _Current; }
            }

            public void Dispose()
            {
                //noop;
            }

            object IEnumerator.Current
            {
                get { return _Current; }
            }

            public bool MoveNext()
            {
                bool res = HasNext();
                if (!res)
                {
                    _Current = -1;
                    return false;
                }
                _Current = Next();
                return true;
            }

            public void Reset()
            {
                _EwahEnumerator.Reset();
                _Current = -1;
                _Pos = 0;
                _LocalRlw = null;
                _LocalBuffer = new int[InitCapacity];
                _LocalBufferSize = 0;
                _BufferPos = 0;
            }

            #endregion
        }

        #endregion
    }
}
