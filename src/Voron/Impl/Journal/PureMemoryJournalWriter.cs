using System;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe class PureMemoryJournalWriter : IJournalWriter
    {
        private readonly string _name;
        private long _journalSize;
        private int _refs;
        private byte* _ptr;

        public override string ToString() => _name;

        public PureMemoryJournalWriter(string name, long journalSize)
        {
            _name = name;
            _journalSize = journalSize;
            _ptr = (byte*)Marshal.AllocHGlobal((IntPtr)_journalSize);
            NumberOfAllocated4Kb = (int)(journalSize / (4 * Constants.Size.Kilobyte));
        }

        public void AddRef()
        {
            Interlocked.Increment(ref _refs);
        }

        public bool Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return false;

            Dispose();
            return true;
        }


        public void Dispose()
        {
            if (_ptr == null)
                return;
            Marshal.FreeHGlobal((IntPtr)_ptr);
            _ptr = null;
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            if ((posBy4Kb + numberOf4Kb) * 4 * Constants.Size.Kilobyte > _journalSize)
                throw new IndexOutOfRangeException("Cannot write to " + posBy4Kb * 4 + " kb because it is after the file end");

            Memory.Copy(_ptr + posBy4Kb * 4 * Constants.Size.Kilobyte, p, numberOf4Kb * 4 * Constants.Size.Kilobyte);
        }

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed => _ptr == null;
        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            throw new NotImplementedException();
        }

        public bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            if (offsetInFile + numOfBytes > _journalSize)
                return false;

            Memory.Copy(buffer, _ptr + offsetInFile, numOfBytes);

            return true;
        }

        public void Truncate(long size)
        {
            _journalSize = size;
        }
    }
}