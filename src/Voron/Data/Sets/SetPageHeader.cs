using System.Runtime.InteropServices;

namespace Voron.Data.Sets
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct SetPageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort Lower;

        [FieldOffset(10)]
        public ushort Upper;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public SetFlags PageFlags;

        [FieldOffset(14)]
        public ushort FreeSpace;

        public int NumberOfEntries
        {
            get
            {
                return (Lower - PageHeader.SizeOf) / sizeof(short);
            }
        }
    }
}
