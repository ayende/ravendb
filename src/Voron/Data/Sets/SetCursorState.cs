using System;
using System.Text;

namespace Voron.Data.Sets
{
    public unsafe struct SetCursorState
    {
        public Page Page;
        public int LastMatch;
        public int LastSearchPosition;
        public SetPageHeader* Header => (SetPageHeader*)Page.Pointer;
      
        public Span<byte> Prefix
        {
            get
            {
                var p = Page.Pointer + PageHeader.SizeOf;
                return new Span<byte>(p + 1, p[0]);
            }
        }

        public string DumpPageDebug()
        {
            var sb = new StringBuilder();
            int total = 0;
            for (int i = 0; i < Header->NumberOfEntries; i++)
            {
                total += Set.GetEntry(Page, i, out var key, out var l);
                sb.AppendLine($" - {Encoding.UTF8.GetString(key)} - {l}");
            }
            sb.AppendLine($"---- size:{total} ----");
            return sb.ToString();
        }
        public override string ToString()
        {
            if (Page.Pointer == null)
                return "<null state>";

            return $"{nameof(Page)}: {Page.PageNumber} - {nameof(LastMatch)} : {LastMatch}, " +
                   $"{nameof(LastSearchPosition)} : {LastSearchPosition} - {Header->NumberOfEntries} entries, {Header->Lower}..{Header->Upper}";
        }
    }
}
