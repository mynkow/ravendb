using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    public sealed unsafe class PrefixTreePage
    {
        public readonly byte* Pointer;
        public readonly int PageSize;
        public readonly string Source;

        private PrefixTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (PrefixTreePageHeader*)Pointer; }
        }

        public PrefixTreePage(byte* pointer, string source, int pageSize)
        {
            Pointer = pointer;
            Source = source;
            PageSize = pageSize;
        }

        public long PhysicalPage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PhysicalPage; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PhysicalPage = value; }
        }

        public override string ToString()
        {
            return $"#{PhysicalPage}";
        }
    }
}
