using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    // [StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeTableHeader
    {
        /// <summary>
        /// The location where the hash table is located.
        /// </summary>
        public int Page;

        /// <summary>
        /// The amount of elements already stored in the table.
        /// </summary>
        public int Count;

        /// <summary>
        /// The current size of the table.
        /// </summary>
        public int Size;
    }
}
