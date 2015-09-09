using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    // Until we have a final implementation, we are using sequential to make it easier to change.
    // TODO: Change when the implementation is more mature.

    // [StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)] 
    public struct PrefixTreeRootHeader
    {
        /// <summary>
        /// The location of the root node
        /// </summary>
        public PrefixTreeNodePtr Root;

        /// <summary>
        /// The location of the head node
        /// </summary>
        public PrefixTreeNodePtr Head;

        /// <summary>
        /// The location of the tail node
        /// </summary>
        public PrefixTreeNodePtr Tail;


        public long TablePageNumber;

        public long TableSize;

        public long TablePageCount;

       
        public long InternalCount;

        public long LeafCount;

        public long NodesPageCount;


        public PrefixTreeFlags Flags;
    }
}
