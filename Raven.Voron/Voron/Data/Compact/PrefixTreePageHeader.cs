using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    //TODO: Change this when we are ready to go.
    //[StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential)]
    public struct PrefixTreePageHeader
    {
        /// <summary>
        /// The physical storage page number
        /// </summary>
        public long PhysicalPage;

        /// <summary>
        /// This is the page number following the growth strategy for the tree structure. This virtual page numbers
        /// are used to navigate the tree in a cache concious fashion and are part of a virtual numbering of the nodes
        /// used for fast retrieval of node offsets.
        /// </summary>
        public long Page;

        /// <summary>
        /// This is the root node index for the current page in the whole tree. This number can be calculated from the VirtualPage
        /// but for performance reasons it makes sense to store it.
        /// </summary>
        public long RootIndex;

        /// <summary>
        /// This is the virtual page number for the parent page.
        /// </summary>
        public long ParentPage;

        /// <summary>
        /// This is the relative index of the parent node at the parent page.
        /// </summary>
        public int ParentIndex;
    }
}
