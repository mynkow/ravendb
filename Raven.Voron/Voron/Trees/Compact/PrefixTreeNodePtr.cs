using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    // Until we have a final implementation, we are using sequential instead of explicit to make it easier to change.
    // TODO: Change when the implementation is more mature.

    /// <summary>
    /// Multiple nodes can be packed in the same page, therefore the pointer to a node is composed of the page and the index of the node within the page. 
    /// </summary>
    // [StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeNodePtr
    {
        /// <summary>
        /// The page where this node is located.
        /// </summary>
        public int Page;
        
        /// <summary>
        /// The indexed number of the node in the page.
        /// </summary>
        public int Index;
    }
}
