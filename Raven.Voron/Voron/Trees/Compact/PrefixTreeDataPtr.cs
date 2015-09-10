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

    // [StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeDataPtr // We will probably want to have an alternative PrefixTreeEmbeededData of the same size to be able to store primitive data values. 
    {        
        public int Page;
        public int Start;
        public int Size;
    }

    public enum PrefixTreeDataFields
    {
        /// <summary>
        /// BlockSize is the actual size of the whole block this data field is using. In cases of single inserts the |Block| = |Key| + |Data| but
        /// when faced with in-place updates with smaller data, for recovery we will need to know where the next data block starts. Block size allow
        /// us to move sequentially along the data segment even in cases where the data has shrink.
        /// </summary>
        BlockSize,
        /// <summary>
        /// The key as a byte[]
        /// </summary>
        Key,
        /// <summary>
        /// The actual data as a byte[]
        /// </summary>
        Data,
    }
}
