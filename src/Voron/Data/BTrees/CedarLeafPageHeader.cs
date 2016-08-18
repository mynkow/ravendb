using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The Cedar Leaf Header can only contain data. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct CedarLeafPageHeader
    {
        /// <summary>
        /// This page number
        /// </summary>
        [FieldOffset(0)]
        public long PageNumber;

        /// <summary>
        /// Page size
        /// </summary>
        [FieldOffset(8)]
        public int OverflowSize;

        /// <summary>
        /// Page flags.
        /// </summary>
        [FieldOffset(12)]
        public PageFlags Flags;

        /// <summary>
        /// Tree pages flags. For this header the only valid value is <see cref="TreePageFlags.Leaf"/>
        /// </summary>
        [FieldOffset(13)]
        public TreePageFlags TreeFlags;

        public bool IsValid
        {
            get
            {
                return Flags == PageFlags.CedarTreePage && TreeFlags == TreePageFlags.Leaf;
            }
        }
    }
}
