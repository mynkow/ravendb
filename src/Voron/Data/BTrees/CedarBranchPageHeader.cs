﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{

    /// <summary>
    /// The Cedar Branch Header is contained by the first page or any Cedar BTree Node
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct CedarBranchPageHeader
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
        /// Tree pages flags. For this header the only valid value is <see cref="TreePageFlags.Branch"/>
        /// </summary>
        [FieldOffset(13)]
        public TreePageFlags TreeFlags;

        /// <summary>
        /// How many nodes pages are available for this node. 
        /// </summary>
        [FieldOffset(16)]
        public int NodePageCount;

        /// <summary>
        /// How many tail pages are available to be used.
        /// </summary>
        [FieldOffset(20)]
        public int TailPageCount;

        /// <summary>
        /// How many blocks pages are available to be used.
        /// </summary>
        [FieldOffset(24)]
        public int BlocksPageCount;


        public bool IsValid
        {
            get
            {
                return Flags == PageFlags.CedarTreePage && TreeFlags == TreePageFlags.Branch;
            }
        }
    }
}
