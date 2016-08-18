using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The pointer to the data. A list of these elements can be found on every Cedar Branch Page immediately after the <see cref="CedarBranchPageHeader"/>    
    /// </summary>
    /// <remarks>
    /// We are willing to pay unaligned access costs here because we are looking for the most efficient storage representation for this data.
    /// </remarks>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct CedarDataPtr
    {
        private const byte FlagsMask = 0x80;
        private const byte SizeMask = 0x0F; // Size mask of total size of 16 is fine because we cannot store bigger than 8 bytes data anyways.

        [FieldOffset(0)]
        public byte Header;

        /// <summary>
        /// This is used for concurrency checks.
        /// </summary>
        [FieldOffset(1)]
        public ushort Version;

        [FieldOffset(3)]
        public long PageNumber;

        [FieldOffset(3)]
        public long Data;

        /// <summary>
        /// The type of the data pointer that we have.
        /// </summary>
        /// <remarks>
        /// In case the value is <see cref="CedarNodeFlags.Data"/> the value should be smaller or equal to 8 bytes in size.
        /// </remarks>
        public TreeNodeFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header & FlagsMask) != 0 ? TreeNodeFlags.Data : TreeNodeFlags.PageRef; }
        }

        /// <summary>
        /// The data will be stored directly in the header (up to 8 bytes) this value will tell how big it is (the data type). 
        /// </summary>
        public byte DataSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (byte)(Header & SizeMask); }
        }

        public static byte* DirectAccess(LowLevelTransaction tx, CedarDataPtr* node)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + sizeof(CedarLeafPageHeader);
            }
            return (byte*)&node->Data;
        }

        public static ValueReader Reader(LowLevelTransaction tx, CedarDataPtr* node)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))             
                return new ValueReader((byte*)&node->PageNumber, sizeof(long));

            Debug.Assert(node->DataSize > 0 && node->DataSize <= 8, "The embedded node data size is not compatible for this type of tree");
            return new ValueReader((byte*)&node->Data, node->DataSize);
        }
    }
}
