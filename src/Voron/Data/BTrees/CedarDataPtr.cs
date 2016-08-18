using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The pointer to the data. A list of these elements can be found on every Cedar Branch Page immediately after the <see cref="CedarBranchHeader"/>    
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct CedarDataPtr
    {
        /// <summary>
        /// The type of the data pointer that we have. 
        /// </summary>
        /// <remarks>
        /// In case the value is <see cref="CedarNodeFlags.Data"/> the value should be smaller or equal to 8 bytes in size.
        /// </remarks>
        [FieldOffset(0)]
        public TreeNodeFlags Flags;

        [FieldOffset(1)]
        public ushort Version;

        /// <summary>
        /// In the case where the data can be stored in the header (up to 8 bytes) we can store it directly and this value will tell how big it is. 
        /// </summary>
        [FieldOffset(3)]
        public byte DataSize;

        [FieldOffset(4)]
        public long PageNumber;

        [FieldOffset(4)]
        public long Data;

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
            {
                var overFlowPage = tx.GetPage(node->PageNumber);

                Debug.Assert(overFlowPage.IsOverflow, "Requested overflow page but got " + overFlowPage.Flags);
                Debug.Assert(overFlowPage.OverflowSize > 0, "Overflow page cannot be size equal 0 bytes");
                Debug.Assert(((CedarLeafPageHeader*)overFlowPage.Pointer)->IsValid, "Overflow page cannot be size equal 0 bytes");

                return new ValueReader(overFlowPage.Pointer + sizeof(CedarLeafPageHeader), overFlowPage.OverflowSize);
            }

            Debug.Assert(node->DataSize > 0 && node->DataSize <= 8, "The embedded node data size is not compatible with this type of tree");
            return new ValueReader((byte*)&node->Data, node->DataSize);
        }
    }
}
