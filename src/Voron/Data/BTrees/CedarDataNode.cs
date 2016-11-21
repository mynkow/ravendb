using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Voron.Global;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The pointer to the data. A list of these elements can be found on every Cedar Branch Page immediately after the <see cref="CedarPageHeader"/>    
    /// </summary>
    /// <remarks>
    /// We are willing to pay unaligned access costs here because we are looking for the most efficient storage representation for this data.
    /// </remarks>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct CedarDataNode
    {
        public const int SizeOf = 11;

        static CedarDataNode()
        {
            Constants.Assert(() => CedarDataNode.SizeOf == sizeof(CedarDataNode), () => $"Update the Size constant to match the size of the {nameof(CedarDataNode)} struct.");
        }

        public const byte FreeNode = 0x80;
        private const byte FlagsMask = 0x40;
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
        public CedarNodeFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header & FlagsMask) != 0 ? CedarNodeFlags.Data : CedarNodeFlags.Branch; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (CedarNodeFlags.Data == value)
                    Header |= FlagsMask;
                else
                    Header &= SizeMask;
            }
        }

        /// <summary>
        /// The data will be stored directly in the header (up to 8 bytes) this value will tell how big it is (the data type). 
        /// </summary>
        public byte DataSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (byte)(Header & SizeMask); }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header = (byte) ((Header & FlagsMask) | (value & SizeMask)); }
        }

        public bool IsFree
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header & FreeNode) != 0; }
            set
            {
                if (value)
                    Header |= FreeNode;
                else
                    Header &= unchecked((byte) ~FreeNode);
            }
        }

        public static byte* DirectAccess(CedarDataNode* node)
        {
            return (byte*)&node->Data;
        }

        public static ValueReader Reader(CedarDataNode* node)
        {
            if (node->Flags == CedarNodeFlags.Branch)
                return new ValueReader((byte*)&node->PageNumber, sizeof(long));

            Debug.Assert(node->DataSize > 0 && node->DataSize <= 8, "The embedded Node data size is not compatible for this type of tree");
            return new ValueReader((byte*)&node->Data, node->DataSize);
        }
    }
}
