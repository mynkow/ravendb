using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Voron.Global;

namespace Voron.Data.BTrees
{
    namespace Cedar
    {
        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal unsafe struct Node
        {
            public const int SizeOf = 4;

            static Node()
            {
                Constants.Assert(() => Node.SizeOf == sizeof(Node), () => $"Update the Size constant to match the size of the {nameof(Node)} struct.");
            }

            [FieldOffset(0)]
            public short Check; // negative means next empty index

            [FieldOffset(2)]
            public short Base; // negative means prev empty index

            [FieldOffset(2)]
            public short Value; // negative means prev empty index

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Set (int @base, int check)
            {
                this.Base = (short)@base;
                this.Check = (short)check;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal unsafe struct NodeInfo // x1.5 update speed; +.25 % memory (8n -> 10n)
        {
            public const int SizeOf = 2;

            static NodeInfo()
            {
                Constants.Assert(() => NodeInfo.SizeOf == sizeof(NodeInfo), () => $"Update the Size constant to match the size of the {nameof(NodeInfo)} struct.");
            }

            [FieldOffset(0)]
            public byte Sibling;  // right sibling (= 0 if not exist)
            [FieldOffset(1)]
            public byte Child;    // first child

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                this.Sibling = 0;
                this.Child = 0;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal unsafe struct BlockMetadata // a BlockMetadata w/ 256 elements
        {
            public const int SizeOf = 20;

            static BlockMetadata()
            {
                Constants.Assert(() => BlockMetadata.SizeOf == sizeof(BlockMetadata), () => $"Update the Size constant to match the size of the {nameof(BlockMetadata)} struct.");
            }

            [FieldOffset(0)]
            public int Prev;   // prev BlockMetadata; 3 bytes
            [FieldOffset(4)]
            public int Next;   // next BlockMetadata; 3 bytes
            [FieldOffset(8)]
            public short Num;    // # empty elements; 0 - 256
            [FieldOffset(10)]
            public short Reject; // minimum # branching failed to locate; soft limit
            [FieldOffset(12)]
            public int Trial;  // # trial
            [FieldOffset(16)]
            public int Ehead;  // first empty item

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Initialize()
            {
                this.Prev = 0;
                this.Next = 0;
                this.Num = 256;
                this.Reject = 257;
                this.Trial = 0;
                this.Ehead = 0;
            }
        }
    }
}
