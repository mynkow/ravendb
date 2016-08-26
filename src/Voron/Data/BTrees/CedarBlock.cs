using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{

    namespace Cedar
    {
        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal struct Node
        {
            [FieldOffset(0)]
            public short Check; // negative means next empty index

            [FieldOffset(4)]
            public short Base; // negative means prev empty index

            [FieldOffset(4)]
            public short Value; // negative means prev empty index

            public Node(int @base = 0, int check = 0)
            {
                this.Check = (short)check;
                this.Base = (short)@base;
                this.Value = (short)@base;
            }

            public Node(short @base = 0, short check = 0)
            {
                this.Check = check;
                this.Base = @base;
                this.Value = @base;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal struct NodeInfo // x1.5 update speed; +.25 % memory (8n -> 10n)
        {
            [FieldOffset(0)]
            public byte Sibling;  // right sibling (= 0 if not exist)
            [FieldOffset(1)]
            public byte Child;    // first child

            public NodeInfo(byte sibling = 0, byte child = 0)
            {
                this.Sibling = sibling;
                this.Child = child;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        internal struct BlockMetadata // a BlockMetadata w/ 256 elements
        {
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

            public BlockMetadata(int prev, int next, short num, short reject, int trial, int ehead)
            {
                this.Prev = prev;
                this.Next = next;
                this.Num = num;
                this.Reject = reject;
                this.Trial = trial;
                this.Ehead = ehead;
            }

            public static BlockMetadata Create(int prev = 0, int next = 0, short num = 256, short reject = 257, int trial = 0, int ehead = 0)
            {
                return new BlockMetadata(prev, next, num, reject, trial, ehead);
            }
        }
    }
}
