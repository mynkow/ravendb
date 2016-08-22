using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Voron.Data.BTrees.Cedar;

namespace Voron.Data.BTrees
{

    namespace Cedar
    {
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
        internal struct node
        {
            public const int Size = 4;

            [FieldOffset(0)]
            public short Check; // negative means next empty index

            [FieldOffset(4)]
            public short Base; // negative means prev empty index

            public node(int @base = 0, int check = 0)
            {
                this.Check = (short)check;
                this.Base = (short)@base;
            }

            public node(short @base = 0, short check = 0)
            {
                this.Check = check;
                this.Base = @base;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
        internal struct ninfo // x1.5 update speed; +.25 % memory (8n -> 10n)
        {
            public const int Size = 2;

            [FieldOffset(0)]
            public byte Sibling;  // right sibling (= 0 if not exist)
            [FieldOffset(1)]
            public byte Child;    // first child

            public ninfo(byte sibling = 0, byte child = 0)
            {
                this.Sibling = sibling;
                this.Child = child;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
        internal struct block // a block w/ 256 elements
        {
            public const int Size = 20;

            [FieldOffset(0)]
            public int Prev;   // prev block; 3 bytes
            [FieldOffset(4)]
            public int Next;   // next block; 3 bytes
            [FieldOffset(8)]
            public short Num;    // # empty elements; 0 - 256
            [FieldOffset(10)]
            public short Reject; // minimum # branching failed to locate; soft limit
            [FieldOffset(12)]
            public int Trial;  // # trial
            [FieldOffset(16)]
            public int Ehead;  // first empty item

            public block(int prev, int next, short num, short reject, int trial, int ehead)
            {
                this.Prev = prev;
                this.Next = next;
                this.Num = num;
                this.Reject = reject;
                this.Trial = trial;
                this.Ehead = ehead;
            }

            public static block Create(int prev = 0, int next = 0, short num = 256, short reject = 257, int trial = 0, int ehead = 0)
            {
                return new block(prev, next, num, reject, trial, ehead);
            }
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = block.Size + 256 * (ninfo.Size + node.Size))]
    internal struct CedarBlock
    {
        [FieldOffset(0)]
        public block Metadata;

        [FieldOffset(20)]
        public byte Start;

        public const int InfoOffset = 0;
        public const int NodesOffset = 256 * ninfo.Size;
    }
}
