using Bond;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    public unsafe partial class PrefixTree
    {
        public enum NodeType : byte
        {
            Internal = 0,
            Leaf = 1
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 48)]
        public struct Node
        {
            [FieldOffset(0)]
            public NodeType Type;

            [FieldOffset(2)]
            public short NameLength;

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            [FieldOffset(4)]
            public long ReferencePtr;

            public bool IsLeaf => Type == NodeType.Leaf;
            public bool IsInternal => Type == NodeType.Leaf;
        }

        /// <summary>
        /// Every internal node contains a pointer to its two children, the extremes ia and ja of its skip interval,
        /// its own extent ea and two additional jump pointers J- and J+. Page 163 of [1].
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 48)]
        public struct Internal
        {
            [FieldOffset(0)]
            public NodeType Type;

            [FieldOffset(2)]
            public short NameLength;

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            [FieldOffset(4)]
            public long ReferencePtr;

            /// <summary>
            /// The right subtrie.
            /// </summary>
            [FieldOffset(12)]
            public long RightPtr;

            /// <summary>
            /// The left subtrie.
            /// </summary>
            [FieldOffset(20)]
            public long LeftPtr;

            /// <summary>
            /// The downward right jump pointer.
            /// </summary>
            [FieldOffset(28)]
            public long JumpRightPtr;

            /// <summary>
            /// The downward left jump pointer.
            /// </summary>
            [FieldOffset(36)]
            public long JumpLeftPtr;

            [FieldOffset(44)]
            public short ExtentLength;

            public bool IsLeaf => Type == NodeType.Leaf;
            public bool IsInternal => Type == NodeType.Leaf;
        }


        /// <summary>
        /// Leaves are organized in a double linked list: each leaf, besides a pointer to the corresponding string of S, 
        /// stores two pointers to the next/previous leaf in lexicographic order. Page 163 of [1].
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 48)]
        public struct Leaf
        {
            [FieldOffset(0)]
            public NodeType Type;

            [FieldOffset(2)]
            public short NameLength;

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            [FieldOffset(4)]
            public long ReferencePtr;

            /// <summary>
            /// The previous leaf in the double linked list referred in page 163 of [1].
            /// </summary>
            [FieldOffset(16)]
            public long PreviousPtr;

            /// <summary>
            /// The public leaf in the double linked list referred in page 163 of [1].
            /// </summary>
            [FieldOffset(24)]
            public long NextPtr;

            /// <summary>
            /// The stored original value passed.
            /// </summary>
            [FieldOffset(32)]
            public long DataPtr;

            public bool IsLeaf => Type == NodeType.Leaf;
            public bool IsInternal => Type == NodeType.Leaf;
        }        
    }
}
