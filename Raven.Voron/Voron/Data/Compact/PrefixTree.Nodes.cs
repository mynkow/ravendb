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
    public unsafe static partial class PrefixTree
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

        /// <summary>
        /// The name of a node, is the string deprived of the string stored at it. Page 163 of [1]
        /// </summary>
        public static BitVector Name<T>(this PrefixTree<T> tree, Node* @this)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        public static BitVector Handle<T>(this PrefixTree<T> tree, Node* @this)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        public static BitVector Handle<T>(this PrefixTree<T> tree, ref Node @this)
        {
            fixed (Node* node = &@this)
            {
                return Handle(tree, node);
            }
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        public static BitVector Handle<T>(this PrefixTree<T> tree, ref Internal @this)
        {
            fixed (Internal* node = &@this)
            {
                return Handle(tree, (Node*)node);
            }
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        public static BitVector Handle<T>(this PrefixTree<T> tree, ref Leaf @this)
        {
            fixed (Leaf* node = &@this)
            {
                return Handle(tree, (Node*)node);
            }
        }

        /// <summary>
        /// The extent of a node, is the longest common prefix of the strings represented by the leaves that are descendants of it.
        /// </summary>            
        public static BitVector Extent<T>(this PrefixTree<T> tree, Node* @this)
        {
            throw new NotImplementedException();
        }


        public static int GetExtentLength<T>(this PrefixTree<T> tree, Node* @this)
        {
            throw new NotImplementedException();
        }


        public static int GetJumpLength<T>(this PrefixTree<T> tree, Internal* @this)
        {
            throw new NotImplementedException();
        }

        public static bool IsExitNodeOf<T>(this PrefixTree<T> tree, Node* @this, int length, int lcpLength)
        {
            return @this->NameLength <= lcpLength && (lcpLength < tree.GetExtentLength(@this) || lcpLength == length);
        }

        public static Leaf* GetRightLeaf<T>(this PrefixTree<T> tree, Node* @this)
        {
            if (@this->IsLeaf)
                return (Leaf*)@this;

            Node* node = @this;
            do
            {
                node = (Node*)tree.ReadDirect(((Internal*)node)->JumpRightPtr);
            }
            while (node->IsInternal);

            return (Leaf*)node;
        }

        public static Leaf* GetLeftLeaf<T>(PrefixTree<T> tree, Node* @this )
        {
            if (@this->IsLeaf)
                return (Leaf*)@this;

            Node* node = @this;
            do
            {
                node = (Node*)tree.ReadDirect(((Internal*)node)->JumpLeftPtr);
            }
            while (node->IsInternal);

            return (Leaf*)node;
        }

        public static bool Intersects(Node* @this, int x)
        {
            if (@this->IsInternal)
                return x >= @this->NameLength && x <= ((Internal*)@this)->ExtentLength;
            
            return x >= @this->NameLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int TwoFattest(int a, int b)
        {
            return -1 << Bits.MostSignificantBit(a ^ b) & b;
        }

        public static string ToDebugString<T>(this PrefixTree<T> tree, Node* @this)
        {
            throw new NotImplementedException();
        }
    }
}
