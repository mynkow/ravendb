using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using static Voron.Data.Compact.PrefixTree;

namespace Voron.Data.Compact
{
    public unsafe static class PrefixTreeOperations
    {
        /// <summary>
        /// The name of a node, is the string deprived of the string stored at it. Page 163 of [1]
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Name(this PrefixTree tree, Node* @this)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Handle(this PrefixTree tree, Node* @this)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Handle(this PrefixTree tree, ref Node @this)
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Handle(this PrefixTree tree, ref Internal @this)
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Handle(this PrefixTree tree, ref Leaf @this)
        {
            fixed (Leaf* node = &@this)
            {
                return Handle(tree, (Node*)node);
            }
        }

        /// <summary>
        /// The extent of a node, is the longest common prefix of the strings represented by the leaves that are descendants of it.
        /// </summary>  
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Extent(this PrefixTree tree, Node* @this)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetExtentLength(this PrefixTree tree, Node* @this)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetJumpLength(this PrefixTree tree, Internal* @this)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsExitNodeOf(this PrefixTree tree, Node* @this, int length, int lcpLength)
        {
            return @this->NameLength <= lcpLength && (lcpLength < tree.GetExtentLength(@this) || lcpLength == length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Leaf* GetRightLeaf(this PrefixTree tree, Node* @this)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Leaf* GetLeftLeaf(PrefixTree tree, Node* @this)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        public static string ToDebugString(this PrefixTree tree, Node* @this)
        {
            throw new NotImplementedException();
        }
    }
}
