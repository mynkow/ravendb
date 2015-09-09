using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    [Flags]
    public enum PrefixTreeNodeType
    {
        Internal = 1,
        Leaf = 2,
    }

    // Until we have a final implementation, we are using sequential to make it easier to change.
    // TODO: Change when the implementation is more mature.

    /// <summary>
    /// This is a union type struct used for the purpose of just querying what type of node we are working on.
    /// Size of the struct is the max between PrefixTreeInternalNode and PrefixTreeLeafNode.
    /// </summary>
    // [StructLayout(LayoutKind.Explicit, Pack = 1, Size = xxx)] 
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeNode
    {
        public PrefixTreeNodeType Type;

        /// <summary>
        /// The length of the name of the node. 
        /// </summary>
        public int NameLength;

        /// <summary>
        /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
        /// in the subtree, as all leaves will share the same key prefix.
        /// </summary>
        public PrefixTreeNodePtr ReferencePtr;
    }

    /// <summary>
    /// Every internal node contains a pointer to its two children, the extremes ia and ja of its skip interval,
    /// its own extent ea and two additional jump pointers J- and J+. Page 163 of [1].
    /// </summary>
    // [StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeInternalNode
    {
        public PrefixTreeNodeType Type;

        /// <summary>
        /// The length of the name of the node. 
        /// </summary>
        public int NameLength;

        /// <summary>
        /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
        /// in the subtree, as all leaves will share the same key prefix.
        /// </summary>
        public PrefixTreeNodePtr ReferencePtr;

        /// <summary>
        /// The length of the extent of this node.
        /// </summary>
        public int ExtentLength;

        /// <summary>
        /// The right subtrie.
        /// </summary>
        public PrefixTreeNodePtr Right;

        /// <summary>
        /// The left subtrie.
        /// </summary>
        public PrefixTreeNodePtr Left;

        /// <summary>
        /// The downward right jump pointer.
        /// </summary>
        public PrefixTreeNodePtr JumpRightPtr;

        /// <summary>
        /// The downward left jump pointer.
        /// </summary>
        public PrefixTreeNodePtr JumpLeftPtr;
    }

    /// <summary>
    /// Leaves are organized in a double linked list: each leaf, besides a pointer to the corresponding string of S, 
    /// stores two pointers to the next/previous leaf in lexicographic order. Page 163 of [1].
    /// </summary>
    // [StructLayout(LayoutKind.Explicit, Pack = 1, Size = xxx)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeLeafNode
    {
        public PrefixTreeNodeType Type;

        /// <summary>
        /// The length of the name of the node. 
        /// </summary>
        public int NameLength;

        /// <summary>
        /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
        /// in the subtree, as all leaves will share the same key prefix.
        /// </summary>
        public PrefixTreeNodePtr ReferencePtr;

        /// <summary>
        /// The previous leaf in the double linked list referred in page 163 of [1].
        /// </summary>
        public PrefixTreeNodePtr Previous;

        /// <summary>
        /// The public leaf in the double linked list referred in page 163 of [1].
        /// </summary>
        public PrefixTreeNodePtr Next;

        /// <summary>
        /// The pointer to the actual data. The data will include the key used to store, so the key will be retrieved from there. 
        /// </summary>
        public PrefixTreeDataPtr Value;
    }
}
