using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Data.BTrees;

namespace Voron.Impl
{
    internal unsafe class TreeSizeOf
    {
        /// <summary>
        /// Calculate the size of a leaf node.
        /// The size depends on the environment's page size; if a data item
        /// is too large it will be put onto an overflow page and the node
        /// size will only include the key and not the data. Sizes are always
        /// rounded up to an even number of bytes, to guarantee 2-byte alignment
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeafEntry<T>(int pageMaxSpace, T key, int len)
            where T : ISlice
        {
            var nodeSize = Constants.NodeHeaderSize;

            if (key.Options == SliceOptions.Key)
                nodeSize += key.Size;

            if (len != 0)
            {
                nodeSize += len;

                if (nodeSize > pageMaxSpace)
                    nodeSize -= len - Constants.PageNumberSize;
            }
            // else - page ref node, take no additional space

            nodeSize += nodeSize & 1;

            return nodeSize;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BranchEntry<T>(T key)
            where T : ISlice
        {
            var sz = Constants.NodeHeaderSize + key.Size;
            sz += sz & 1;
            return sz;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NodeEntry<T>(int pageMaxSpace, T key, int len)
            where T : ISlice
        {
            if (len < 0)
                return BranchEntry(key);

            return LeafEntry(pageMaxSpace, key, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NodeEntry(TreeNodeHeader* other)
        {
            var sz = other->KeySize + Constants.NodeHeaderSize;
            if (other->Flags == TreeNodeFlags.Data || other->Flags == TreeNodeFlags.MultiValuePageRef)
                sz += other->DataSize;

            sz += sz & 1;

            return sz;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NodeEntryWithAnotherKey<T>(TreeNodeHeader* other, T key)
            where T : ISlice
        {
            var keySize = key.HasValue ? key.Size : other->KeySize;
            var sz = keySize + Constants.NodeHeaderSize;
            if (other->Flags == TreeNodeFlags.Data || other->Flags == TreeNodeFlags.MultiValuePageRef)
                sz += other->DataSize;

            sz += sz & 1;

            return sz;
        }
    }
}
