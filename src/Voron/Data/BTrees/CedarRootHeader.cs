using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The Cedar BTree Root Header.
    /// </summary>    
    /// <remarks>This header extends the <see cref="RootHeader"/> structure.</remarks>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct CedarRootHeader
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;

        [FieldOffset(1)]
        public TreeFlags Flags;

        [FieldOffset(2)]
        public long RootPageNumber;

        /// <summary>
        /// This is the number of blocks pages we are going to allow. For now it is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though now it is still a constant.
        /// </summary>
        public const int NumberOfBlocksPages = 10;

        /// <summary>
        /// This is the number of tail pages we are going to allow. For now it is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though now it is still a constant.
        /// </summary>
        public const int NumberOfTailPages = 6;

        /// <summary>
        /// This is the number of data nodes pages we are going to allow. For now it is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though now it is still a constant.
        /// </summary>
        public const int NumberOfDataNodePages = 10;

        [FieldOffset(10)]
        public long BranchPages;
        [FieldOffset(18)]
        public long LeafPages;
        [FieldOffset(34)]
        public long OverflowPages;
        [FieldOffset(42)]
        public long PageCount;
        [FieldOffset(50)]
        public long NumberOfEntries;
        [FieldOffset(58)]
        public int Depth;

        /// <summary>
        /// This is the total number of pages that each Node will spawn. 
        /// The idea is that all those enter into a very few L1 cache lines to exploit the layered access pattern. 
        /// </summary>
        public const int TotalNumberOfPagesPerNode = NumberOfDataNodePages + NumberOfBlocksPages + NumberOfTailPages + 1;

        public static readonly int[] DefaultLayout = { 1, CedarRootHeader.NumberOfBlocksPages, CedarRootHeader.NumberOfTailPages, CedarRootHeader.NumberOfDataNodePages };

    }
}
