using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 16)]
    public unsafe struct CedarLayout
    {
        [FieldOffset(0)]
        public int HeaderPages;
        [FieldOffset(4)]
        public int BlockPages;
        [FieldOffset(8)]
        public int TailPages;
        [FieldOffset(12)]
        public int DataPages;   
        
        public int TotalPages => HeaderPages + BlockPages + TailPages + DataPages;
    }

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
        /// This is the number of blocks pages we are going to allow. For performance reasons this is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though it is still a constant.
        /// </summary>
        public const int NumberOfBlocksPages = 10;

        /// <summary>
        /// This is the number of tail pages we are going to allow.  For performance reasons this is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though it is still a constant.
        /// </summary>
        public const int NumberOfTailPages = 6;

        /// <summary>
        /// This is the number of data nodes pages we are going to allow.  For performance reasons this is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though it is still a constant.
        /// </summary>
        public const int NumberOfDataNodePages = 10;

        /// <summary>
        /// This is the total number of pages that each Node will spawn. 
        /// The idea is that all those enter into a very few L1 cache lines to exploit the layered access pattern. 
        /// </summary>
        public const int TotalNumberOfPages = NumberOfDataNodePages + NumberOfBlocksPages + NumberOfTailPages + 1;

        public static readonly CedarLayout DefaultLayout = new CedarLayout
        {
            HeaderPages = 1,
            BlockPages = CedarRootHeader.NumberOfBlocksPages,
            TailPages = CedarRootHeader.NumberOfTailPages,
            DataPages = CedarRootHeader.NumberOfDataNodePages
        };
    }
}
