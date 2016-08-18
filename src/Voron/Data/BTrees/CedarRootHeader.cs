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
        /// This is the number of nodes pages we are going to allow. For now it is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though now it is still a constant.
        /// </summary>
        public const long NumberOfNodePages = 3;
        /// <summary>
        /// This is the number of tail pages we are going to allow. For now it is a constant, but the idea is that
        /// we can control this. That's why it is already defined at the CedarRootHeader even though now it is still a constant.
        /// </summary>
        public const long NumberOfTailPages = 1;

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
    }
}
