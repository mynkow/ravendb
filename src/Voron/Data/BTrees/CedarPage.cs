using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// Each CedarPage is composed of the following components:
    /// - Header
    /// - BlocksPages with as many as <see cref="CedarRootHeader.NumberOfBlocksPages"/>
    ///     - The first page is going to be shared with the <see cref="CedarPageHeader"/> therefore it will have a few lesser blocks than possible in a page.
    /// - TailPages with as many as <see cref="CedarRootHeader.NumberOfTailPages"/>
    /// - NodesPages with as many as <see cref="CedarRootHeader.NumberOfNodePages"/>    
    /// </summary>
    public unsafe class CedarPage
    {
        public unsafe struct BlocksAccessor
        {
            private readonly CedarPage _page;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public BlocksAccessor(CedarPage page)
            {
                _page = page;
            }
            
            public CedarBlock* this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { throw new NotImplementedException(); }
            }
        }

        public unsafe struct TailAccessor
        {
            private readonly CedarPage _page;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TailAccessor(CedarPage page)
            {
                _page = page;
            }


            public byte this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { throw new NotImplementedException(); }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { throw new NotImplementedException(); }
            }
        }

        public unsafe struct NodesAccessor
        {
            private readonly CedarPage _page;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NodesAccessor(CedarPage page)
            {
                _page = page;
            }

            public CedarDataPtr* this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { throw new NotImplementedException(); }
            }
        }

        private readonly LowLevelTransaction _llt;
        private readonly PageLocator _pageLocator;

        public CedarPage(LowLevelTransaction llt, long pageNumber, CedarPage page = null)
        {
            this._llt = llt;            
            this._pageLocator = new PageLocator(_llt, 8);

            throw new NotImplementedException();
        }

        public CedarPageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { throw new NotImplementedException(); }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
        }

        public BlocksAccessor Blocks => new BlocksAccessor(this);
        public TailAccessor Tail => new TailAccessor(this);
        public NodesAccessor Nodes => new NodesAccessor(this);

        public bool IsBranch
        {
            get { throw new NotImplementedException(); }
        }    
    }

}
