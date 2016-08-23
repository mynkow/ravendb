using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Voron.Data.BTrees.Cedar;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// Each CedarPage is composed of the following components:
    /// - Header
    /// - BlocksMetadata (in the header page)
    /// - BlocksPages with as many as <see cref="CedarRootHeader.NumberOfBlocksPages"/>
    /// - TailPages with as many as <see cref="CedarRootHeader.NumberOfTailPages"/>
    /// - NodesPages with as many as <see cref="CedarRootHeader.NumberOfNodePages"/>    
    /// </summary>
    public unsafe class CedarPage
    {
        protected struct BlocksAccessor
        {
            private readonly CedarPage _page;
            private short _currentPageOffset;
            private PageHandlePtr _currentPtr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public BlocksAccessor(CedarPage page)
            {
                _page = page;
                _currentPtr = new PageHandlePtr();
                _currentPageOffset = -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void* Read<T>(int i) where T : struct
            {
                if (typeof(T) == typeof(block))
                {
                    throw new NotImplementedException();
                }
                else
                {
                    int pageOffset = i / _page._blocksPerPage;
                    int pageIndex = i % _page._blocksPerPage;

                    if (pageOffset != _currentPageOffset)
                    {
                        _currentPageOffset = (short)pageOffset;
                        _currentPtr = _page.GetBlocksPageByOffset(pageOffset);
                    }

                    var start = (CedarBlock*)_currentPtr.Value.DataPointer;

                    if (typeof(T) == typeof(ninfo))
                    {
                        return &(start + pageIndex)->NodeInfo;
                    }
                    if (typeof(T) == typeof(node))
                    {
                        return &(start + pageIndex)->Node;
                    }
                }

                throw new NotSupportedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void* Write<T>(int i) where T : struct
            {
                if (typeof(T) == typeof(block))
                {
                    throw new NotImplementedException();
                }
                else
                {
                    int pageOffset = i / _page._blocksPerPage;
                    int pageIndex = i % _page._blocksPerPage;

                    if (pageOffset != _currentPageOffset || !_currentPtr.IsWritable)
                    {
                        _currentPageOffset = (short)pageOffset;
                        _currentPtr = _page.GetBlocksPageByOffset(pageOffset);
                    }

                    var start = (CedarBlock*)_currentPtr.Value.DataPointer;

                    if (typeof(T) == typeof(ninfo))
                    {
                        return &(start + pageIndex)->NodeInfo;
                    }
                    if (typeof(T) == typeof(node))
                    {
                        return &(start + pageIndex)->Node;
                    }
                }

                throw new NotSupportedException();
            }
        }

        protected struct TailAccessor
        {
            private readonly CedarPage _page;
            private short _currentPageOffset;
            private PageHandlePtr _currentPtr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TailAccessor(CedarPage page)
            {
                _page = page;
                _currentPtr = new PageHandlePtr();
                _currentPageOffset = -1;
            }

            public int Length
            {
                get { throw new NotImplementedException(); }
                set { throw new NotImplementedException(); }
            }

            public byte this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    int pageOffset = i / _page._tailBytesPerPage;
                    int pageIndex = i % _page._tailBytesPerPage;
                    if (pageOffset != _currentPageOffset)
                    {
                        _currentPageOffset = (short)pageOffset;
                        _currentPtr = _page.GetTailPageByOffset(pageOffset);
                    }

                    return *(_currentPtr.Value.DataPointer + pageIndex);
                }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    int pageOffset = i / _page._tailBytesPerPage;
                    int pageIndex = i % _page._tailBytesPerPage;
                    if (pageOffset != _currentPageOffset || !_currentPtr.IsWritable)
                    {
                        _currentPageOffset = (short)pageOffset;
                        _currentPtr = _page.GetTailPageByOffset(pageOffset, true);
                    }

                    *(_currentPtr.Value.DataPointer + pageIndex) = value;
                }
            }

            public T Read<T>(int i) where T : struct
            {
                // TODO: Check if typeof(T) == typeof(int) gets optimized.
                throw new NotImplementedException();
            }

            public void Write<T>(int i, T v) where T : struct
            {
                // TODO: Check if typeof(T) == typeof(int) gets optimized.
                throw new NotImplementedException();
            }
        }

        protected struct NodesAccessor
        {
            private readonly CedarPage _page;
            private short _currentPageOffset;
            private PageHandlePtr _currentPtr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NodesAccessor(CedarPage page)
            {
                _page = page;
                _currentPtr = new PageHandlePtr();
                _currentPageOffset = -1;
            }

            public CedarDataPtr* Read(int i)
            {
                int pageOffset = i / _page._nodesPerPage;
                int pageIndex = i % _page._nodesPerPage;
                if (pageOffset != _currentPageOffset)
                {
                    _currentPageOffset = (short)pageOffset;
                    _currentPtr = _page.GetNodesPageByOffset(pageOffset);
                }

                return (CedarDataPtr*)_currentPtr.Value.DataPointer + pageIndex;
            }

            public CedarDataPtr* Write(int i)
            {
                int pageOffset = i / _page._nodesPerPage;
                int pageIndex = i % _page._nodesPerPage;
                if (pageOffset != _currentPageOffset || !_currentPtr.IsWritable)
                {
                    _currentPageOffset = (short)pageOffset;
                    _currentPtr = _page.GetNodesPageByOffset(pageOffset, true);
                }

                return (CedarDataPtr*)_currentPtr.Value.DataPointer + pageIndex;
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetBlocksPageByOffset(int pageOffset, bool writable = false)
        {
            long pageNumber = Header->BlocksPageNumber + pageOffset;
            return writable ? new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) : new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetTailPageByOffset(int pageOffset, bool writable = false)
        {
            long pageNumber = Header->TailPageNumber + pageOffset;
            return writable ? new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) : new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetNodesPageByOffset(int pageOffset, bool writable = false)
        {
            long pageNumber = Header->NodesPageNumber + pageOffset;
            return writable ? new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) : new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }


        private readonly LowLevelTransaction _llt;
        private readonly PageLocator _pageLocator;
        private readonly Page _mainPage;
        private readonly int _tailBytesPerPage;
        private readonly int _blocksPerPage;
        private readonly int _nodesPerPage;

        protected readonly BlocksAccessor Blocks;
        protected readonly TailAccessor Tail;
        protected readonly NodesAccessor Nodes;

        public CedarPage(LowLevelTransaction llt, long pageNumber, CedarPage page = null)
        {
            this._llt = llt;
            this._pageLocator = new PageLocator(_llt, 8);

            if (page != null)
            {
                Debug.Assert(page.PageNumber == pageNumber);
                this._mainPage = page._mainPage;
            }
            else
            {
                this._mainPage = _pageLocator.GetReadOnlyPage(pageNumber);
            }
            
            this._blocksPerPage = (_llt.DataPager.PageSize - sizeof(CedarPageHeader)) / sizeof(CedarBlock);
            this._tailBytesPerPage = _llt.DataPager.PageSize - sizeof(PageHeader);
            this._nodesPerPage = (_llt.DataPager.PageSize - sizeof(PageHeader)) / sizeof(CedarDataPtr);

            this.Blocks = new BlocksAccessor(this);
            this.Tail = new TailAccessor(this);
            this.Nodes = new NodesAccessor(this);
        }

        public CedarPageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (CedarPageHeader*) _mainPage.Pointer; }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->IsBranchPage; }
        }
    }

}
