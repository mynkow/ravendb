using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow;
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
    ///     - NodeInfo sequence
    ///     - Node sequence
    /// - TailPages with as many as <see cref="CedarRootHeader.NumberOfTailPages"/>
    /// - NodesPages with as many as <see cref="CedarRootHeader.NumberOfDataNodePages"/>    
    /// </summary>
    public unsafe partial class CedarPage
    {
        public struct HeaderAccessor
        {
            private readonly CedarPage _page;
            private PageHandlePtr _currentPtr;
            public CedarPageHeader* Ptr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public HeaderAccessor(CedarPage page, PageHandlePtr pagePtr)
            {
                _page = page;
                _currentPtr = pagePtr;

                Ptr = (CedarPageHeader*)_currentPtr.Value.Pointer;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SetWritable()
            {
                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetMainPage(true);

                Ptr = (CedarPageHeader*)_currentPtr.Value.Pointer;
            }
        }

        public struct DataAccessor
        {
            private readonly CedarPage _page;
            private PageHandlePtr _currentPtr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public DataAccessor(CedarPage page)
            {
                _page = page;
                _currentPtr = _page.GetDataNodesPage();
            }

            public CedarDataPtr* DirectRead(long i = 0)
            {
                Debug.Assert(i >= 0);

                return (CedarDataPtr*)(_currentPtr.Value.DataPointer + sizeof(int)) + i;
            }

            public CedarDataPtr* DirectWrite(long i = 0)
            {
                Debug.Assert(i >= 0);

                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetDataNodesPage(true);

                return (CedarDataPtr*)(_currentPtr.Value.DataPointer + sizeof(int)) + i;
            }

            internal int NextFree
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return *(int*)_currentPtr.Value.DataPointer; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { *(int*)_currentPtr.Value.DataPointer = value; }
            }

            public bool CanAllocateNode()
            {
                // If there are no more allocable nodes, we fail.
                if (NextFree == -1)
                    return false;

                return true;
            }

            public int AllocateNode()
            {
                // If there are no more allocable nodes, we fail.
                if (NextFree == -1)
                    throw new InvalidOperationException("Cannot allocate more nodes. There is no enough space. This cannot happen.");

                // We need write access.
                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetDataNodesPage(true);

                int index = NextFree;
                var ptr = (CedarDataPtr*)(_currentPtr.Value.DataPointer + sizeof(int)) + index;

                Debug.Assert(index >= 0, "Index cannot be negative.");
                Debug.Assert(index < _page.Header.Ptr->DataNodesPerPage, "Index cannot be bigger than the quantity of nodes available to use.");
                Debug.Assert(ptr->IsFree);

                // We will store in the data pointer the next free.
                NextFree = (int)ptr->Data;
                ptr->IsFree = false;

                return index;
            }

            public void FreeNode(int index)
            {
                Debug.Assert(index >= 0, "Index cannot be negative.");
                Debug.Assert(index < _page.Header.Ptr->DataNodesPerPage, "Index cannot be bigger than the quantity of nodes available to use.");

                // We need write access.
                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetDataNodesPage(true);

                var ptr = (CedarDataPtr*)(_currentPtr.Value.DataPointer + sizeof(int)) + index;
                Debug.Assert(!ptr->IsFree);

                int currentFree = NextFree;

                ptr->IsFree = true;
                ptr->Data = currentFree;

                NextFree = index;
            }
        }

        protected struct BlocksAccessor
        {
            private readonly CedarPage _page;
            private PageHandlePtr _currentPtr;
            private PageHandlePtr _currentMetadataPtr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public BlocksAccessor(CedarPage page)
            {
                _page = page;
                _currentMetadataPtr = _page.GetBlocksMetadataPage();
                _currentPtr = _page.GetBlocksPage();
            }

            /// <summary>
            /// Returns the first <see cref="Node"/>. CAUTION: Can only be used for reading, use DirectWrite if you need to write to it.
            /// </summary>
            internal Node* Nodes
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (Node*)DirectRead<Node>(); }
            }

            /// <summary>
            /// Returns the first <see cref="NodeInfo"/>. CAUTION: Can only be used for reading, use DirectWrite if you need to write to it.
            /// </summary>
            internal NodeInfo* NodesInfo
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (NodeInfo*)DirectRead<NodeInfo>(); }
            }

            /// <summary>
            /// Returns the first <see cref="BlockMetadata"/>. CAUTION: Can only be used for reading, use DirectWrite if you need to write to it.
            /// </summary>
            internal BlockMetadata* Metadata
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (BlockMetadata*)DirectRead<BlockMetadata>(); }
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void* DirectRead<T>(long i = 0) where T : struct
            {
                if (typeof(T) == typeof(BlockMetadata))
                {
                    return (BlockMetadata*)(_currentMetadataPtr.Value.Pointer + _page.Header.Ptr->MetadataOffset) + i;
                }

                if (typeof(T) == typeof(NodeInfo))
                {
                    return (NodeInfo*)_currentPtr.Value.DataPointer + i;
                }

                // We prefer the Node to go last because sizeof(NodeInfo) == 2 therefore we only shift _blocksPerPage instead of multiply.
                if (typeof(T) == typeof(Node))
                {
                    return (Node*)(_currentPtr.Value.DataPointer + sizeof(NodeInfo) * _page.Header.Ptr->BlocksPerPage) + i;
                }

                throw new NotSupportedException("Access type not supported by this accessor.");
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void* DirectWrite<T>(long i = 0) where T : struct
            {
                if (typeof(T) == typeof(BlockMetadata))
                {
                    if (!_currentMetadataPtr.IsWritable)
                        _currentMetadataPtr = _page.GetBlocksMetadataPage(true);

                    return (BlockMetadata*)(_currentMetadataPtr.Value.Pointer + _page.Header.Ptr->MetadataOffset) + i;
                }

                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetBlocksPage(true);

                if (typeof(T) == typeof(NodeInfo))
                {
                    return (NodeInfo*)_currentPtr.Value.DataPointer + i;
                }

                // We prefer the Node to go last because sizeof(NodeInfo) == 2 therefore we only shift _blocksPerPage instead of multiply.
                if (typeof(T) == typeof(Node))
                {
                    return (Node*)(_currentPtr.Value.DataPointer + sizeof(NodeInfo) * _page.Header.Ptr->BlocksPerPage) + i;
                }

                throw new NotSupportedException("Access type not supported by this accessor.");
            }
        }

        protected struct Tail0Accessor
        {
            private readonly CedarPage _page;
            private PageHandlePtr _currentPtr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Tail0Accessor(CedarPage page)
            {
                _page = page;
                _currentPtr = _page.GetMainPage();
            }

            public int Length
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this[0]; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this[0] = value; }
            }

            public int this[long i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    int* ptr = (int*)(_currentPtr.Value.Pointer + _page.Header.Ptr->Tail0Offset);
                    return *(ptr + i);
                }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    SetWritable();

                    int* ptr = (int*)(_currentPtr.Value.Pointer + _page.Header.Ptr->Tail0Offset);
                    *(ptr + i) = value;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SetWritable()
            {
                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetMainPage(true);
            }

        }

        protected struct TailAccessor
        {
            private readonly CedarPage _page;
            private PageHandlePtr _currentPtr;


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TailAccessor(CedarPage page)
            {
                _page = page;
                _currentPtr = _page.GetTailPage();
            }

            public int Length
            {
                get { return *(int*)_currentPtr.Value.DataPointer; }
                set
                {
                    if (!_currentPtr.IsWritable)
                        _currentPtr = _page.GetTailPage(true);

                    *(int*)_currentPtr.Value.DataPointer = value;
                }
            }

            public byte this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return *(_currentPtr.Value.DataPointer + i); }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    if (!_currentPtr.IsWritable)
                        _currentPtr = _page.GetTailPage(true);

                    *(_currentPtr.Value.DataPointer + i) = value;
                }
            }

            public byte this[long i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return *(_currentPtr.Value.DataPointer + i); }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    if (!_currentPtr.IsWritable)
                        _currentPtr = _page.GetTailPage(true);

                    *(_currentPtr.Value.DataPointer + i) = value;
                }
            }

            public byte* DirectRead(long i = 0)
            {
                return _currentPtr.Value.DataPointer + i;
            }

            public byte* DirectWrite(long i = 0)
            {
                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetTailPage(true);

                return _currentPtr.Value.DataPointer + i;
            }

            public void SetWritable()
            {
                if (!_currentPtr.IsWritable)
                    _currentPtr = _page.GetTailPage(true);
            }
        }

        internal const int BlockSize = 256;

        private readonly LowLevelTransaction _llt;
        private readonly PageLocator _pageLocator;
        private Page _mainPage;

        public HeaderAccessor Header;
        protected BlocksAccessor Blocks;
        protected TailAccessor Tail;
        protected Tail0Accessor Tail0;
        public DataAccessor Data;        

        public CedarPage(LowLevelTransaction llt, long pageNumber, CedarPage page = null)
        {
            this._llt = llt;
            this._pageLocator = new PageLocator(_llt);

            if (page != null)
            {
                Debug.Assert(page.PageNumber == pageNumber);
                this._mainPage = page._mainPage;
            }
            else
            {
                this._mainPage = _pageLocator.GetReadOnlyPage(pageNumber);
            }

            this.Header = new HeaderAccessor(this, new PageHandlePtr(this._mainPage, false));
            this.Blocks = new BlocksAccessor(this);
            this.Tail = new TailAccessor(this);
            this.Tail0 = new Tail0Accessor(this);
            this.Data = new DataAccessor(this);            
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header.Ptr->PageNumber; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header.Ptr->IsBranchPage; }
        }

        public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !Header.Ptr->IsBranchPage; }
        }

        public int[] Layout => new[] { 1, Header.Ptr->BlocksPageCount, Header.Ptr->TailPageCount, Header.Ptr->DataPageCount };

        public static void FreeOnCommit(LowLevelTransaction llt, CedarPage page)
        {
            CedarPageHeader* header = page.Header.Ptr;

            llt.FreePageOnCommit(header->PageNumber);
            llt.FreePageOnCommit(header->BlocksPageNumber);
            llt.FreePageOnCommit(header->TailPageNumber);
            llt.FreePageOnCommit(header->NodesPageNumber);
        }

        public static CedarPage Clone(LowLevelTransaction llt, CedarPage page)
        {
            CedarPageHeader* header = page.Header.Ptr;

            CedarPage clone = Allocate(llt, page.Layout, header->TreeFlags);

            var srcMainPage = page.GetMainPage().Value;
            var destMainPage = clone.GetMainPage(true).Value;
            Memory.Copy(destMainPage.DataPointer, srcMainPage.DataPointer, (srcMainPage.IsOverflow ? srcMainPage.OverflowSize : llt.PageSize) - sizeof(PageHeader));

            var srcBlocksPage = page.GetBlocksPage().Value;
            var destBlocksPage = clone.GetBlocksPage(true).Value;
            Memory.Copy(destBlocksPage.DataPointer, srcBlocksPage.DataPointer, (srcBlocksPage.IsOverflow ? srcBlocksPage.OverflowSize : llt.PageSize) - sizeof(PageHeader));

            var srcTailPage = page.GetTailPage().Value;
            var destTailPage = clone.GetTailPage(true).Value;
            Memory.Copy(destTailPage.DataPointer, srcTailPage.DataPointer, (srcTailPage.IsOverflow ? srcTailPage.OverflowSize : llt.PageSize) - sizeof(PageHeader));

            var srcDataPage = page.GetDataNodesPage().Value;
            var destDataPage = clone.GetDataNodesPage(true).Value;
            Memory.Copy(destDataPage.DataPointer, srcDataPage.DataPointer, (srcDataPage.IsOverflow ? srcDataPage.OverflowSize : llt.PageSize) - sizeof(PageHeader));

            return clone;
        }

        public static CedarPage Allocate(LowLevelTransaction llt, int[] layout, TreePageFlags pageType)
        {
            Debug.Assert(layout.Length == 4);            

            int totalPages = layout[0] + layout[1] + layout[2] + layout[3];
            var pages = llt.AllocatePages(layout, totalPages);

            var header = (CedarPageHeader*)pages[0].Pointer;

            // We do not allow changing the amount of pages because of now we will consider them constants.
            header->BlocksPageCount = layout[1];
            header->BlocksPerPage = ((pages[1].IsOverflow ? pages[1].OverflowSize : llt.PageSize) - sizeof(PageHeader)) / (sizeof(Node) + sizeof(NodeInfo));
            Debug.Assert(header->BlocksPerPage > 0);

            header->TailPageCount = layout[2];
            header->TailBytesPerPage = (pages[2].IsOverflow ? pages[2].OverflowSize : llt.PageSize) - sizeof(PageHeader);
            Debug.Assert(header->TailBytesPerPage > 0);

            header->DataPageCount = layout[3];
            header->DataNodesPerPage = ((pages[3].IsOverflow ? pages[3].OverflowSize : llt.PageSize) - sizeof(PageHeader) - sizeof(int)) / sizeof(CedarDataPtr);
            Debug.Assert(header->DataNodesPerPage > 0);

            header->TreeFlags = pageType;
            Debug.Assert(header->TreeFlags == TreePageFlags.Leaf || header->TreeFlags == TreePageFlags.Branch);

            var page = new CedarPage(llt, pages[0].PageNumber);
            page.Initialize();

            return page;
        }

        internal void Initialize()
        {
            Header.SetWritable();

            // Zero out the main page. 
            PageHandlePtr mainPage = GetMainPage(true);
            byte* ptr = mainPage.Value.Pointer + 40; // 40 is the first byte after the pages size definition.
            Memory.Set(ptr, 0, (mainPage.Value.IsOverflow ? mainPage.Value.OverflowSize : _llt.PageSize) - 40);

            // Bulk zero out the content pages. 
            var pages = new[] { GetBlocksPage(true), GetDataNodesPage(true), GetTailPage(true)};
            foreach (var handle in pages)
            {
                ptr = handle.Value.DataPointer;
                Memory.Set(ptr, 0, (handle.Value.IsOverflow ? handle.Value.OverflowSize : _llt.PageSize) - sizeof(PageHeader));
            }

            CedarPageHeader* header = Header.Ptr;

            // We make sure we do now account for any block that is not complete. 
            header->Size = BlockSize;
            header->Capacity = header->BlocksPerPage - (header->BlocksPerPage % BlockSize);

            header->NumberOfEntries = 0;
            header->ImplicitAfterAllKeys = CedarPageHeader.InvalidImplicitKey;
            header->ImplicitBeforeAllKeys = CedarPageHeader.InvalidImplicitKey;

            // Aligned to 16 bytes
            int offset = sizeof(CedarPageHeader) + 16;
            header->MetadataOffset = offset - offset % 16;
            header->Tail0Offset = header->MetadataOffset + (header->BlocksPerPage + 1) / BlockSize * sizeof(BlockMetadata);

            Debug.Assert(header->MetadataOffset > sizeof(CedarPageHeader));
            Debug.Assert(header->Tail0Offset < _llt.PageSize - 1024); // We need at least 1024 bytes for it. 
        
            for (int i = 0; i < BlockSize; i++)
                header->Reject[i] = (short)(i + 1);

            // Request for writing all the pages. 
            var array = (Node*)Blocks.DirectWrite<Node>();
            var block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();
            var data = Data.DirectWrite();

            array[0] = new Node(0, -1);
            for (int i = 1; i < 256; ++i)
                array[i] = new Node(i == 1 ? -255 : -(i - 1), i == 255 ? -1 : -(i + 1));

            // Initialize the default blocks
            for (int i = 0; i < header->Capacity / header->Size; i++)
                block[i] = BlockMetadata.Create();                    

            block[0].Ehead = 1; // bug fix for erase

            // Initialize the free data node linked list.
            int count = Header.Ptr->DataNodesPerPage - 1;            
            for (int i = 0; i < count; i++)
            {
                // Link the current node to the next.
                data->Header = CedarDataPtr.FreeNode;
                data->Data = i + 1;

                data++;
            }

            // Close the linked list.
            data->Header = CedarDataPtr.FreeNode;
            data->Data = -1;

            Debug.Assert(Data.DirectRead(Header.Ptr->DataNodesPerPage - 1)->IsFree, "Last node is not free.");
            Debug.Assert(Data.DirectRead(Header.Ptr->DataNodesPerPage - 1)->Data == -1, "Free node linked list does not end.");            

            Data.NextFree = 0;

            Tail.Length = sizeof(int);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetMainPage(bool writable = false)
        {
            if (writable)
                Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            this._mainPage = writable ?
                _pageLocator.GetWritablePage(_mainPage.PageNumber) :
                _pageLocator.GetReadOnlyPage(_mainPage.PageNumber);

            return new PageHandlePtr(_mainPage, writable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetBlocksPage(bool writable = false)
        {
            if (writable)
                Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            long pageNumber = Header.Ptr->BlocksPageNumber;
            return writable ?
                new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) :
                new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetTailPage(bool writable = false)
        {
            if (writable)
                Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            long pageNumber = Header.Ptr->TailPageNumber;
            return writable ?
                new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) :
                new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PageHandlePtr GetDataNodesPage(bool writable = false)
        {
            if (writable)
                Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            long pageNumber = Header.Ptr->NodesPageNumber;
            return writable ?
                new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) :
                new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }


        private PageHandlePtr GetBlocksMetadataPage(bool writable = false)
        {
            if (writable)
                Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            long pageNumber = _mainPage.PageNumber;
            return writable ?
                new PageHandlePtr(_pageLocator.GetWritablePage(pageNumber), true) :
                new PageHandlePtr(_pageLocator.GetReadOnlyPage(pageNumber), false);
        }

        public CedarActionStatus AddBranchRef(Slice key, long pageNumber)
        {
            Debug.Assert(this.IsBranch);

            if (key.Options == SliceOptions.Key)
            {
                CedarDataPtr* ptr;
                var status = this.Update(key, sizeof(long), out ptr, nodeFlag: CedarNodeFlags.Branch);
                if (status == CedarActionStatus.Success || status == CedarActionStatus.Found)
                    ptr->Data = pageNumber;

                return status;
            }
            else
            {
                int index = this.Data.AllocateNode();

                CedarDataPtr* ptr = this.Data.DirectWrite(index);
                ptr->Flags = CedarNodeFlags.Branch;
                ptr->PageNumber = pageNumber;
                ptr->DataSize = sizeof(long);

                if (key.Options == SliceOptions.BeforeAllKeys)
                {
                    this.Header.Ptr->ImplicitBeforeAllKeys = (short)index;
                }
                else if (key.Options == SliceOptions.AfterAllKeys)
                {
                    this.Header.Ptr->ImplicitAfterAllKeys = (short)index;
                }

                return CedarActionStatus.Success;
            }            
        }
    }

}
