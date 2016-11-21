using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Impl;

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
        internal const int BlockSize = 256;

        private readonly LowLevelTransaction _llt;
        private FixedPageLocator _pageLocator;

        public HeaderAccessor Header;
        public DataAccessor Data;

        internal BlocksAccessor Blocks;
        internal TailAccessor Tail;
        internal Tail0Accessor Tail0;

        /// <summary>
        /// This version of the constructor is used for temporary pages. It will initialize everything in Read-Write mode.
        /// </summary>
        internal CedarPage(LowLevelTransaction llt, byte* storage, CedarLayout layout)
        {
            this._llt = llt;

            // The idea here is that the PageLocator should never evict one of these pages therefore it must have
            // enough holding space for the whole tree section pages. 
            
            // Because the storage for this page is "raw memory" we need to add all the pages to the page locator to fool the 
            // code on thinking that it is getting the pages from the storage. 

            // OPTIMIZE: This locator should be allocated from the transaction pool instead.             
            
            for (int i = 0; i < layout.TotalPages; i++)
            {
                var tempPage = new Page(storage);               

                if (i == 0)
                {                  
                    this._pageLocator = new FixedPageLocator(_llt, tempPage.PageNumber);
                    this.PageNumber = tempPage.PageNumber;
                }

                this._pageLocator.AddWritable(tempPage);
                storage += llt.PageSize;
            }

            this.Header = new HeaderAccessor(this);
            this.Tail0 = new Tail0Accessor(this);
            this.Blocks = new BlocksAccessor(this);
            this.Tail = new TailAccessor(this);
            this.Data = new DataAccessor(this);
        }

        public CedarPage(LowLevelTransaction llt, Page page, CedarLayout layout, TreePageFlags pageType)
        {
            this._llt = llt;
            this.PageNumber = page.PageNumber;

            var header = (CedarPageHeader*)page.Pointer;

            // We do not allow changing the amount of pages because of now we will consider them constants.
            header->BlocksPageCount = layout.BlockPages;
            header->BlocksPageNumber = header->PageNumber + 1;

            header->TailPageCount = layout.TailPages;
            header->TailPageNumber = header->BlocksPageNumber + header->BlocksPageCount;

            header->DataNodesPageCount = layout.DataPages;
            header->DataNodesPageNumber = header->TailPageNumber + header->TailPageCount;

            header->TreeFlags = pageType;
            Debug.Assert(header->TreeFlags == TreePageFlags.Leaf || header->TreeFlags == TreePageFlags.Branch);

            // The idea here is that the PageLocator should never evict one of these pages therefore it must have
            // enough holding space for the whole tree section pages. 
            this._pageLocator = new FixedPageLocator(_llt, page.PageNumber);

            this.Header = new HeaderAccessor(this, page.Pointer);
            this.Tail0 = new Tail0Accessor(this, page.Pointer);
            this.Blocks = new BlocksAccessor(this);
            this.Tail = new TailAccessor(this);
            this.Data = new DataAccessor(this);
        }

        public CedarPage(LowLevelTransaction llt, long pageNumber)
        {
            this._llt = llt;
            this.PageNumber = pageNumber;

            // The idea here is that the PageLocator should never evict one of these pages therefore it must have
            // enough holding space for the whole tree section pages. 
            this._pageLocator = new FixedPageLocator(_llt, pageNumber);

            this.Header = new HeaderAccessor(this);
            this.Tail0 = new Tail0Accessor(this);
            this.Blocks = new BlocksAccessor(this);
            this.Tail = new TailAccessor(this);
            this.Data = new DataAccessor(this);
        }

        public readonly long PageNumber;

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

        public CedarLayout Layout => new CedarLayout
        {
            HeaderPages = 1,
            BlockPages = Header.Ptr->BlocksPageCount,
            TailPages = Header.Ptr->TailPageCount,
            DataPages = Header.Ptr->DataNodesPageCount
        };

        private struct TemporaryPageScope : IDisposable
        {
            private ByteStringContext.InternalScope _memoryScope;

            public TemporaryPageScope(ByteStringContext.InternalScope scope)
            {
                this._memoryScope = scope;
            }

            public void Dispose()
            {
                _memoryScope.Dispose();
            }
        }

        public static IDisposable CloneInMemory(LowLevelTransaction llt, CedarPage srcPage, out CedarPage destPage)
        {
            ByteString tempStorage;
            var scope = new TemporaryPageScope(llt.Allocator.Allocate(srcPage.Layout.TotalPages * llt.PageSize, out tempStorage));

            long firstPageNumber = srcPage.Header.Ptr->PageNumber;
            
            int pageSize = llt.PageSize;
            byte* destBuffer = tempStorage.Ptr;            
            for (long i = 0; i < srcPage.Layout.TotalPages; i++)
            {
                // We cannot assume that the individual pages will be contiguous in-memory, even though they have contiguous locations in the data file.
                var page  = srcPage._pageLocator.GetReadOnlyPage(firstPageNumber + i);
                Memory.BulkCopy(destBuffer, page.Pointer, pageSize);
                destBuffer += pageSize;
            }

            destPage = new CedarPage(llt, tempStorage.Ptr, srcPage.Layout);          

            return scope;
        }

        public static CedarPage Allocate(LowLevelTransaction llt, CedarLayout layout, TreePageFlags pageType)
        {
            int totalPages = layout.TotalPages;
            Debug.Assert(layout.BlockPages > 1);

            var page = llt.AllocatePage(totalPages, zeroPage: true);
            llt.BreakLargeAllocationToSeparatePages(page.PageNumber); // Separate all pages.

            var cedarPage = new CedarPage(llt, page, layout, pageType);
            cedarPage.Initialize();
            return cedarPage;
        }


        internal void Initialize()
        {
            Header.SetWritable();

            CedarPageHeader* header = Header.Ptr;

            // We make sure we do now account for any block that is not complete. 
            header->Size = BlockSize;
            header->Capacity = header->BlocksTotalCount;
            header->Capacity -= header->Capacity % BlockSize;

            header->NumberOfEntries = 0;
            header->ImplicitAfterAllKeys = CedarPageHeader.InvalidImplicitKey;
            header->ImplicitBeforeAllKeys = CedarPageHeader.InvalidImplicitKey;           
        
            for (int i = 0; i < BlockSize; i++)
                header->Reject[i] = (short)(i + 1);

            // Request for writing all the pages. 
            NodesWritePtr array = Blocks.DirectWrite<NodesWritePtr>();
            BlockMetadataWritePtr block = Blocks.DirectWrite<BlockMetadataWritePtr>();

            array.Write(0)->Set(0, -1);
            for (int i = 1; i < 256; ++i)
                array.Write(i)->Set(i == 1 ? -255 : -(i - 1), i == 255 ? -1 : -(i + 1));

            // Initialize the default blocks
            for (int i = 0; i < header->Capacity / BlockSize; i++)
                block.Write(i)->Initialize();                    

            block.Write(0)->Ehead = 1; // bug fix for erase

            // Initialize the free data node linked list.

            Data.Initialize();

            // Zero out the tail pages.            
            Tail.Length = sizeof(int);
        }

        public CedarActionStatus AddBranchRef(Slice key, long pageNumber)
        {
            Debug.Assert(this.IsBranch);

            if (key.Options == SliceOptions.Key)
            {
                CedarDataNode* ptr;
                var status = this.Update(key, sizeof(long), out ptr, nodeFlag: CedarNodeFlags.Branch);
                if (status == CedarActionStatus.Success || status == CedarActionStatus.Found)
                    ptr->Data = pageNumber;

                return status;
            }
            else
            {
                int index = this.Data.AllocateNode();

                CedarDataNodeWritePtr accesor = this.Data.DirectWrite(index);
                var ptr = (CedarDataNode*)accesor;
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
