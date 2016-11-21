using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public enum CedarActionStatus
    {
        /// <summary>
        /// Operation worked as expected.
        /// </summary>
        Success,    
        /// <summary>
        /// Key does not fit into the Cedar page. We need to split it.
        /// </summary>
        NotEnoughSpace,
        /// <summary>
        /// Operation worked as expected but did not created a new item.
        /// </summary>
        Found,
        /// <summary>
        /// Key was not found into the Cedar page.
        /// </summary>
        NotFound
    }

    public unsafe class CedarTree
    {
        /// <summary>
        /// Analyze the impact of having page size as a constant. 
        /// </summary>
        internal const int PageSize = Constants.Storage.PageSize;

        private readonly Transaction _tx;
        private readonly LowLevelTransaction _llt;
        private readonly CedarMutableState _state;

        private readonly Dictionary<long, CedarPage> _recentlyUsedPages = new Dictionary<long, CedarPage>(NumericEqualityComparer.Instance);

        private enum ActionType
        {
            Add,
            Delete
        }

        //public event Action<long> PageModified;
        //public event Action<long> PageFreed;        

        public Slice Name { get; set; }

        public CedarMutableState State => _state;
        public LowLevelTransaction Llt => _llt;

        private CedarTree(LowLevelTransaction llt, Transaction tx, long root)
        {
            _llt = llt;
            _tx = tx; 

            _state = new CedarMutableState(llt)
            {
                RootPageNumber = root
            };
        }

        public static CedarTree Open(LowLevelTransaction llt, Transaction tx, CedarRootHeader* header)
        {
            if (PageSize != llt.PageSize)
                throw new NotSupportedException("Cedar Trees are only supported on transactions running the same size of Voron pages as configured in the global constants.");

            return new CedarTree(llt, tx, header->RootPageNumber)
            {
                _state =
                {                     
                    PageCount = header->PageCount,
                    BranchPages = header->BranchPages,
                    Depth = header->Depth,
                    OverflowPages = header->OverflowPages,
                    LeafPages = header->LeafPages,
                    NumberOfEntries = header->NumberOfEntries,
                    Flags = header->Flags,
                    InWriteTransaction = (llt.Flags == TransactionFlags.ReadWrite),
                }
            };
        }

        internal CedarLayout Layout => CedarRootHeader.DefaultLayout;


        public static CedarTree Create(LowLevelTransaction llt, Transaction tx, TreeFlags flags = TreeFlags.None)
        {
            Debug.Assert(llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            if (PageSize != llt.PageSize)
                throw new NotSupportedException("Cedar Trees are only supported on transactions running the same size of Voron pages as configured in the global constants.");

            var leaf = CedarPage.Allocate(llt, CedarRootHeader.DefaultLayout, TreePageFlags.Leaf);
            leaf.Header.Ptr->TreeFlags = TreePageFlags.Leaf;

            var tree = new CedarTree(llt, tx, leaf.PageNumber)
            {
                _state =
                {
                    Depth = 1,
                    Flags = flags,
                    InWriteTransaction = true,
                }
            };

            if (tree.Layout.BlockPages * CedarPageHeader.BlocksPerPage / 256 > CedarPageHeader.MaxSupportedBlocks)
                throw new NotSupportedException($"Cedar Trees only allow a maximum of {CedarPageHeader.MaxSupportedBlocks} metadata blocks.");

            tree.State.RecordNewPage(leaf, 1);
            return tree;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(string key, long value, ushort? version = null)
        {
            Slice keySlice;
            Slice.From(_tx.Allocator, key, out keySlice);
            Add(keySlice, value, version);
        }

        public void Add(Slice key, long value, ushort? version = null)
        {
            State.IsModified = true;
            var pos = DirectAdd(key, version);

            // TODO: Check how to write this (endianess).
            *((long*)pos) = value;                                               
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public long Read(string key)
        {
            Slice keySlice;
            Slice.From(_tx.Allocator, key, out keySlice);
            return Read(keySlice);
        }

        public long Read(Slice key)
        {
            var pos = DirectRead(key);

            // TODO: Check how to write this (endianess).
            return *((long*)pos);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Delete(string key, ushort? version = null)
        {
            Slice keySlice;
            Slice.From(_tx.Allocator, key, out keySlice);
            Delete(keySlice, version);
        }

        public void Delete(Slice key, ushort? version = null)
        {
            if (_llt.Flags != TransactionFlags.ReadWrite)
                throw new ArgumentException("Cannot delete a value in a read only transaction");

            // We look for the leaf page that is going to host this data. 
            using (var cursor = FindPageFor(key))
            {
                CedarPage page = cursor.CurrentPage;
                Debug.Assert(page.IsLeaf);

                ushort nodeVersion;
                if (page.Remove(key, out nodeVersion) == CedarActionStatus.Success)
                {
                    CheckConcurrency(key, version, nodeVersion, ActionType.Delete);

                    if (page.Header.Ptr->NumberOfEntries == 0 && !cursor.IsRoot)
                        throw new NotImplementedException("We didnt implement yet the remove when we are the only value in the node.");

                    State.IsModified = true;
                    State.NumberOfEntries--;
                }
            }
        }


        public byte* DirectRead(Slice key)
        {
            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            // We look for the leaf page that is going to host this data. 
            using (var cursor = FindPageFor(key))
            {
                cursor.Seek(key);

                // This is efficient because we return the very same Slice so checking can be done via pointer comparison. 
                return cursor.Key.Equals(key) ? cursor.Value : null;
            }
        }

        public byte* DirectAdd(Slice key, ushort? version = null, int size = 8)
        {
            if (_llt.Flags != TransactionFlags.ReadWrite)
                throw new ArgumentException("Cannot add a value in a read only transaction");

            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            // We will be able to write the data if the data fit into the allocated space or if we have smaller than 8 bytes data to store.
            if (size > 8 || size < 0)
                throw new ArgumentOutOfRangeException(nameof(size), "The supported range is between 0 and 8 bytes.");

            if (State.InWriteTransaction)
                State.IsModified = true;

            // We look for the leaf page that is going to host this data. 
            using (var cursor = FindPageFor(key))
            {
                // Updates may fail if we have to split the page. 
                CedarDataNode* ptr;
                CedarActionStatus status;
                do
                {
                    // We always need the current one, just in case that a page split happened.
                    CedarPage page = cursor.CurrentPage;

                    // It will output the position of the data to be written to. 
                    status = page.Update(key, size, out ptr);
                    if (status == CedarActionStatus.NotEnoughSpace)
                    {
                        // We need to split because there is not enough space available to add this key into the page.
                        var pageSplitter = new CedarPageSplitter(_llt, this, cursor, key);
                        pageSplitter.Execute(); // This effectively acts as a FindPageFor(key, out node) call;
                    }
                }
                while (status == CedarActionStatus.NotEnoughSpace);

                // If we are creating a new entry, record it.
                // It can happen that we are just overwriting one, which doesnt increase the counter.
                if (status == CedarActionStatus.Success)
                    State.NumberOfEntries++;

                return (byte*)&ptr->Data;
            }
        }

        public CedarTreeIterator Iterate(bool prefetch)
        {
            return new CedarTreeIterator(this, _llt, prefetch);
        }

        internal CedarCursor FindPageFor(Slice key)
        {
            CedarCursor cursor = new CedarCursor(_llt, this, GetPage(State.RootPageNumber));

            while (cursor.CurrentPage.IsBranch)
            {
                cursor.Seek(key);

                if (cursor.Key.Same(Slices.BeforeAllKeys))
                    cursor.MoveNext();

                Debug.Assert(cursor.Pointer != null, "This cannot happen on a branch page, as long as there is a branch page, there is a branch that can be taken.");

                CedarDataNode* ptr = cursor.Pointer;
                Debug.Assert(ptr->Flags == CedarNodeFlags.Branch, "This is a branch node. The flags must be branch for every single one");

                var newPage = GetPage(ptr->PageNumber);
                cursor.Push(newPage);
            }

            Debug.Assert(cursor.CurrentPage.IsLeaf);

            return cursor;
        }

        private void CheckConcurrency(Slice key, ushort? expectedVersion, ushort nodeVersion, ActionType actionType)
        {
            if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
                throw new ConcurrencyException(string.Format("Cannot {0} '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name))
                {
                    ActualETag = nodeVersion,
                    ExpectedETag = expectedVersion.Value,
                };
        }


        private CedarPage _lastUsedPage;

        public CedarPage GetPage(long pageNumber)
        {
            // TODO: Look if a fast LRU would work here. 
            if (_lastUsedPage != null && _lastUsedPage.PageNumber == pageNumber)
                return _lastUsedPage;

            CedarPage page;
            if (!_recentlyUsedPages.TryGetValue(pageNumber, out page))
            {
                page = new CedarPage(_llt, pageNumber);
                _recentlyUsedPages[pageNumber] = page;
            }

            _lastUsedPage = page;
            return page;
        }
    }
}
