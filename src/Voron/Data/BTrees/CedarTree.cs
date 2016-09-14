using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Voron.Exceptions;
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
    }

    public unsafe class CedarTree
    {        
        private readonly Transaction _tx;
        private readonly LowLevelTransaction _llt;
        private readonly CedarMutableState _state;

        private readonly RecentlyFoundCedarPages _recentlyFoundPages;

        private enum ActionType
        {
            Add,
            Delete
        }


        //public event Action<long> PageModified;
        //public event Action<long> PageFreed;        

        public string Name { get; set; }


        public CedarMutableState State => _state;
        public LowLevelTransaction Llt => _llt;

        private CedarTree(LowLevelTransaction llt, Transaction tx, long root)
        {
            _llt = llt;
            _tx = tx;
            _recentlyFoundPages = new RecentlyFoundCedarPages(llt.Flags == TransactionFlags.Read ? 8 : 2);
            _state = new CedarMutableState(llt)
            {
                RootPageNumber = root
            };
        }

        public static CedarTree Open(LowLevelTransaction llt, Transaction tx, CedarRootHeader* header)
        {
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
        

        public static CedarTree Create(LowLevelTransaction llt, Transaction tx, TreeFlags flags = TreeFlags.None)
        {
            Debug.Assert(llt.Flags == TransactionFlags.ReadWrite, "Create is being called in a read transaction.");

            var leaf = CedarPage.Allocate(llt, CedarRootHeader.DefaultLayout, CedarRootHeader.TotalNumberOfPagesPerNode);
            leaf.Header.Ptr->TreeFlags = TreePageFlags.Leaf;
            leaf.Initialize();
            
            var tree = new CedarTree(llt, tx, leaf.PageNumber)
            {
                _state =
                {
                    Depth = 1,
                    Flags = flags,
                    InWriteTransaction = true,
                }
            };

            tree.State.RecordNewPage(leaf, 1);
            return tree;
        }
        
        public void Add(string key, long value, ushort? version = null)
        {
            Add(Slice.From(_tx.Allocator, key), value, version);
        }

        public void Add(Slice key, long value, ushort? version = null)
        {
            State.IsModified = true;
            var pos = DirectAdd(key, version);

            // TODO: Check how to write this (endianess).
            *((long*)pos) = value;                                               
        }

        public long Read(string key)
        {
            return Read(Slice.From(_tx.Allocator, key));
        }

        public long Read(Slice key)
        {
            var pos = DirectRead(key);

            // TODO: Check how to write this (endianess).
            return *((long*)pos);
        }

        public byte* DirectRead(Slice key)
        {
            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            // We look for the branch page that is going to host this data. 
            CedarCursor cursor;
            CedarPage page = FindLocationFor(key, out cursor);

            // This is efficient because we return the very same Slice so checking can be done via pointer comparison. 
            if (cursor.Key.Same(key))
                return cursor.Value;

            // If this triggers it is signaling a defect on the cursor implementation
            // This is a checked invariant on debug builds. 
            Debug.Assert(!cursor.Key.Equals(key));

            return null;
        }

        public byte* DirectAdd(Slice key, ushort? version = null, int size = 8)
        {
            if (State.InWriteTransaction)
                State.IsModified = true;

            if (_llt.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot add a value in a read only transaction");

            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            // We will be able to write the data if the data fit into the allocated space or if we have smaller than 8 bytes data to store.
            if (size > 8 || size < 0)
                throw new ArgumentOutOfRangeException(nameof(size), "The supported range is between 0 and 8 bytes.");

            // We look for the branch page that is going to host this data. 
            CedarCursor cursor;
            CedarPage page = FindLocationFor(key, out cursor);

            // This is efficient because we return the very same Slice so checking can be done via pointer comparison. 
            byte* pos;            
            if (cursor.Key.Same(key)) 
            {
                // This is an update operation (key was found).               
                CheckConcurrency(key, version, cursor.NodeVersion, ActionType.Add);                
                
                // We need write access to the node.
                short dataPosition = cursor.Result.Value;
                CedarDataPtr* ptr = page.Data.DirectWrite(dataPosition);
                ptr->DataSize = (byte) size;

                return (byte*)&ptr->Data;
            }

            // If this triggers it is signaling a defect on the cursor implementation
            // This is a checked invariant on debug builds. 
            Debug.Assert(!cursor.Key.Equals(key));

            // Updates may fail if we have to split the page. 
            CedarActionStatus status;
            do
            {
                // It will output the position of the data to be written to. 
                status = page.Update(key, size, out pos);
                if (status == CedarActionStatus.NotEnoughSpace)
                {
                    // We need to split because there is not enough space available to add this key into the page.
                    var pageSplitter = new CedarPageSplitter(_llt, this, cursor, key);
                    cursor = pageSplitter.Execute(); // This effectively acts as a FindLocationFor(key, out node) call;
                }

            }
            while (status != CedarActionStatus.Success);

            // Record the new entry.
            State.NumberOfEntries++;

            return pos;
        }

        public CedarIterator Iterate(bool prefetch)
        {
            throw new NotImplementedException();
        }

        internal CedarPage FindLocationFor(Slice key)
        {
            CedarPage page;
            if (TryUseRecentTransactionPage(key, out page))
            {
                return page;
            }

            return SearchPageForKey(key);
        }

        internal CedarPage FindLocationFor(Slice key, out CedarCursor cursor)
        {
            CedarPage page;
            if (TryUseRecentTransactionPage(key, out page, out cursor))
                return page;

            return SearchPageForKey(key, out cursor);
        }

        private CedarPage SearchPageForKey(Slice key)
        {
            var cursor = new CedarCursor(_llt, new CedarPage(_llt, State.RootPageNumber));
            cursor.FindLocation(key);

            return cursor.CurrentPage;
        }

        private CedarPage SearchPageForKey(Slice key, out CedarCursor cursor)
        {
            cursor = new CedarCursor(_llt, new CedarPage(_llt, State.RootPageNumber));
            cursor.FindLocation(key);

            return cursor.CurrentPage;
        }

        private bool TryUseRecentTransactionPage(Slice key, out CedarPage page, out CedarCursor cursor)
        {
            page = null;
            cursor = null;

            var foundPage = _recentlyFoundPages?.Find(key);
            if (foundPage == null)
                return false;

            // This is the page where the header lives.                        
            if (foundPage.Page.IsBranch)
                throw new InvalidDataException("Index points to a non leaf page");

            page = new CedarPage(_llt, foundPage.Number, foundPage.Page);
            cursor = new CedarCursor(_llt, page, foundPage.CursorPath);

            return true;
        }

        private bool TryUseRecentTransactionPage(Slice key, out CedarPage page)
        {
            page = null;

            var foundPage = _recentlyFoundPages?.Find(key);
            if (foundPage == null)
                return false;

            // This is the page where the header lives.                        
            if (foundPage.Page.IsBranch)
                throw new InvalidDataException("Index points to a non leaf page");

            page = new CedarPage(_llt, foundPage.Number, foundPage.Page);

            return true;
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

        public void ClearRecentFoundPages()
        {
            throw new NotImplementedException();
        }
    }
}
