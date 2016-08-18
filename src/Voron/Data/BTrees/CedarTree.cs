using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public unsafe class CedarTree
    {
        private readonly CedarMutableState _state;

        private enum ActionType
        {
            Add,
            Delete
        }

        //public event Action<long> PageModified;
        //public event Action<long> PageFreed;

        private readonly RecentlyFoundCedarPages _recentlyFoundPages;

        public string Name { get; set; }

        public CedarMutableState State
        {
            get { return _state; }
        }

        private readonly LowLevelTransaction _llt;
        private readonly Transaction _tx;

        public LowLevelTransaction Llt
        {
            get { return _llt; }
        }

        public enum OperationStatus
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
            /// Items count is too small for this page to sustain. 
            /// </summary>
            TooMuchSpace,                    
        }      

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

            var newRootPage = Initialize(llt);

            var tree = new CedarTree(llt, tx, newRootPage.PageNumber)
            {
                _state =
                {
                    Depth = 1,
                    Flags = flags,
                    InWriteTransaction = true,
                }
            };

            return tree;
        }

        private static Page Initialize(LowLevelTransaction tx)
        {
            throw new NotImplementedException();
        }
        
        public void Add(string key, long value, ushort? version = null)
        {
            Add(Slice.From( _tx.Allocator, key), value, version);
        }

        public void Add(Slice key, long value, ushort? version = null)
        {
            State.IsModified = true;
            var pos = DirectAdd(key, version);

            // TODO: Check how to write this (endianess).
            *((long*)pos) = value;                                               
        }

        public long Read(Slice key)
        {
            var pos = DirectRead(key);

            // TODO: Check how to write this (endianess).
            return *((long*)pos);
        }

        private static void CopyStreamToPointer(LowLevelTransaction tx, Stream value, byte* pos)
        {
            TemporaryPage tmp;
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
            {
                var tempPageBuffer = tmp.TempPageBuffer;
                var tempPagePointer = tmp.TempPagePointer;
                while (true)
                {
                    var read = value.Read(tempPageBuffer, 0, tempPageBuffer.Length);
                    if (read == 0)
                        break;

                    Memory.CopyInline(pos, tempPagePointer, read);
                    pos += read;

                    if (read != tempPageBuffer.Length)
                        break;
                }
            }
        }

        public byte* DirectRead(Slice key)
        {
            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            // We look for the branch page that is going to host this data. 
            CedarBranchPageHeader* node;
            CedarCursor cursor = FindLocationFor(key, out node);

            // This is efficient because we return the very same Slice so checking can be done via pointer comparison. 
            if (cursor.Key.Same(key))
            {
                // We will be able to overwrite the data if the data fit into the allocated space or if we have smaller than 8 bytes data to store.                       
                return GetDataPointer(cursor);
            }

            // If this triggers it is signaling a defect on the cursor implementation
            // This is a checked invariant on debug builds. 
            Debug.Assert(!cursor.Key.Equals(key));

            return null;
        }

        public byte* DirectAdd(Slice key, ushort? version = null)
        {
            if (State.InWriteTransaction)
                State.IsModified = true;

            if (_llt.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot add a value in a read only transaction");

            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            // We look for the branch page that is going to host this data. 
            CedarBranchPageHeader* node;
            CedarCursor cursor = FindLocationFor(key, out node);

            // This is efficient because we return the very same Slice so checking can be done via pointer comparison. 
            byte* pos;            
            if (cursor.Key.Same(key)) 
            {
                // This is an update operation (key was found).               
                CheckConcurrency(key, version, cursor.NodeVersion, ActionType.Add);

                // We will be able to overwrite the data if the data fit into the allocated space or if we have smaller than 8 bytes data to store.                       
                return GetDataPointer(cursor);
            }

            // If this triggers it is signaling a defect on the cursor implementation
            // This is a checked invariant on debug builds. 
            Debug.Assert(!cursor.Key.Equals(key));

            // Updates may fail if we have to split the page. 
            OperationStatus status;
            do
            {
                // It will output the position of the data to be written to. 
                status = TryUpdate(cursor, key, version, out pos);
                if (status == OperationStatus.NotEnoughSpace)
                {
                    // We need to split because there is not enough space available to add this key into the page.
                    var pageSplitter = new CedarPageSplitter(_llt, this, cursor);
                    cursor = pageSplitter.Execute();
                }
            }
            while (status != OperationStatus.Success);

            // Record the new entry.
            State.NumberOfEntries++;

            return pos;
        }

        public CedarIterator Iterate(bool prefetch)
        {
            throw new NotImplementedException();
        }

        internal CedarCursor FindLocationFor(Slice key, out CedarBranchPageHeader* node)
        {
            throw new NotImplementedException();
        }

        private OperationStatus TryUpdate(CedarCursor cursor, Slice key, ushort? version, out byte* pos)
        {
            throw new NotImplementedException();
        }

        private void CheckConcurrency(Slice key, ushort? version, ushort nodeVersion, ActionType add)
        {
            throw new NotImplementedException();
        }

        private byte* GetDataPointer(CedarCursor cursor)
        {
            throw new NotImplementedException();
        }
    }
}
