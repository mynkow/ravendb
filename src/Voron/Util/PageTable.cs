using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Collections.LockFree;
using Voron.Impl;

namespace Voron.Util
{
    [StructLayout(LayoutKind.Explicit, Size = SizeOf)]    
    public unsafe struct PageTranslationPosition
    {
        private const long Invalid = -1;
        public const int SizeOf = 24;        
        public const int HighestBitMask = 0x8000000;

        [FieldOffset(0)]
        public long TransactionId;

        [FieldOffset(8)]
        private int _journalNumber;
        [FieldOffset(12)]
        private int _scratchNumber;

        [FieldOffset(16)]
        public long ScratchPos;

        public PageTranslationPosition(long scratchPos, long transactionId, int journalNumber, int scratchNumber, bool isFreedPageMarker = false)
        {
            Debug.Assert((journalNumber & HighestBitMask) == 0);
            
            this.TransactionId = transactionId;
            this.ScratchPos = scratchPos;
            this._journalNumber = isFreedPageMarker ? journalNumber | HighestBitMask : journalNumber;
            this._scratchNumber = scratchNumber;
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return TransactionId >= 0; }
        }

        public int JournalNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _journalNumber & ~HighestBitMask; }
        }

        public bool IsFreePage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_journalNumber & HighestBitMask) != 0; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (value)
                {
                    // Set the highest bit to true.
                    _journalNumber |= HighestBitMask;
                }
                else
                {
                    // Set the highest bit to zero.
                    _journalNumber &= ~HighestBitMask;
                }
            }
        }

        public int ScratchNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _scratchNumber & ~HighestBitMask; }
        }

        public bool IsUnused
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_scratchNumber & HighestBitMask) != 0; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (value)
                {
                    // Set the highest bit to true.
                    _scratchNumber |= HighestBitMask;
                }
                else
                {
                    // Set the highest bit to zero.
                    _scratchNumber &= ~HighestBitMask;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Reset(PageTranslationPosition* instance)
        {
            instance->TransactionId = Invalid;
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
    public unsafe struct PageTranslation
    { 
        public const int TableSize = 3;
        public const int SizeOf = 4 + TableSize * PageTranslationPosition.SizeOf;

        /// <summary>
        /// If the count is positive, then it is a count of the current used table data. 
        /// If the count is negative, then it is the offset in absolute value for the fallback structure holder.
        /// </summary>
        [FieldOffset(0)]
        public int Count;        
        
        public bool HasFallback
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Count < 0; }
        }
        
        public int FallbackOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasFallback); // We are calling this only if we have checked that we have a fallback structure in place.
                return -Count - 1;
            }
            set
            {
                Debug.Assert(!HasFallback); // We are not going to overwrite the fallback
                Count = -(value + 1);
                Debug.Assert(FallbackOffset == value); // We are retrieving the same value that we stored.
            }
        }

        [FieldOffset(4)]
        private fixed byte _table[TableSize * PageTranslationPosition.SizeOf];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PageTranslationPosition* GetPosition(PageTranslation* instance, int offset)
        {
            Debug.Assert(offset >= 0 && offset < TableSize);
            return (PageTranslationPosition*)instance->_table + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PageTranslationPosition* GetTable(PageTranslation* instance)
        {
            return (PageTranslationPosition*)instance->_table;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Reset(PageTable owner, PageTranslation* instance)
        {
            if (instance->HasFallback)
            {
                owner.ReturnFallbackStorage(instance->FallbackOffset);
            }

            instance->Count = 0;

            PageTranslationPosition* table = GetTable(instance);
            for (int i = 0; i < TableSize; i++)
            {
                PageTranslationPosition.Reset(table + i);
            }
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
    public unsafe struct PageBlock
    {
        public const int SizeOf = 28 + TableSize * PageTranslation.SizeOf;
        
        /// <summary>
        /// With 32 elements in the table we know that each PageBlock would handle all the allocated pages 
        /// within the boundaries of 256Kb because each page is 8Kb long and have an extra memory cost of 
        /// roughly 2460 bytes per active section. 
        /// </summary>
        public const int TableSize = 32;

        /// <summary>
        /// This is the shift necessary to isolate the higher level index.
        /// </summary>
        public const int BlockShift = 5;

        /// <summary>
        /// This is the mask necessary to isolate the lower level index.
        /// </summary>
        public const int BlockMask = 0x001F;

        private const long Invalid = -1;
        
        /// <summary>
        /// The maximum transaction seen on this page block.
        /// </summary>
        [FieldOffset(0)]
        public long MaxTransactionId;

        /// <summary>
        /// The minimum transaction seen on this page block. This is useful to know if we need to scan this block to find transactions to remove.
        /// </summary>
        [FieldOffset(8)]
        public long MinTransactionId;

        /// <summary>
        /// The base page this page block is addressing.
        /// </summary>
        [FieldOffset(16)]
        public long BasePage;

        /// <summary>
        /// This keep tracks of the amount of used translations at this block. When it reaches zero, we can safely dispose or return it to the pool.
        /// </summary>
        [FieldOffset(24)]
        public int Count;

        [FieldOffset(28)]
        private fixed byte _table[TableSize * PageTranslation.SizeOf];

        public bool IsValid => MaxTransactionId != Invalid;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PageTranslation* GetTranslation(PageBlock* instance, int offset)
        {
            Debug.Assert(offset >= 0 && offset < TableSize);
            return (PageTranslation*)instance->_table + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PageTranslation* GetTable(PageBlock* instance)
        {
            return (PageTranslation*)instance->_table;
        }
        
        public static void Reset(PageTable owner, PageBlock* instance)
        {
            instance->MaxTransactionId = Invalid;
            instance->MinTransactionId = Invalid;
            instance->BasePage = Invalid;
            instance->Count = 0;

            PageTranslation* table = GetTable(instance);
            for (int i = 0; i < TableSize; i++)
            {
                PageTranslation.Reset(owner, table + i);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Initialize(PageBlock* instance, long basePage)
        {
            instance->BasePage = basePage;
        }
    }

    public sealed unsafe class PageTranslationHashBlock
    {                
        private PageTable _owner;
        private PageBlock _table = default(PageBlock);

        public void Initialize(PageTable owner, long basePage)
        {
            fixed (PageBlock* block = &_table)
            {
                PageBlock.Initialize(block, basePage);
            }
        }

        public void Reset()
        {
            fixed (PageBlock* block = &_table)
            {
                PageBlock.Reset(_owner, block);
            }
        }
    }

    /// <summary>
    /// This class assumes a single writer and many readers
    /// </summary>
    public sealed unsafe class PageTable : IDisposable
    {
        //// We are indexing based on page number
        //private readonly ConcurrentDictionary<long, PagesBuffer> _values = new ConcurrentDictionary<long, PagesBuffer>(NumericEqualityComparer.Instance);
        //private readonly SortedList<long, Dictionary<long, PagePosition>> _transactionPages = new SortedList<long, Dictionary<long, PagePosition>>();


        /// <summary>
        /// We are indexing based on block number. A block number is the higher part of the page number (aka pageNumber >> log2(PageBlock.TableSize) ) 
        /// </summary>       
        private readonly ConcurrentDictionary<long, PageTranslationHashBlock> _pageTranslationTable = new ConcurrentDictionary<long, PageTranslationHashBlock>(NumericEqualityComparer.Instance);
        private readonly SortedList<long, Dictionary<long, PagePosition>> _transactionPages = new SortedList<long, Dictionary<long, PagePosition>>();

        private readonly object _writeSync = new object();
        
        private long _maxSeenTransaction;

        //private class PagesBuffer
        //{
        //    // There is an implicit transaction id here. 
        //    public readonly PagePosition[] PagePositions;
        //    public int Start, End;

        //    public PagesBuffer(PagePosition[] buffer, PagesBuffer previous)
        //    {
        //        PagePositions = buffer;
        //        if (previous == null)
        //            return;
        //        End = previous.End - previous.Start;
        //        Array.Copy(previous.PagePositions, previous.Start, PagePositions, 0, End);
        //    }

        //    public int Count
        //    {
        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        get { return End - Start; }
        //    }

        //    public bool CanAdd
        //    {
        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        get { return End < PagePositions.Length; }
        //    }

        //    public int Capacity
        //    {
        //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //        get { return PagePositions.Length; }
        //    }

        //    public void Add(PagePosition p)
        //    {
        //        PagePositions[End++] = p;
        //    }

        //    public void RemoveBefore(long lastSyncedTransactionId, List<PagePosition> unusedPages)
        //    {
        //        while (
        //            Start < PagePositions.Length &&
        //            PagePositions[Start] != null &&
        //            PagePositions[Start].TransactionId <= lastSyncedTransactionId
        //            )
        //        {
        //            unusedPages.Add(PagePositions[Start++]);
        //        }
        //    }
        //}

        public bool IsEmpty => _pageTranslationTable.Count == 0;

        public void SetItems(LowLevelTransaction tx, Dictionary<long, PagePosition> items)
        {                        
            // Under lock we need to update the MaxSeenTransactionId. 
            // We are fine with it being not so current (relaxed semantics) but we should ensure that only higher would succeed.
            lock (_writeSync)
            {
                UpdateMaxSeenTxId(tx);
                
                // Assign the pages used to the transaction storage.. 
                _transactionPages.Add(tx.Id, items);                
            }
            
            foreach (var item in items)
            {
                // REVIEW: All this can be simplified handling the growing logic of the PagePosition inside the PageBuffer class itself.                

                // We must find the proper PageBuffer to add the current PagePosition to the list. 
                // But we heavily rely on the fact that only one thread can update the concurrent dictionary

                
                ///////////////

                //PagesBuffer value;
                //if (_values.TryGetValue(item.Key, out value) == false)
                //{
                //    // We preallocate a page buffer with 2 positions. 
                //    value = new PagesBuffer(new PagePosition[2], null);
                //    _values.TryAdd(item.Key, value);
                //}
                //if (value.CanAdd == false)
                //{
                //    // We grow (doubling the size) the allocated page buffer.
                //    var newVal = new PagesBuffer(new PagePosition[value.Capacity*2], value);
                //    _values.TryUpdate(item.Key, newVal, value);
                //    value = newVal;
                //}
                //value.Add(item.Value);
            }

            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateMaxSeenTxId(LowLevelTransaction tx)
        {
            if (_maxSeenTransaction <= tx.Id)
            {
                _maxSeenTransaction = tx.Id;
                return;                
            }
                
            ThrowTransactionsIdsMustAlwaysIncrease(tx);            
        }

        private void ThrowTransactionsIdsMustAlwaysIncrease(LowLevelTransaction tx)
        {
            throw new InvalidOperationException("Transaction ids has to always increment, but got " + tx.Id +
                                                " when already seen tx " + _maxSeenTransaction);
        }

        public void RemoveKeysWhereAllPagesOlderThan(long lastSyncedTransactionId, FastList<PagePosition> unusedPages)
        {
            // For each transaction that has already being flushed
            //      Return as unused all the pages on the scratch pad that has been flushed.            
            //      Remove the flushed transactions from the pages tracked.

            throw new NotImplementedException();

            ///////////////

            //foreach (var kvp in _values)
            //{
            //    var valueBuffer = kvp.Value;
            //    var position = valueBuffer.PagePositions[valueBuffer.End - 1];
            //    if (position == null)
            //        continue;

            //    if (position.TransactionId > lastSyncedTransactionId)
            //        continue;

            //    valueBuffer.RemoveBefore(lastSyncedTransactionId, unusedPages);
            //    if (valueBuffer.Count != 0)
            //        continue;

            //    PagesBuffer _;
            //    _values.TryRemove(kvp.Key,out _);
            //}
        }
        
        public bool TryGetValue(LowLevelTransaction tx, long page, out PageTranslationPosition* value)
        {
            throw new NotImplementedException();

            // If the page hasn't been involved in a write transaction lately (or at all), we return false.        

            // for all transactions registered for this page 
            //   if the transaction id is higher than the current transaction, it is of no use for us.
            //   if the page has already being removed, we are done and return false.
            //   else we found the highest marked transaction and return that scratch page.

            // else all the current values are _after_ this transaction started, so it sees nothing
            value = null;
            return false;

            ///////////////

            //PagesBuffer bufferHolder;
            //if (_values.TryGetValue(page, out bufferHolder) == false )
            //{
            //    value = null;
            //    return false;
            //}

            //var bufferStart = bufferHolder.Start;
            //var bufferPagePositions = bufferHolder.PagePositions;

            //for (int i = bufferHolder.End - 1; i >= bufferStart; i--)
            //{
            //    var position = bufferPagePositions[i];
            //    if (position == null || position.TransactionId > tx.Id)
            //        continue;

            //    if (position.IsFreedPageMarker)
            //        break;

            //    value = position;
            //    Debug.Assert(value != null);
            //    return true;
            //}

            //// all the current values are _after_ this transaction started, so it sees nothing
            //value = null;
            //return false;
        }

        public long MaxTransactionId()
        {
            // Returns the highest transaction id (more current) write transaction that the journal has received. 
            // Scanning over the whole page table. 
            throw new NotImplementedException();
                        
            //long maxTx = 0;

            //foreach (var bufferHolder in _values.Values)
            //{
            //    var position = bufferHolder.PagePositions[bufferHolder.End - 1];
            //    if (position != null && maxTx < position.TransactionId)
            //        maxTx = position.TransactionId;
            //}

            //return maxTx;
        }

        public long GetLastSeenTransactionId()
        {
            return Volatile.Read(ref _maxSeenTransaction);
        }

        // TODO: The signature of the method must change to account for the unified. 
        public FastList<Dictionary<long, PageTranslationPosition>> GetModifiedPagesForTransactionRange(long minTxInclusive, long maxTxInclusive)
        {
            // We will scan all the PPT hash blocks involved in those transactions and extract all the pages modified by them.            
                        
            // Get a merged view of all the pages used by a range of transactions and return only the last written buffer in between those transactions.
            throw new NotImplementedException();
            
            //var list = new List<Dictionary<long, PagePosition>>();
            //lock (_transactionPages)
            //{
            //    var start = _transactionPages.IndexOfKey(minTxInclusive);
            //    if (start == -1)
            //    {
            //        for (long i = minTxInclusive + 1; i <= maxTxInclusive; i++)
            //        {
            //            start = _transactionPages.IndexOfKey(i);
            //            if (start != -1)
            //                break;
            //        }
            //    }
            //    if (start != -1)
            //    {
            //        for (int i = start; i < _transactionPages.Count; i++)
            //        {
            //            if (_transactionPages.Keys[i] > maxTxInclusive)
            //                break;

            //            var val = _transactionPages.Values[i];
            //            list.Add(val);
            //        }
            //    }
            //}
            //return list;
        }

        public struct PageTranslationHashBlockResetBehavior : IResetSupport<PageTranslationHashBlock>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void IResetSupport<PageTranslationHashBlock>.Reset(PageTranslationHashBlock value)
            {
                value.Reset();
            }
        }

        private const int FallbackPoolSize = 50;
        private static readonly ObjectPool<PageTranslationHashBlock, PageTranslationHashBlockResetBehavior> _fallbackPool = new ObjectPool<PageTranslationHashBlock, PageTranslationHashBlockResetBehavior>( () => new PageTranslationHashBlock(), FallbackPoolSize);

        private int _currentStorageBlockId = -1;
        private readonly ConcurrentDictionary<int, PageTranslationHashBlock> _storageBlocks = new ConcurrentDictionary<int, PageTranslationHashBlock>();
        
        internal int LeaseFallbackStorage(long basePage)
        {
            int allocatedId = Interlocked.Increment(ref _currentStorageBlockId);
            
            // This can happen only when a write transaction happens. 
            var storage = _fallbackPool.Allocate();
            storage.Initialize(this, basePage);
            _storageBlocks[allocatedId] = storage;
            
            return allocatedId;
        }

        internal void ReturnFallbackStorage(int storageId)
        {
            // This can happen only when a flush/sync happens. 
            if (_storageBlocks.TryRemove(storageId, out PageTranslationHashBlock storage))
            {
                _fallbackPool.Free(storage);
            }
        }

        public void Dispose()
        {            
            int i = 0;
            foreach (var item in _storageBlocks)
            {
                if (i >= FallbackPoolSize) 
                    break; // When the size of the object pool is hit, no need to free any more block.

                _fallbackPool.Free(item.Value);

                i++;
            }                
        }
    }
}