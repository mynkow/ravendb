using Bond;
using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using static Voron.Data.Compact.PrefixTree;

namespace Voron.Data.Compact
{
    partial class PrefixTree
    {
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 16)]
        internal unsafe struct Entry
        {
            [FieldOffset(0)]
            public uint Hash;

            [FieldOffset(4)]
            public uint Signature;

            [FieldOffset(8)]
            public long NodePtr;

            public Entry(uint hash, uint signature, long nodePtr)
            {
                this.Hash = hash;
                this.Signature = signature;
                this.NodePtr = nodePtr;
            }
        }
    }

    unsafe partial class PrefixTree<TValue>
    {
        internal sealed class InternalTable
        {
            private readonly PrefixTree<TValue> owner;

            public const int InvalidNodePosition = -1;

            private const uint kDeleted = 0xFFFFFFFE;
            private const uint kUnused = 0xFFFFFFFF;
            private const long kInvalidNode = -1;

            private const uint kHashMask = 0xFFFFFFFE;
            private const uint kSignatureMask = 0x7FFFFFFE;
            private const uint kDuplicatedMask = 0x80000000;

            /// <summary>
            /// By default, if you don't specify a hashtable size at construction-time, we use this size.  Must be a power of two, and at least kMinCapacity.
            /// </summary>
            private const int kInitialCapacity = 64;

            /// <summary>
            /// By default, if you don't specify a hashtable size at construction-time, we use this size.  Must be a power of two, and at least kMinCapacity.
            /// </summary>
            private const int kMinCapacity = 4;

            // TLoadFactor4 - controls hash map load. 4 means 100% load, ie. hashmap will grow
            // when number of items == capacity. Default value of 6 means it grows when
            // number of items == capacity * 3/2 (6/4). Higher load == tighter maps, but bigger
            // risk of collisions.
            public const int LoadFactor = 6;

            private Entry* _entries;

            private PrefixTreeTablePageHeader* _header;

            /// <summary>
            /// The current capacity of the dictionary
            /// </summary>
            public int Capacity
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._header->Capacity; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._header->Capacity = value; }
            }

            /// <summary>
            /// This is the real counter of how many items are in the hash-table (regardless of buckets)
            /// </summary>
            public int Count
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._header->Size; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._header->Size = value; }
            }

            /// <summary>
            /// This is the initial capacity of the dictionary, we will never shrink beyond this point.
            /// </summary>
            private int InitialCapacity
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._header->InitialCapacity; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._header->InitialCapacity = value; }
            }

            /// <summary>
            /// How many used buckets. 
            /// </summary>
            private int NumberOfUsed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._header->NumberOfUsed; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._header->NumberOfUsed = value; }
            }

            /// <summary>
            /// How many occupied buckets are marked deleted
            /// </summary>
            private int NumberOfDeleted
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._header->NumberOfDeleted; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._header->NumberOfDeleted = value; }
            }

            /// <summary>
            /// The next growth threshold. 
            /// </summary>
            private int NextGrowthThreshold
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._header->NextGrowthThreshold; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._header->NextGrowthThreshold = value; }
            }

            public InternalTable(PrefixTree<TValue> owner, int initialBucketCount = 256)
            {
                this.owner = owner;

                int newCapacity = initialBucketCount >= kMinCapacity ? initialBucketCount : kMinCapacity;
                this._header = this.owner.Initialize(newCapacity);
                this._entries = this.owner.LoadEntries(_header);
            }

            public InternalTable(PrefixTree<TValue> owner, PrefixTreeTablePageHeader* header)
            {
                this.owner = owner;

                this._header = header;
                this._entries = owner.LoadEntries(_header);
            }

            public void Add(long nodePtr, uint signature)
            {
                ResizeIfNeeded();

                // We shrink the signature to the proper size (31 bits)
                signature = signature & kSignatureMask;

                int hash = (int)(signature & kHashMask);
                int bucket = hash % Capacity;

                uint uhash = (uint)hash;
                int numProbes = 1;
                do
                {
                    if (_entries[bucket].Signature == signature)
                        _entries[bucket].Signature |= kDuplicatedMask;

                    uint nHash = _entries[bucket].Hash;
                    if (nHash == kUnused)
                    {
                        NumberOfUsed++;
                        Count++;

                        goto SET;
                    }
                    else if (nHash == kDeleted)
                    {
                        NumberOfDeleted--;
                        Count++;

                        goto SET;
                    }

                    bucket = (bucket + numProbes) % Capacity;
                    numProbes++;
                }
                while (true);

                SET:
                this._entries[bucket].Hash = uhash;
                this._entries[bucket].Signature = signature;
                this._entries[bucket].NodePtr = nodePtr;

#if DETAILED_DEBUG_H
                Console.WriteLine(string.Format("Add: {0}, Bucket: {1}, Signature: {2}", node.ToDebugString(this.owner), bucket, signature));
#endif
#if VERIFY
                VerifyStructure();
#endif
            }

            public void Remove(long nodePtr, uint signature)
            {
                // We shrink the signature to the proper size (30 bits)
                signature = signature & kSignatureMask;

                int hash = (int)(signature & kHashMask);
                int bucket = hash % Capacity;

                var entries = _entries;

                int lastDuplicated = -1;
                uint numProbes = 1; // how many times we've probed

                do
                {
                    if ((entries[bucket].Signature & kSignatureMask) == signature)
                        lastDuplicated = bucket;

                    if (entries[bucket].NodePtr == nodePtr)
                    {
                        // This is the last element and is not a duplicate, therefore the last one is not a duplicate anymore. 
                        if ((entries[bucket].Signature & kDuplicatedMask) == 0 && lastDuplicated != -1)
                            entries[bucket].Signature &= kSignatureMask;

                        if (entries[bucket].Hash < kDeleted)
                        {

#if DETAILED_DEBUG_H
                            Console.WriteLine(string.Format("Remove: {0}, Bucket: {1}, Signature: {2}", node.ToDebugString(this.owner), bucket, signature));
#endif

                            entries[bucket].Hash = kDeleted;
                            entries[bucket].Signature = kUnused;
                            entries[bucket].NodePtr = kInvalidNode;

                            NumberOfDeleted++;
                            Count--;
                        }

                        Contract.Assert(NumberOfDeleted >= Contract.OldValue<int>(NumberOfDeleted));
                        Contract.Assert(entries[bucket].Hash == kDeleted);
                        Contract.Assert(entries[bucket].Signature == kUnused);

                        if (3 * this.NumberOfDeleted / 2 > this.Capacity - this.NumberOfUsed)
                        {
                            // We will force a rehash with the growth factor based on the current size.
                            Shrink(Math.Max(InitialCapacity, Count * 2));
                        }

                        return;
                    }

                    bucket = (int)((bucket + numProbes) % Capacity);
                    numProbes++;

                    Debug.Assert(numProbes < 100);
                }
                while (entries[bucket].Hash != kUnused);
            }

            public void Replace(long oldNodePtr, long newNodePtr, uint signature)
            {
                // We shrink the signature to the proper size (30 bits)
                signature = signature & kSignatureMask;

                int hash = (int)(signature & kHashMask);
                int pos = hash % Capacity;

                int numProbes = 1;

                while (this._entries[pos].NodePtr != oldNodePtr)
                {
                    pos = (pos + numProbes) % Capacity;
                    numProbes++;
                }

                AssertReplace(pos, hash, newNodePtr);

                this._entries[pos].NodePtr = newNodePtr;

#if DETAILED_DEBUG_H
                Console.WriteLine(string.Format("Old: {0}, Bucket: {1}, Signature: {2}", oldNode.ToDebugString(this.owner), pos, hash, signature));
                Console.WriteLine(string.Format("New: {0}", newNode.ToDebugString(this.owner)));
#endif

#if VERIFY
                VerifyStructure();
#endif

            }

            [Conditional("DEBUG")]
            public void AssertReplace(int pos, int hash, long newNodePtr)
            {
                Debug.Assert(this._entries[pos].NodePtr != kInvalidNode);
                Debug.Assert(this._entries[pos].Hash == (uint)hash);

                var node = (Node*)this.owner.ReadDirect(this._entries[pos].NodePtr);
                var newNode = (Node*)this.owner.ReadDirect(newNodePtr);
                Debug.Assert(this.owner.Handle(node).CompareTo(this.owner.Handle(newNode)) == 0);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetExactPosition(BitVector key, int prefixLength, uint signature)
            {
                signature = signature & kSignatureMask;

                int pos = (int)(signature & kHashMask) % Capacity;

                int numProbes = 1;

                uint nSignature;
                do
                {
                    nSignature = this._entries[pos].Signature;

                    if ((nSignature & kSignatureMask) == signature)
                    {
                        Node* node = (Node*)this.owner.ReadDirect(this._entries[pos].NodePtr);
                        if (this.owner.GetExtentLength(node) == prefixLength)
                        {
                            Node* referenceNodePtr = (Node*)this.owner.ReadDirect(node->ReferencePtr);
                            if ( key.IsPrefix(this.owner.Name(referenceNodePtr), prefixLength))
                                return pos;
                        }                            
                    }

                    pos = (pos + numProbes) % Capacity;
                    numProbes++;

                    Debug.Assert(numProbes < 100);
                }
                while (this._entries[pos].Hash != kUnused);

                return -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetPosition(BitVector key, int prefixLength, uint signature)
            {
                signature = signature & kSignatureMask;

                int pos = (int)(signature & kHashMask) % Capacity;

                int numProbes = 1;

                uint nSignature;
                do
                {
                    nSignature = this._entries[pos].Signature;

                    if ((nSignature & kSignatureMask) == signature)
                    {                        
                        var node = (Node*)this.owner.ReadDirect(this._entries[pos].NodePtr);
                        if ((nSignature & kDuplicatedMask) == 0) 
                            return pos;
                        
                        if (this.owner.GetExtentLength(node) == prefixLength)
                        {
                            Node* referenceNodePtr = (Node*)this.owner.ReadDirect(node->ReferencePtr);
                            if (key.IsPrefix(this.owner.Name(referenceNodePtr), prefixLength))
                                return pos;
                        }

                    }

                    pos = (pos + numProbes) % Capacity;
                    numProbes++;

                    Debug.Assert(numProbes < 100);
                }
                while (this._entries[pos].Hash != kUnused);

                return -1;
            }

            public long this[int position]
            {
                get
                {
                    return this._entries[position].NodePtr;
                }
            }

            internal string DumpNodesTable(PrefixTree<TValue> tree)
            {
                var builder = new StringBuilder();

                bool first = true;
                builder.Append("After Insertion. NodesTable: {");
                foreach (var node in this.Values)
                {
                    if (!first)
                        builder.Append(", ");
                    
                    var copyOfNode = (Node*)this.owner.ReadDirect( node );

                    builder.Append(tree.Handle(copyOfNode).ToDebugString())
                           .Append(" => ")
                           .Append(tree.ToDebugString(copyOfNode));

                    first = false;
                }
                builder.Append("} Root: ")
                       .Append(tree.ToDebugString( tree.Root ));

                return builder.ToString();
            }

            internal string DumpTable()
            {
                var builder = new StringBuilder();

                builder.AppendLine("NodesTable: {");

                for (int i = 0; i < this.Capacity; i++)
                {
                    var entry = this._entries[i];
                    if (entry.Hash != kUnused)
                    {
                        var node = (Node*)this.owner.ReadDirect(entry.NodePtr);

                        builder.Append("Signature:")
                               .Append(entry.Signature & kSignatureMask)
                               .Append((entry.Signature & kDuplicatedMask) != 0 ? "-dup" : string.Empty)
                               .Append(" Hash: ")
                               .Append(entry.Hash)
                               .Append(" Node: ")
                               .Append(this.owner.Handle(node).ToDebugString())
                               .Append(" => ")
                               .Append(this.owner.ToDebugString(node))
                               .AppendLine();
                    }
                }

                builder.AppendLine("}");

                return builder.ToString();
            }

            public KeyCollection Keys
            {
                get { return new KeyCollection(this); }
            }

            public ValueCollection Values
            {
                get { return new ValueCollection(this); }
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ResizeIfNeeded()
            {
                if (Count >= NextGrowthThreshold)
                {
                    Grow(Capacity * 2);
                }
            }

            private void Grow(int newCapacity)
            {
                Contract.Requires(newCapacity >= Capacity);
                Contract.Ensures((Capacity & (Capacity - 1)) == 0);

                var newHeader = this.owner.Initialize(newCapacity);
                var newEntries = this.owner.LoadEntries(newHeader);

                Rehash(newHeader, newEntries);

                this._header = newHeader;
                this._entries = newEntries;
            }

            private void Shrink(int newCapacity)
            {
                Contract.Requires(newCapacity > Count);
                Contract.Ensures(this.NumberOfUsed < this.Capacity);

                // Calculate the next power of 2.
                newCapacity = Math.Max(Bits.NextPowerOf2(newCapacity), InitialCapacity);

                var newHeader = this.owner.Initialize(newCapacity);
                var newEntries = this.owner.LoadEntries(newHeader);

                Rehash(newHeader, newEntries);

                this._header = newHeader;
                this._entries = newEntries;
            }

            private void Rehash(PrefixTreeTablePageHeader* header, Entry* entries)
            {
                uint capacity = (uint)header->Capacity;

                var size = 0;                
                for (int it = 0; it < capacity; it++)
                {
                    uint hash = _entries[it].Hash;
                    if (hash >= kDeleted) // No interest for the process of rehashing, we are skipping it.
                        continue;

                    uint signature = _entries[it].Signature & kSignatureMask;

                    uint bucket = hash % capacity;

                    uint numProbes = 1;
                    while (!(entries[bucket].Hash == kUnused))
                    {
                        if (entries[bucket].Signature == signature)
                            entries[bucket].Signature |= kDuplicatedMask;

                        bucket = (bucket + numProbes) % capacity;
                        numProbes++;
                    }

                    entries[bucket].Hash = hash;
                    entries[bucket].Signature = signature;
                    entries[bucket].NodePtr = _entries[it].NodePtr;

                    size++;
                }

                header->Capacity = (int)capacity;
                header->Size = size;

                header->NumberOfUsed = size;
                header->NumberOfDeleted = 0;
                header->NextGrowthThreshold = (int)capacity * 4 / LoadFactor;
            }


            public sealed class KeyCollection : IEnumerable<BitVector>, IEnumerable
            {
                private InternalTable dictionary;

                public KeyCollection(InternalTable dictionary)
                {
                    Contract.Requires(dictionary != null);

                    this.dictionary = dictionary;
                }

                public Enumerator GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                public void CopyTo(BitVector[] array, int index)
                {
                    if (array == null)
                        throw new ArgumentNullException(nameof(array), "The array cannot be null");

                    if (index < 0 || index > array.Length)
                        throw new ArgumentOutOfRangeException(nameof(index));

                    if (array.Length - index < dictionary.Count)
                        throw new ArgumentException("The array plus the offset is too small.");

                    int count = dictionary.Capacity;
                    var entries = dictionary._entries;

                    for (int i = 0; i < count; i++)
                    {
                        if (entries[i].Hash < kDeleted)
                        {
                            var node = (Node*)dictionary.owner.ReadDirect(entries[i].NodePtr);
                            array[index++] = dictionary.owner.Handle(node);
                        }                            
                    }
                }

                public int Count
                {
                    get { return dictionary.Count; }
                }


                IEnumerator<BitVector> IEnumerable<BitVector>.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }


                [Serializable]
                public struct Enumerator : IEnumerator<BitVector>, IEnumerator
                {
                    private InternalTable dictionary;
                    private int index;
                    private BitVector currentKey;

                    internal Enumerator(InternalTable dictionary)
                    {
                        this.dictionary = dictionary;
                        index = 0;
                        currentKey = default(BitVector);
                    }

                    public void Dispose()
                    {
                    }

                    public bool MoveNext()
                    {
                        var count = dictionary.Capacity;

                        var entries = dictionary._entries;
                        while (index < count)
                        {
                            if (entries[index].Hash < kDeleted)
                            {
                                var node = (Node*)dictionary.owner.ReadDirect(entries[index].NodePtr);
                                currentKey = dictionary.owner.Handle(node);
                                index++;

                                return true;
                            }
                            index++;
                        }

                        index = count + 1;
                        currentKey = default(BitVector);
                        return false;
                    }

                    public BitVector Current
                    {
                        get
                        {
                            return currentKey;
                        }
                    }

                    Object System.Collections.IEnumerator.Current
                    {
                        get
                        {
                            if (index == 0 || (index == dictionary.Count + 1))
                                throw new InvalidOperationException("Cant happen.");

                            return currentKey;
                        }
                    }

                    void System.Collections.IEnumerator.Reset()
                    {
                        index = 0;
                        currentKey = default(BitVector);
                    }
                }
            }



            public sealed class ValueCollection : IEnumerable<long>, IEnumerable
            {
                private InternalTable dictionary;

                public ValueCollection(InternalTable dictionary)
                {
                    Contract.Requires(dictionary != null);

                    this.dictionary = dictionary;
                }

                public Enumerator GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                public void CopyTo(long[] array, int index)
                {
                    if (array == null)
                        throw new ArgumentNullException(nameof(array), "The array cannot be null");

                    if (index < 0 || index > array.Length)
                        throw new ArgumentOutOfRangeException(nameof(index));

                    if (array.Length - index < dictionary.Count)
                        throw new ArgumentException("The array plus the offset is too small.");

                    int count = dictionary.Capacity;

                    var entries = dictionary._entries;
                    for (int i = 0; i < count; i++)
                    {
                        if (entries[i].Hash < kDeleted)
                            array[index++] = entries[i].NodePtr;
                    }
                }

                public int Count
                {
                    get { return dictionary.Count; }
                }

                IEnumerator<long> IEnumerable<long>.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }


                [Serializable]
                public struct Enumerator : IEnumerator<long>, IEnumerator
                {
                    private InternalTable dictionary;
                    private int index;
                    private long currentValue;

                    internal Enumerator(InternalTable dictionary)
                    {
                        this.dictionary = dictionary;
                        index = 0;
                        currentValue = kInvalidNode;
                    }

                    public void Dispose()
                    {
                    }

                    public bool MoveNext()
                    {
                        var count = dictionary.Capacity;

                        var entries = dictionary._entries;
                        while (index < count)
                        {
                            if (entries[index].Hash < kDeleted)
                            {
                                currentValue = entries[index].NodePtr;
                                index++;
                                return true;
                            }
                            index++;
                        }

                        index = count + 1;
                        currentValue = kInvalidNode;
                        return false;
                    }

                    public long Current
                    {
                        get
                        {
                            return currentValue;
                        }
                    }
                    Object IEnumerator.Current
                    {
                        get
                        {
                            if (index == 0 || (index == dictionary.Count + 1))
                                throw new InvalidOperationException("Cant happen.");

                            return currentValue;
                        }
                    }

                    void IEnumerator.Reset()
                    {
                        index = 0;
                        currentValue = kInvalidNode;
                    }
                }
            }

            internal static uint CalculateHashForBits(BitVector vector, Hashing.Iterative.XXHash32Block state, int length = int.MaxValue, int lcp = int.MaxValue)
            {
                length = Math.Min(vector.Count, length); // Ensure we use the proper value.

                int words = length / BitVector.BitsPerWord;
                int remaining = length % BitVector.BitsPerWord;

                ulong remainingWord = 0;
                int shift = 0;
                if (remaining != 0)
                {
                    remainingWord = vector.GetWord(words); // Zero addressing ensures we get the next byte.
                    shift = BitVector.BitsPerWord - remaining;
                }

                unsafe
                {
                    fixed (ulong* bitsPtr = vector.Bits)
                    {
                        uint hash = Hashing.Iterative.XXHash32.CalculateInline((byte*)bitsPtr, words * sizeof(ulong), state, lcp / BitVector.BitsPerByte);

                        // #if ALTERNATIVE_HASHING
                        remainingWord = ((remainingWord) >> shift) << shift;
                        ulong intermediate = Hashing.CombineInline(remainingWord, ((ulong)remaining) << 32 | (ulong)hash);

                        hash = (uint)intermediate ^ (uint)(intermediate >> 32);
                        //#else
                        //                        uint* combine = stackalloc uint[4];
                        //                        ((ulong*)combine)[0] = ((remainingWord) >> shift) << shift;
                        //                        combine[2] = (uint)remaining;
                        //                        combine[3] = hash;

                        //                        hash = Hashing.XXHash32.CalculateInline((byte*)combine, 4 * sizeof(uint));
                        //#endif

#if DETAILED_DEBUG_H
                        Console.WriteLine(string.Format("\tHash -> Hash: {0}, Remaining: {2}, Bits({1}), Vector:{3}", hash, remaining, remainingWord, vector.SubVector(0, length).ToBinaryString()));
#endif
                        return hash;
                    }
                }
            }

            private void Verify(Func<bool> action)
            {

                if (action() == false)
                    throw new Exception("Fail");
            }

            internal void VerifyStructure()
            {
                int count = 0;
                for (int i = 0; i < this.Capacity; i++)
                {
                    if (this._entries[i].NodePtr != kInvalidNode)
                    {
                        var node = (Node*) this.owner.ReadDirect(this._entries[i].NodePtr);

                        var handle = this.owner.Handle(node);
                        var hashState = Hashing.Iterative.XXHash32.Preprocess(handle.Bits);

                        uint hash = CalculateHashForBits(handle, hashState);

                        int position = GetExactPosition(handle, handle.Count, hash & kSignatureMask);

                        Verify(() => position != -1);
                        Verify(() => this._entries[i].NodePtr == this._entries[position].NodePtr);
                        Verify(() => this._entries[i].Hash == (hash & kHashMask & kSignatureMask));
                        Verify(() => i == position);

                        count++;
                    }
                }

                Verify(() => count == this.Count);

                if (count == 0)
                    return;

                var overallHashes = new HashSet<uint>();
                int start = 0;
                int first = -1;
                while (this._entries[start].NodePtr != kInvalidNode)
                {
                    Verify(() => this._entries[start].Hash != kUnused || this._entries[start].Hash != kDeleted);
                    Verify(() => this._entries[start].Signature != kUnused);

                    start = (start + 1) % Capacity;
                }

                do
                {
                    while (this._entries[start].NodePtr == kInvalidNode)
                    {
                        Verify(() => this._entries[start].Hash == kUnused || this._entries[start].Hash == kDeleted);
                        Verify(() => this._entries[start].Signature == kUnused);

                        start = (start + 1) % Capacity;
                    }

                    if (first == -1)
                        first = start;
                    else if (first == start)
                        break;

                    int end = start;
                    while (this._entries[end].NodePtr != kInvalidNode)
                    {
                        Verify(() => this._entries[end].Hash != kUnused || this._entries[start].Hash != kDeleted);
                        Verify(() => this._entries[end].Signature != kUnused && this._entries[end].Signature != kDuplicatedMask);

                        end = (end + 1) % Capacity;
                    }

                    var hashesSeen = new HashSet<uint>();
                    var signaturesSeen = new HashSet<uint>();

                    for (int pos = end; pos != start;)
                    {
                        pos = (pos - 1) % Capacity;
                        if (pos < 0)
                            break;

                        bool newSignature = signaturesSeen.Add(this._entries[pos].Signature & kSignatureMask);
                        Verify(() => newSignature != ((this._entries[pos].Signature & kDuplicatedMask) != 0));
                        hashesSeen.Add(this._entries[pos].Hash);
                    }

                    foreach (var hash in hashesSeen)
                    {
                        bool added = overallHashes.Add(hash);
                        Verify(() => added);
                    }

                    start = end;
                }
                while (true);
            }


            private static class BlockCopyMemoryHelper
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void Memset(Entry* pointer, int entries, Entry value)
                {
                    int block = 64, index = 0;
                    int length = Math.Min(block, entries);

                    //Fill the initial array
                    while (index < length)
                        pointer[index++] = value;

                    length = entries;
                    while (index < length)
                    {
                        int bytesToCopy = Math.Min(block, (length - index)) * sizeof(Entry);

                        Memory.Copy((byte*)(pointer + index), (byte*)pointer, bytesToCopy);

                        index += block;
                        block *= 2;
                    }
                }
            }
        }

        private Entry* LoadEntries(PrefixTreeTablePageHeader* header)
        {
            throw new NotImplementedException();
        }

        private PrefixTreeTablePageHeader* Initialize(int newCapacity)
        {
            // Calculate the next power of 2.
            newCapacity = Bits.NextPowerOf2(newCapacity);

            PrefixTreeTablePageHeader* tableHeader = null;

            throw new NotImplementedException();

            tableHeader->Capacity = newCapacity;
            tableHeader->NumberOfUsed = 0;
            tableHeader->NumberOfDeleted = 0;
            tableHeader->Size = 0;
            tableHeader->NextGrowthThreshold = newCapacity * 4 / InternalTable.LoadFactor;

            return tableHeader;
        }

        internal byte* ReadDirect(long pointer)
        {
            throw new NotImplementedException();
        }
    }
}
