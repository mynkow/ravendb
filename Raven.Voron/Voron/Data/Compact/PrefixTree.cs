using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using static Voron.Data.Compact.PrefixTree;

namespace Voron.Data.Compact
{
    /// <summary>
    /// In-Memory Dynamic Z-Fast TRIE supporting predecessor, successor using low linear additional space. 
    /// "A dynamic z-fast trie is a compacted trie endowed with two additional pointers per internal node and with a 
    /// dictionary. [...] it can be though of as an indexing structure built on a set of binary strings S. 
    /// 
    /// As described in "Dynamic Z-Fast Tries" by Belazzougui, Boldi and Vigna in String Processing and Information
    /// Retrieval. Lecture notes on Computer Science. Volume 6393, 2010, pp 159-172 [1]
    /// </summary>
    public unsafe partial class PrefixTree
    {
        private readonly static ObjectPool<Stack<long>> nodesStackPool = new ObjectPool<Stack<long>>(() => new Stack<long>());

        private readonly LowLevelTransaction _tx;
        private readonly Tree _parent;
        private readonly InternalTable _table;
        private readonly PrefixTreeRootMutableState _state;

        public string Name { get; set; }

        public PrefixTree(LowLevelTransaction tx, Tree parent, PrefixTreeRootMutableState state, Slice treeName)
        {
            _tx = tx;
            _parent = parent;
            _state = state;
            _table = new InternalTable(this, _tx, _state);

            Name = treeName.ToString();
        }

        public static PrefixTree Create(LowLevelTransaction tx, Tree parent, Slice treeName)
        {
            var rootPage = tx.AllocatePage(1);

            var header = (PrefixTreeRootHeader*)parent.DirectAdd(treeName, sizeof(PrefixTreeRootHeader));            
            var state = new PrefixTreeRootMutableState(tx, header);

            state.RootPage = rootPage.PageNumber;            
            state.Head = Constants.InvalidNode;
            state.Tail = Constants.InvalidNode;
            state.Items = 0;

            var tablePage = InternalTable.Allocate(tx, state);
            state.Table = tablePage.PageNumber;

            return new PrefixTree(tx, parent, state, treeName);    
        }

        public static PrefixTree Open( LowLevelTransaction tx, Tree parent, Slice treeName)
        {
            var header = (PrefixTreeRootHeader*)parent.DirectRead(treeName);            
            var state = new PrefixTreeRootMutableState(tx, header);
            return new PrefixTree(tx, parent, state, treeName);            
        }

        public void Add(Slice key, Slice value, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(Slice key, byte[] value, ushort? version = null)
        {
            // We dont want this to show up in the stack, but it is a very convenient way when we are dealing with
            // managed memory. So we are aggresively inlining this one.
            fixed ( byte* ptr = value )
            {
                Add(key, ptr, value.Length, version);
            }            
        }

        public void Add(Slice key, byte* value, int length, ushort? version = null)
        {
            // We prepare the signature to compute incrementally. 
            BitVector searchKey = key.ToBitVector();

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Add(Binary: {1}, Key: {0})", key.ToString(), searchKey.ToBinaryString()));
#endif
            if (Count == 0)
            {
                // We set the root of the current key to the new leaf.
                // We add the leaf after the head.  

                throw new NotImplementedException();
            }

            var hashState = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // We look for the parent of the exit node for the key.

            // If the exit node is a leaf and the key is equal to the LCP 
            // Then we are done (we found the key already).

            // Is the exit node internal?
            // Compute the exit direction from the LCP.  
            // Is this cut point low or high?


            // Create a new internal node that will hold the new leaf.  
            // Link the internal and the leaf according to its exit direction.

            // Ensure that the right leaf has a 1 in position and the left one has a 0. (TRIE Property).

            // If the exit node is the root
            // Then update the root
            // Else update the parent exit node.

            // Update the jump table after the insertion.
            // Link the new leaf with it's predecessor and successor.

            throw new NotImplementedException();
        }

        public void Add<TValue> (Slice key, TValue value, ushort? version = null )
        { 
            /// For now output the data to a buffer then send the proper Add(key, byte*, length)
            throw new NotImplementedException();
        }

        public void Delete(Slice key, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public Slice Successor(Slice key)
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            throw new NotImplementedException();
        }

        public Slice SuccessorOrDefault(Slice key)
        {
            if (Count == 0)
                return Slice.Empty;

            throw new NotImplementedException();
        }

        public Slice Predecessor(Slice key)
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            throw new NotImplementedException();
        }

        public Slice PredecessorOrDefault(Slice key)
        {
            if (Count == 0)
                return Slice.Empty;

            throw new NotImplementedException();
        }


        public Slice FirstKey()
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            throw new NotImplementedException();
        }

        public Slice FirstKeyOrDefault()
        {
            if (Count == 0)
                return Slice.Empty;

            throw new NotImplementedException();
        }

        public Slice LastKey()
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            throw new NotImplementedException();
        }

        public Slice LastKeyOrDefault()
        {
            if (Count == 0)
                return Slice.Empty;

            throw new NotImplementedException();
        }

        public bool Contains(Slice key)
        {
            if (Count == 0)
                return false;

            throw new NotImplementedException();
        }

        public bool TryGet(Slice key, out byte* value, out int sizeOf)
        {
            if (Count == 0)
            {
                value = null;
                sizeOf = 0;
                return false;
            }
                

            throw new NotImplementedException();
        }

        public bool TryGet<Value>(Slice key, out Value value)
        {
            if (Count == 0)
            {
                value = default(Value);
                return false;
            }
                

            throw new NotImplementedException();
        }

        public long Count => _state.Items;
        internal Node* Root => this.ReadNodeByName(0);
        internal Node* Tail => this.ReadNodeByName(_state.Tail);
        internal Node* Head => this.ReadNodeByName(_state.Head);
        internal InternalTable NodesTable => this._table;

        internal Node* ReadNodeByName(long nodeName)
        {
            throw new NotImplementedException();
        }

        internal Slice ReadKey(long dataPtr)
        {
            throw new NotImplementedException();
        }

        internal int GetKeySize(long dataPtr)
        {
            throw new NotImplementedException();
        }
    }
}
