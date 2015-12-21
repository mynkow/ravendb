using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(Slice key, Slice value, ushort? version = null)
        {
            if (value.Array != null )
            {
                // We dont want this to show up in the stack, but it is a very convenient way when we are dealing with
                // managed memory. So we are aggresively inlining this one.
                fixed (byte* ptr = value.Array)
                {
                    Add(key, ptr, value.Array.Length, version);
                }
            }
            else
            {
                Add(key, value.Pointer, value.Size, version);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(Slice key, byte[] value, ushort? version = null)
        {
            // We dont want this to show up in the stack, but it is a very convenient way when we are dealing with
            // managed memory. So we are aggresively inlining this one.
            fixed (byte* ptr = value)
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

        /// <summary>
        /// Deletion mostly follow the insertion steps uneventfully. [...] To fix the jump pointers, we need to know
        /// the 2-fat ancestors of the parent of parex(x), not of parex(x). Page 171 of [1].
        /// </summary>
        public bool Delete(Slice key, ushort? version = null)
        {
            if (Count == 0)
            {
                return false;
            }                
            else if ( Count == 1)
            {
                // Is the root key (which has to be a Leaf) equal to the one we are looking for?
                throw new NotImplementedException();

                // We remove the root.    
                return true;
            }

            var searchKey = key.ToBitVector();
            var hashState = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // We look for the parent of the exit node for the key.

            // If the exit node is not a leaf or the key is not equal to the LCP             
            // Then we are done (The key does not exist).


            // If the parentExitNode is not null and not the root
            // Then we need to fix the grand parent child pointer.

            // If the parent node is the root, then the child becomes the root.

            // If the exit node (which should be a leaf) reference is not null 
            // Then fix the parent and grandparent references.      

            // Delete the leaf and fix it's predecessor and successor references.   

            // Is this cut point low or high? 

            // Update the jump table after the deletion.

            // If the cut point was low and the child is internal
            //   We remove the existing child node from the jump table
            //   We replace the parent exit node
            //   We update the jumps table for the child node.
            // Else
            //   We remove the parent node from the jump table.

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

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);

            // If the exit node is a leaf and the key is equal to the LCP 
            Debug.Assert(exitNode.Exit->IsLeaf);
            var exitNodeAsLeaf = (Leaf*)exitNode.Exit;
            if (exitNodeAsLeaf != null && GetKeySize(exitNodeAsLeaf->DataPtr) == exitNode.LongestPrefix)
            {
                value = ReadValue(exitNodeAsLeaf->DataPtr, out sizeOf);

                return true; // Then we are done (we found the key already).
            }

            value = null;
            sizeOf = 0;
            return false;
        }

        public bool TryGet<Value>(Slice key, out Value value)
        {
            if (Count == 0)
            {
                value = default(Value);
                return false;
            }

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);

            // If the exit node is a leaf and the key is equal to the LCP 
            Debug.Assert(exitNode.Exit->IsLeaf);
            var exitNodeAsLeaf = (Leaf*)exitNode.Exit;
            if (exitNodeAsLeaf != null && GetKeySize(exitNodeAsLeaf->DataPtr) == exitNode.LongestPrefix)
            {
                value = ReadValue<Value>(exitNodeAsLeaf->DataPtr);

                return true; // Then we are done (we found the key already).
            }

            value = default(Value);
            return false;
        }

        public bool Contains(Slice key)
        {
            if (Count == 0)
                return false;

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);

            // If the exit node is a leaf and the key is equal to the LCP 
            Debug.Assert(exitNode.Exit->IsLeaf);
            var exitNodeAsLeaf = (Leaf*)exitNode.Exit;
            if (exitNodeAsLeaf != null && GetKeySize(exitNodeAsLeaf->DataPtr) == exitNode.LongestPrefix)
                return true; // Then we are done (we found the key already).

            return false;
        }

        public Slice Successor(Slice key)
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            return this.ReadKey(SuccessorInternal(key)->DataPtr);
        }

        public Slice Predecessor(Slice key)
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            return this.ReadKey(PredecessorInternal(key)->DataPtr);
        }

        public Slice FirstKey()
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            Debug.Assert(_state.Head != Constants.InvalidNode);

            var headRef = this.Head;
            Debug.Assert(headRef->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(((Leaf*)headRef)->DataPtr);
        }

        public Slice FirstKeyOrDefault()
        {
            if (Count == 0)
                return Slice.BeforeAllKeys;

            Debug.Assert(_state.Head != Constants.InvalidNode);

            var headRef = this.Head;
            Debug.Assert(headRef->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(((Leaf*)headRef)->DataPtr);
        }

        public Slice LastKey()
        {
            if (Count == 0)
                throw new KeyNotFoundException();

            Debug.Assert(_state.Tail != Constants.InvalidNode);

            var tailRef = this.Tail;
            Debug.Assert(tailRef->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(((Leaf*)tailRef)->DataPtr);
        }

        public Slice LastKeyOrDefault()
        {
            if (Count == 0)
                return Slice.AfterAllKeys;

            Debug.Assert(_state.Tail != Constants.InvalidNode);

            var tailRef = this.Tail;
            Debug.Assert(tailRef->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(((Leaf*)tailRef)->DataPtr);
        }

        public long Count => _state.Items;

        internal Node* Root => this.ReadNodeByName(0);
        internal Node* Tail => this.ReadNodeByName(_state.Tail);
        internal Node* Head => this.ReadNodeByName(_state.Head);
        internal InternalTable NodesTable => this._table;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Leaf* SuccessorInternal(Slice key)
        {
            // x+ = min{y ? S | y = x} (the successor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);
            var exitNode = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (exitFound.SearchKey.CompareToInline(this.Extent(exitNode), out dummy) <= 0)
            {
                // If the key is smaller than the extent, we exit to the left leaf.
                return this.GetLeftLeaf(exitNode);
            }
            else
            {
                // If the key is greater than the extent, we exit to the right leaf and get the next.
                var nodeRef = this.GetRightLeaf(exitNode);
                var leafRef = this.ReadNodeByName(nodeRef->NextPtr);
                Debug.Assert(leafRef->IsLeaf);

                return (Leaf*)leafRef;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Leaf* PredecessorInternal(Slice key)
        {
            // x- = max{y ? S | y < x} (the predecessor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);
            var exitNode = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (this.Extent(exitNode).CompareToInline(exitFound.SearchKey, out dummy) < 0)
            {
                // If the key is greater than the extent, we exit to the right leaf.
                return this.GetRightLeaf(exitNode);
            }
            else
            {
                // If the key is smaller than the extent, we exit to the left leaf and get the previous leaf.
                var nodeRef = this.GetLeftLeaf(exitNode);
                var leafRef = this.ReadNodeByName(nodeRef->PreviousPtr);
                Debug.Assert(leafRef->IsLeaf);

                return (Leaf*)leafRef;
            }
        }

        private ExitNode FindExitNode(Slice key)
        {
            Debug.Assert(Count != 0);

            // We prepare the signature to compute incrementally. 
            BitVector searchKey = key.ToBitVector();

            if (Count == 1)
                return new ExitNode(searchKey.LongestCommonPrefixLength(this.Extent(this.Root)), this.Root, searchKey);

            // We look for the parent of the exit node for the key.
            var state = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // Find parex(key), exit(key) or fail spectacularly (with very low probability). 
            Internal* parexOrExitNode = FatBinarySearch(searchKey, state, -1, searchKey.Count, isExact: false);

            // Check if the node is either the parex(key) and/or exit(key). 
            Node* candidateNode;
            if (parexOrExitNode->ExtentLength < searchKey.Count && searchKey[parexOrExitNode->ExtentLength])
                candidateNode = ReadNodeByName(parexOrExitNode->RightPtr);
            else
                candidateNode = ReadNodeByName(parexOrExitNode->LeftPtr);

            int lcpLength = searchKey.LongestCommonPrefixLength(this.Extent(candidateNode));

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (this.IsExitNodeOf(candidateNode, searchKey.Count, lcpLength))
                return new ExitNode(lcpLength, candidateNode, searchKey);

            lcpLength = Math.Min(parexOrExitNode->ExtentLength, lcpLength);
            if (this.IsExitNodeOf(parexOrExitNode, searchKey.Count, lcpLength))
                return new ExitNode(lcpLength, (Node*)parexOrExitNode, searchKey);

            // With very low priority we screw up and therefore we start again but without skipping anything. 
            parexOrExitNode = FatBinarySearch(searchKey, state, -1, searchKey.Count, isExact: true);
            if (this.Extent((Node*)parexOrExitNode).IsProperPrefix(searchKey))
            {
                if (parexOrExitNode->ExtentLength < searchKey.Count && searchKey[parexOrExitNode->ExtentLength])
                    candidateNode = ReadNodeByName(parexOrExitNode->RightPtr);
                else
                    candidateNode = ReadNodeByName(parexOrExitNode->LeftPtr);
            }
            else
            {
                candidateNode = (Node*)parexOrExitNode;
            }

            return new ExitNode(searchKey.LongestCommonPrefixLength(this.Extent(candidateNode)), candidateNode, searchKey);
        }

        private unsafe Internal* FatBinarySearch(BitVector searchKey, Hashing.Iterative.XXHash32Block state, int startBit, int endBit, bool isExact)
        {
            Debug.Assert(searchKey != null);
            Debug.Assert(state != null);
            Debug.Assert(startBit < endBit - 1);

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FatBinarySearch({0},({1}..{2})", searchKey.ToDebugString(), startBit, endBit));
#endif
            endBit--;

            Internal* top = null;

            if (startBit == -1)
            {
                Debug.Assert(this.Root->IsInternal);

                top = (Internal*)this.Root;
                startBit = top->ExtentLength;
            }

            var nodesTable = this.NodesTable;

            uint checkMask = (uint)(-1 << Bits.CeilLog2(endBit - startBit));
            while (endBit - startBit > 0)
            {
                Debug.Assert(checkMask != 0);

#if DETAILED_DEBUG
                Console.WriteLine(string.Format("({0}..{1})", startBit, endBit + 1));
#endif
                int current = endBit & (int)checkMask;
                if ((startBit & checkMask) != current)
                {
#if DETAILED_DEBUG
                    Console.WriteLine(string.Format("Inquiring with key {0} ({1})", searchKey.SubVector(0, current).ToBinaryString(), current));
#endif
                    // We calculate the hash up to the word it makes sense. 
                    uint hash = InternalTable.CalculateHashForBits(searchKey, state, current);

                    int position = isExact ? nodesTable.GetExactPosition(searchKey, current, hash)
                                           : nodesTable.GetPosition(searchKey, current, hash);
                    

                    long itemPtr = position != -1 ? nodesTable[position] : Constants.InvalidNode;
                    if (itemPtr == Constants.InvalidNode)
                    {
#if DETAILED_DEBUG
                        Console.WriteLine("Missing " + ((isExact) ? "exact" : "non exact"));
#endif
                        endBit = current - 1;
                    }
                    else 
                    {
                        Internal* item = (Internal*)ReadNodeByName(itemPtr);
                        Debug.Assert(item->IsInternal); // Make sure there are only internal nodes there. 

                        if (item->ExtentLength < current)
                        {
#if DETAILED_DEBUG
                            Console.WriteLine("Missing " + ((isExact) ? "exact" : "non exact"));
#endif
                            endBit = current - 1;
                        }
                        else
                        {
#if DETAILED_DEBUG
                            Console.WriteLine("Found " + ((isExact) ? "exact" : "non exact") + " extent of length " + item->ExtentLength + " with GetExtentLength of " + this.GetExtentLength(item));
#endif
                            // Add it to the stack, update search and continue
                            top = item;

                            startBit = item->ExtentLength;
                        }
                    }
                }

                checkMask >>= 1;
            }

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Final interval: ({0}..{1}); Top: {2}", startBit, endBit + 1, this.ToDebugString((Node*)top)));
#endif
            return top;
        }

        internal Node* ReadNodeByName(long nodeName)
        {
            throw new NotImplementedException();
        }

        internal Slice ReadKey(long dataPtr)
        {
            throw new NotImplementedException();
        }

        internal Value ReadValue<Value>(long dataPtr)
        {
            throw new NotImplementedException();
        }

        internal byte* ReadValue(long dataPtr, out int sizeOf)
        {
            throw new NotImplementedException();
        }

        internal int GetKeySize(long dataPtr)
        {
            throw new NotImplementedException();
        }
    }
}
