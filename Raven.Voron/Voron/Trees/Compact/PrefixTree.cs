using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.FileHeaders;

namespace Voron.Trees.Compact
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
        private readonly Transaction _tx;
        private readonly Slice _treeName;
        private readonly PrefixTreeMutableState _state;

        private static readonly StructureSchema<PrefixTreeDataFields> _dataSchema;

        static PrefixTree()
        {
           _dataSchema = new StructureSchema<PrefixTreeDataFields>()
                                    .Add<int>(PrefixTreeDataFields.BlockSize)
                                    .Add<byte[]>(PrefixTreeDataFields.Key)
                                    .Add<byte[]>(PrefixTreeDataFields.Data);
        }

        public PrefixTreeMutableState State
        {
            get { return _state; }
        }

        public long NumberOfEntries
        {
            get { return _state.LeafCount; }
        }

        private PrefixTree(Transaction tx, Slice treeName, PrefixTreeRootHeader* header, PrefixTreeFlags flags = PrefixTreeFlags.None)
        {
            _tx = tx;
            _state = new PrefixTreeMutableState(header);
            _treeName = treeName;
        }

        private PrefixTree(Transaction tx, Slice treeName, PrefixTreeMutableState state)
        {
            _tx = tx;
            _state = state;
            _treeName = treeName;
        }

        public static PrefixTree Open(Transaction tx, Slice treeName, PrefixTreeRootHeader* header)
        {
            return new PrefixTree(tx, treeName, header);
        }

        public static PrefixTree Create(Transaction tx, Slice treeName, PrefixTreeFlags flags = PrefixTreeFlags.None)
        {
            throw new NotImplementedException();

            //var newRootPage = tx.AllocatePage(1, PageFlags.Leaf);
            
            //var tree = new PrefixTree(tx, newRootPage.PageNumber);
            //tree.State.RecordNewPage(newRootPage, 1);

            //return tree;
        }

        public Slice Name
        {
            get { return _treeName; }
        }

        public void Add(Slice key, byte[] value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");

            _state.IsModified = true;

            throw new NotImplementedException();
        }

        public void Add(Slice key, Stream value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");
            if (value.Length > int.MaxValue)
                throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

            _state.IsModified = true;

            throw new NotImplementedException();
        }

        public void Add(Slice key, Slice value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");

            _state.IsModified = true;

            throw new NotImplementedException();
        }

        public void Delete(Slice key, ushort? version = null)
        {
            _state.IsModified = true;

            throw new NotImplementedException();
        }

        public PrefixTreeIterator Successor(Slice key)
        {
            var successor = SuccessorInternal(key);
            Debug.Assert(successor->Type == PrefixTreeNodeType.Leaf);

            var tail = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(tail->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, successor, tail);
        }

        public PrefixTreeIterator Predecessor(Slice key)
        {
            var predecessor = PredecessorInternal(key);
            Debug.Assert(predecessor->Type == PrefixTreeNodeType.Leaf);

            var tail = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(tail->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, predecessor, tail);
        }

        public PrefixTreeIterator First()
        {
            if (this.NumberOfEntries == 0)
                throw new KeyNotFoundException();

            var node = (PrefixTreeLeafNode*)AcquireNode(_state.Head);
            Debug.Assert(node->Type == PrefixTreeNodeType.Leaf);

            var nextNode = (PrefixTreeLeafNode*)AcquireNode(node->Next);
            Debug.Assert(nextNode->Type == PrefixTreeNodeType.Leaf);

            var tail = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(tail->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, nextNode, tail);
        }

        public PrefixTreeIterator Last()
        {
            if (this.NumberOfEntries == 0)
                throw new KeyNotFoundException();

            var tail = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(tail->Type == PrefixTreeNodeType.Leaf);

            var previousNode = (PrefixTreeLeafNode*)AcquireNode(tail->Previous);
            Debug.Assert(previousNode->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, previousNode, tail);
        }

        public Slice SuccessorKey(Slice key)
        {
            if (this.NumberOfEntries == 0)
                throw new KeyNotFoundException();

            var node = (PrefixTreeLeafNode*)SuccessorInternal(key);
            Debug.Assert(node->Type == PrefixTreeNodeType.Leaf);

            var reader = new StructureReader<PrefixTreeDataFields>(AcquireDataPtr(node->Value), _dataSchema);
            return new Slice(reader.ReadBytes(PrefixTreeDataFields.Key));
        }

        public Slice PredecessorKey(Slice key)
        {
            if (this.NumberOfEntries == 0)
                throw new KeyNotFoundException();

            var node = (PrefixTreeLeafNode*)PredecessorInternal(key);
            Debug.Assert(node->Type == PrefixTreeNodeType.Leaf);

            var reader = new StructureReader<PrefixTreeDataFields>(AcquireDataPtr(node->Value), _dataSchema);
            return new Slice(reader.ReadBytes(PrefixTreeDataFields.Key));
        }

        public Slice FirstKey()
        {
            if (this.NumberOfEntries == 0)
                throw new KeyNotFoundException();

            var node = (PrefixTreeLeafNode*)AcquireNode(_state.Head);
            Debug.Assert(node->Type == PrefixTreeNodeType.Leaf);

            var nextNode = (PrefixTreeLeafNode*)AcquireNode(node->Next);
            Debug.Assert(nextNode->Type == PrefixTreeNodeType.Leaf);

            var reader = new StructureReader<PrefixTreeDataFields>(AcquireDataPtr(nextNode->Value), _dataSchema);
            return new Slice(reader.ReadBytes(PrefixTreeDataFields.Key));
        }

        public Slice LastKey()
        {
            if (this.NumberOfEntries == 0)
                throw new KeyNotFoundException();

            var node = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(node->Type == PrefixTreeNodeType.Leaf);

            var previousNode = (PrefixTreeLeafNode*)AcquireNode(node->Previous);
            Debug.Assert(previousNode->Type == PrefixTreeNodeType.Leaf);

            var reader = new StructureReader<PrefixTreeDataFields>(AcquireDataPtr(previousNode->Value), _dataSchema);
            return new Slice(reader.ReadBytes(PrefixTreeDataFields.Key));
        }

        public PrefixTreeIterator Iterate()
        {
            var head = (PrefixTreeLeafNode*)AcquireNode(_state.Head);
            Debug.Assert(head->Type == PrefixTreeNodeType.Leaf);

            var tail = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(tail->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, head, tail);
        }

        public PrefixTreeIterator Iterate(Slice start)
        {
            var startNode = SuccessorInternal(start);
            Debug.Assert(startNode->Type == PrefixTreeNodeType.Leaf);

            var tail = (PrefixTreeLeafNode*)AcquireNode(_state.Tail);
            Debug.Assert(tail->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, startNode, tail);            
        }

        public PrefixTreeIterator Iterate(Slice start, Slice end)
        {
            var startNode = SuccessorInternal(start);
            var endNode = PredecessorInternal(end);

            Debug.Assert(startNode->Type == PrefixTreeNodeType.Leaf);
            Debug.Assert(endNode->Type == PrefixTreeNodeType.Leaf);

            return new PrefixTreeIterator(this, startNode, endNode);
        }



        private PrefixTreeLeafNode* SuccessorInternal(Slice key)
        {
            throw new NotImplementedException();
        }

        private PrefixTreeLeafNode* PredecessorInternal(Slice key)
        {
            throw new NotImplementedException();
        }

        private PrefixTreeNode* AcquireNode(PrefixTreeNodePtr prefixTreeNodePtr)
        {
            throw new NotImplementedException();
        }

        private byte* AcquireDataPtr(PrefixTreeDataPtr prefixTreeDataPtr)
        {
            throw new NotImplementedException();
        }
       
    }
}
