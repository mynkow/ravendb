using Sparrow;
using System;
using System.Collections.Generic;
using System.IO;
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
        
        private PrefixTreeRootMutableState _state;

        public string Name { get; set; }

        internal Node* Root
        {
            get { throw new NotImplementedException(); }
        }

        internal InternalTable NodesTable
        {
            get { throw new NotImplementedException(); }
        }

        public long Count
        {
            get { throw new NotImplementedException(); }
        }


        public PrefixTree(LowLevelTransaction tx, Tree parent, Slice treeName)
        {
            _tx = tx;
            _parent = parent;
            Name = treeName.ToString();

            var header = (PrefixTreeRootHeader*)parent.DirectRead(treeName);
            _state = new PrefixTreeRootMutableState(tx, header);
        }

        public static PrefixTree Create(LowLevelTransaction tx)
        {
            throw new NotImplementedException();
        }

        public static PrefixTree Open( LowLevelTransaction tx, PrefixTreeRootHeader* header )
        {
            throw new NotImplementedException();
        }


        public void Add(Slice key, Stream value, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public void Add(Slice key, Slice value, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public void Add(Slice key, byte[] value, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public void Add<TValue> (Slice key, TValue value, ushort? version = null )
        {
            throw new NotImplementedException();
        }


        public void Delete(Slice key, ushort? version = null)
        {
            throw new NotImplementedException();
        }

        public Slice Successor( Slice key )
        {
            throw new NotImplementedException();
        }

        public Slice Predecessor( Slice key )
        {
            throw new NotImplementedException();
        }

        public Slice LastKeyOrDefault()
        {
            throw new NotImplementedException();
        }

        public Slice FirstKeyOrDefault()
        {
            throw new NotImplementedException();
        }
    }
}
