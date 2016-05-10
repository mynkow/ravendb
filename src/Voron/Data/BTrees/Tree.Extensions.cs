using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    partial class Tree
    {
        public long Increment(string key, long delta)
        {
            return Increment<SliceArray>(key, delta);
        }

        public long Increment( string key, long delta, ushort version)
        {
            return Increment<SliceArray>(key, delta, version);
        }

        public void Add(string key, Stream value, ushort version)
        {
            Add<SliceArray>(key, value, version);
        }

        public void Add(string key, Stream value)
        {
            Add<SliceArray>(key, value);
        }

        public void Add(string key, MemoryStream value, ushort? version = null)
        {
            Add<SliceArray>(key, value, version);
        }

        public void Add( string key, byte[] value, ushort version)
        {
            Add<SliceArray>(key, value, version);
        }

        public void Add(string key, byte[] value)
        {
            Add<SliceArray>(key, value);
        }

        public void Add(string key, string value, ushort? version = null)
        {
            Add<SliceArray, SliceArray>(key, value, version);
        }

        public unsafe byte* DirectAdd(string key, int len, TreeNodeFlags nodeType = TreeNodeFlags.Data, ushort? version = null)
        {
            return DirectAdd<SliceArray>(key, len, nodeType, version);
        }

        public void Delete(string key)
        {
            Delete<SliceArray>(key);
        }

        public void Delete(string key, ushort version)
        {
            Delete<SliceArray>(key, version);
        }

        public ReadResult Read(string key)
        {
            return Read<SliceArray>(key);
        }

        public ushort ReadVersion(string key)
        {
            return ReadVersion<SliceArray>(key);
        }


        public void MultiAdd(string key, string value, ushort? version = null)
        {
            MultiAdd(new SliceArray(key), new SliceArray(value), version);
        }

        public void MultiAdd<T>(T key, string value, ushort? version = null)
            where T : class, ISlice
        {
            MultiAdd(key, new SliceArray(value), version);
        }

        public void MultiDelete(string key, string value, ushort? version = null)
        {
            MultiDelete<SliceArray, SliceArray>(key, value, version);
        }

        public IIterator MultiRead(string key)
        {
            return MultiRead<SliceArray>(key);
        }
    }
}
