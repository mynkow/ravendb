using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Xunit;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;

namespace FastTests.Voron.Cedar
{
    public unsafe class Basic : StorageTest
    {

        [Fact]
        public void Construction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.False(root.IsBranch); // It is just created, so it is a leaf node.   
                Assert.Equal(256, root.Header.Ptr->Size);

                Assert.True(root.Header.Ptr->Capacity > root.Header.Ptr->Size * 2);
                Assert.Equal(0,  root.Header.Ptr->Capacity % 256);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(0, root.NumberOfKeys);
                Assert.Equal(0, root.NonZeroSize);
                Assert.Equal(0, root.NonZeroLength);

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);

                CedarDataNode* ptr;
                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(ptr == null);
            }
        }

        [Fact]
        public void SingleInsertAndQuery()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("test", 1);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(256, root.Header.Ptr->Size);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(1, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(256, root.Header.Ptr->Size);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(1, root.NonZeroSize);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tes", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "test1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "a", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));

                Slice testSlice;
                Slice.From(tx.Allocator, "test", out testSlice);

                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));

                Slice outputKey;
                Slice.Create(tx.Allocator, 1024, out outputKey);

                long from = 0;
                long len = 0;
                var iterator = root.End(outputKey, ref from, ref len);
                Assert.Equal(CedarResultCode.Success, iterator.Error);
                outputKey.SetSize((int)len);
                Assert.True(SliceComparer.Equals(testSlice, outputKey));
            }
        }


        [Fact]
        public void InsertSplitAtTheBeginningAndQuery()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("test", 1);
                tree.Add("aest", 2);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(256, root.Header.Ptr->Size);

                Assert.Equal(2, root.NumberOfKeys);
                Assert.Equal(2, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(256, root.Header.Ptr->Size);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(2, root.NumberOfKeys);
                Assert.Equal(2, root.NonZeroSize);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "aest", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tes", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "test1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "aest1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));

                CedarKeyPair resultKey;
                Slice testSlice;

                Slice.From(tx.Allocator, "aest", out testSlice);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));

                Slice.From(tx.Allocator, "test", out testSlice);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));

                Slice outputKey;
                Slice.Create(tx.Allocator, 1024, out outputKey);

                long from = 0;
                long len = 0;
                var iterator = root.End(outputKey, ref from, ref len);
                Assert.Equal(CedarResultCode.Success, iterator.Error);
                outputKey.SetSize((int)len);
                Assert.True(SliceComparer.Equals(testSlice, outputKey));
            }
        }

        [Fact]
        public void InsertSplitAtTheEnd()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("test", 1);
                tree.Add("tesa", 1);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(2, root.NumberOfKeys);
                Assert.Equal(5, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(512, root.Header.Ptr->Size);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(2, root.NumberOfKeys);
                Assert.Equal(5, root.NonZeroSize);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tesa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tes", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "test1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tesa1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
            }
        }

        [Fact]
        public void InsertSplitAtTheEndAndMiddle()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("test", 1);
                tree.Add("tesa", 2);
                tree.Add("tasa", 3);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(3, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(6, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(3, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(6, root.NonZeroSize);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Slice.From(tx.Allocator, "tesa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Slice.From(tx.Allocator, "tasa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, ptr->Data);

                Slice.From(tx.Allocator, "tes", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tas1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tesa1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
            }
        }


        [Fact]
        public void InsertSelfContainedAndQuery()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("test", 1);
                tree.Add("test1234", 2);
                tree.Add("test12", 3);
                tree.Add("test123456", 4);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(4, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(12, root.NonZeroSize);
                Assert.Equal(4, root.Header.Ptr->NumberOfEntries);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(4, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(12, root.NonZeroSize);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Slice.From(tx.Allocator, "test1234", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Slice.From(tx.Allocator, "test12", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, ptr->Data);
                Slice.From(tx.Allocator, "test123456", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(4, ptr->Data);

                Slice.From(tx.Allocator, "tes", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tas1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));
                Slice.From(tx.Allocator, "tesa1", out value);
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(value, out result, out ptr));

                CedarKeyPair resultKey;
                Slice testSlice;
                Slice.From(tx.Allocator, "test", out testSlice);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));

                Slice.From(tx.Allocator, "test123456", out testSlice);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));


                Slice outputKey;
                Slice.Create(tx.Allocator, 1024, out outputKey);

                long from = 0;
                long len = 0;
                var iterator = root.End(outputKey, ref from, ref len);
                Assert.Equal(CedarResultCode.Success, iterator.Error);
                outputKey.SetSize((int)len);
                Assert.True(SliceComparer.Equals(testSlice, outputKey));
            }
        }

        [Fact]
        public void InsertSameMultipleTimes()
        {            
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(0, root.NumberOfKeys);

                CedarKeyPair resultKey;
                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                tree.Add("test", 1);
                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Assert.Equal(1, root.Header.Ptr->NumberOfEntries);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("test", 2);
                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Assert.Equal(1, root.Header.Ptr->NumberOfEntries);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("test", 3);
                Slice.From(tx.Allocator, "test", out value);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, ptr->Data);
                Assert.Equal(1, root.Header.Ptr->NumberOfEntries);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(1, root.NumberOfKeys);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;
                Slice.From(tx.Allocator, "test", out value);

                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, ptr->Data);

                CedarKeyPair resultKey;

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));
            }
        }


        [Fact]
        public void InsertSameMultipleTimesX10000()
        {
            List<Slice> items = new List<Slice>();
            for (int i = 0; i < 100; i++)
            {
                Slice value;
                Slice.From(Allocator, GenerateRandomString(5, 2), out value);

                items.Add(value);
            }                

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");                

                foreach (Slice t in items)
                    tree.Add(t, -1);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Random r = new Random(1);
                for (int i = 0; i < 10000; i++)
                {
                    Slice item = items[r.Next(items.Count)];
                    tree.Add(item, i);

                    Assert.Equal(100, root.NumberOfKeys);

                    CedarRef result;
                    CedarDataNode* ptr;
                    Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(item, out result, out ptr));
                    Assert.Equal(i, ptr->Data);

                    Assert.Equal(100, root.Header.Ptr->NumberOfEntries);
                    Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                }

                tx.Commit();
            }
        }

        [Fact]
        public void InsertBasic()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("users/1", 1);
                tree.Add("users/2", 2);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(2, root.NumberOfKeys);
                Assert.Equal(2, root.Header.Ptr->NumberOfEntries);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(2, root.NumberOfKeys);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "users/1", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Slice.From(tx.Allocator, "users/2", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, ptr->Data);
            }
        }

        [Fact]
        public void InsertBasic2()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("collections/1", 1);
                tree.Add("collections/2", 2);
                tree.Add("collections/3", 3);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(3, root.NumberOfKeys);
                Assert.Equal(3, root.Header.Ptr->NumberOfEntries);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(3, root.NumberOfKeys);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                Slice.From(tx.Allocator, "collections/1", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Slice.From(tx.Allocator, "collections/2", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Slice.From(tx.Allocator, "collections/3", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, ptr->Data);

                CedarKeyPair resultKey;
                Slice.From(tx.Allocator, "collections/1", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "collections/3", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));
            }
        }


        [Fact]
        public void CanAddAndRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("b", 2);

                CedarDataNode* ptr;
                CedarKeyPair resultKey;
                Slice value;

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);

                Slice.From(tx.Allocator, "b", out value);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("c", 3);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "c", out value);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("a", 1);

                Slice.From(tx.Allocator, "a", out value);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "c", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));


                var actual = tree.Read("a");

                Slice.From(tx.Allocator, "a", out value);

                using (var it = tree.Iterate(false))
                {
                    Assert.True(it.Seek(value));
                    Assert.Equal("a", it.CurrentKey.ToString());
                }

                Assert.Equal(1, actual);                
            }
        }


        [Fact]
        public void Example1()
        {
            // This example is generated to look like the following graph: https://linux.thai.net/~thep/datrie/trie2.gif

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("pool", 1);
                tree.Add("prize", 2);
                tree.Add("preview", 3);
                tree.Add("prepare", 4);
                tree.Add("produce", 5);
                tree.Add("progress", 6);

                CedarDataNode* ptr;
                CedarKeyPair resultKey;
                Slice value;

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(6, root.Header.Ptr->NumberOfEntries);

                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "progress", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                long from = 0;
                long len = 0;
                Slice outputKey;
                Slice.Create(tx.Allocator, 4096, out outputKey);

                Slice.From(tx.Allocator, "pqql", out value);
                var iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prfze", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prize", out value);
                iterator = root.PredecessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pria", out value);
                iterator = root.PredecessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "preview", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prfze", out value);
                iterator = root.PredecessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "preview", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prjze", out value);
                iterator = root.PredecessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
            }
        }


        [Fact]
        public void Predecessor()
        {
            // This example is generated to look like the following graph: https://linux.thai.net/~thep/datrie/trie2.gif

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("pool", 1);
                tree.Add("prize", 2);
                tree.Add("preview", 3);
                tree.Add("prepare", 4);
                tree.Add("produce", 5);
                tree.Add("producer", 7);
                tree.Add("progress", 6);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(7, root.Header.Ptr->NumberOfEntries);

                Slice outputKey;
                Slice.Create(tx.Allocator, 4096, out outputKey);

                Slice value;

                long from = 0;
                long len = 0;
                Slice.From(tx.Allocator, "poop", out value);
                var iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "priz", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "preview", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pra", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pr", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pre", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prep", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prepare", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "producer", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "produce", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));
                outputKey.Reset();

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pooa", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);
            }
        }

        [Fact]
        public void PredecessorSharedPrefix()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("ace", 1);
                tree.Add("acer", 2);
                tree.Add("ac", 1);
                tree.Add("a", 1);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Slice value;
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);

                Slice outputKey;
                Slice.Create(tx.Allocator, 4096, out outputKey);


                long from = 0;
                long len = 0;
                Slice.From(tx.Allocator, "acer", out value);
                var iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "ace", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "ace", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "ac", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "ac", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "a", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));


                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "a", out value);
                iterator = root.Predecessor(value, ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);
            }
        }

        [Fact]
        public void Successor()
        {
            // This example is generated to look like the following graph: https://linux.thai.net/~thep/datrie/trie2.gif

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("pool", 1);
                tree.Add("prize", 2);
                tree.Add("preview", 3);
                tree.Add("prepare", 4);
                tree.Add("produce", 5);
                tree.Add("producer", 7);
                tree.Add("progress", 6);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(7, root.Header.Ptr->NumberOfEntries);

                Slice value;
                Slice outputKey;
                Slice.Create(tx.Allocator, 4096, out outputKey);


                long from = 0;
                long len = 0;
                Slice.From(tx.Allocator, "pooa", out value);
                var iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "poola", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));


                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "priz", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pop", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "po", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pri", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));


                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pre", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prev", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "preview", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pool", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "produce", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "producer", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "progs", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "progressive", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);
            }
        }

        [Fact]
        public void SuccessorOrEquals()
        {
            // This example is generated to look like the following graph: https://linux.thai.net/~thep/datrie/trie2.gif

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("pool", 1);
                tree.Add("prize", 2);
                tree.Add("preview", 3);
                tree.Add("prepare", 4);
                tree.Add("produce", 5);
                tree.Add("producer", 7);
                tree.Add("progress", 6);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(7, root.Header.Ptr->NumberOfEntries);

                Slice value;
                Slice outputKey;
                Slice.Create(tx.Allocator, 4096, out outputKey);

                long from = 0;
                long len = 0;
                Slice.From(tx.Allocator, "pooa", out value);
                var iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pool", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));


                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "priz", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pop", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "po", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "pool", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pri", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prize", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));


                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "pre", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "prepare", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "prev", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "preview", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "produce", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "produce", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "producer", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Slice.From(tx.Allocator, "producer", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, value));

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "progs", out value);
                iterator = root.SuccessorOrEqual(value, ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);

                from = 0;
                len = 0;
                Slice.From(tx.Allocator, "progressive", out value);
                iterator = root.Successor(value, ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);
            }
        }

        [Fact]
        public void FindBounds()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("aa", 2);
                tree.Add("ab", 3);

                CedarDataNode* ptr;
                CedarKeyPair resultKey;
                Slice value;

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Slice.From(tx.Allocator, "aa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "ab", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("ac", 1);

                Slice.From(tx.Allocator, "aa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "ac", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("aca", 3);

                Slice.From(tx.Allocator, "aa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "aca", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out resultKey, out ptr));

                Slice.From(tx.Allocator, "aca", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                tree.Add("b", 3);

                Slice.From(tx.Allocator, "aa", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));

                Slice.From(tx.Allocator, "b", out value);
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, value));
            }
        }


        [Fact]
        public void SingleNodeRemove()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice poolSlice;
                Slice.From(tx.Allocator, "pool", out poolSlice);

                var tree = tx.CreateTrie("foo");

                tree.Add(poolSlice, 1);

                CedarDataNode* ptr;
                CedarKeyPair resultKey;
                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, poolSlice));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, poolSlice));

                tree.Delete(poolSlice);

                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(poolSlice, out resultKey, out ptr));
            }
        }

        [Fact]
        public void DualNodeRemove()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice firstSlice;
                Slice.From(tx.Allocator, "test", out firstSlice);

                Slice secondSlice;
                Slice.From(tx.Allocator, "test1234", out secondSlice);


                var tree = tx.CreateTrie("foo");

                tree.Add(firstSlice, 1);
                tree.Add(secondSlice, 2);

                CedarDataNode* ptr;
                CedarKeyPair resultKey;
                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, firstSlice));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, secondSlice));

                tree.Delete(firstSlice);

                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(firstSlice, out resultKey, out ptr));


                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, secondSlice));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, secondSlice));
            }
        }

        [Fact]
        public void RemoveLowerToHigher()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                tree.Add("test", 1);
                tree.Add("test1234", 2);
                tree.Add("test12", 3);
                tree.Add("test123456", 4);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(4, root.NumberOfKeys);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                tree.Delete("test");
                Slice.From(tx.Allocator, "test", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, root.NumberOfKeys);

                tree.Delete("test1234");
                Slice.From(tx.Allocator, "test1234", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, root.NumberOfKeys);

                tree.Delete("test12");
                Slice.From(tx.Allocator, "test12", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, root.NumberOfKeys);

                tree.Delete("test123456");
                Slice.From(tx.Allocator, "test123456", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(0, root.NumberOfKeys);
            }
        }

        [Fact]
        public void RemoveHigherToLower()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");                

                tree.Add("test", 1);
                tree.Add("test1234", 2);
                tree.Add("test12", 3);
                tree.Add("test123456", 4);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(4, root.NumberOfKeys);

                CedarDataNode* ptr;
                CedarRef result;
                Slice value;

                int tailLength = root.Tail.Length;

                tree.Delete("test123456");
                Slice.From(tx.Allocator, "test123456", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(3, root.NumberOfKeys);
                
                root.ShrinkTail();                
                Assert.True(root.Tail.Length < tailLength);

                tree.Delete("test1234");
                Slice.From(tx.Allocator, "test1234", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(2, root.NumberOfKeys);

                tree.Delete("test12");
                Slice.From(tx.Allocator, "test12", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(1, root.NumberOfKeys);

                tree.Delete("test");
                Slice.From(tx.Allocator, "test", out value);
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(value, out result, out ptr));
                Assert.Equal(0, root.NumberOfKeys);
            }
        }

        [Fact]
        public void CanAddAndReadStats()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice key;
                Slice.From(tx.Allocator, "test", out key);

                var tree = tx.CreateTrie("foo");
                tree.Add(key, 1);

                tx.Commit();

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);
            }
        }

        [Fact]
        public void AfterSequentialPageSplitAllDataIsValid()
        {
            int count = 0;
            CedarPage root;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                do
                {
                    Slice value;
                    using (Slice.From(tx.Allocator, "test-" + count.ToString("0000"), ByteStringType.Immutable, out value))
                    {
                        tree.Add(value, count);
                    }

                    count++;
                }
                while (tree.State.PageCount == 1);

                Assert.Equal(count, tree.State.NumberOfEntries);
                Assert.Equal(3, tree.State.PageCount);
                Assert.Equal(2, tree.State.LeafPages);
                Assert.Equal(1, tree.State.BranchPages);
                Assert.Equal(2, tree.State.Depth);
                Assert.True(tree.State.IsModified);

                root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.NotEqual(CedarPageHeader.InvalidImplicitKey, root.Header.Ptr->ImplicitBeforeAllKeys);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                Assert.Equal(count, tree.State.NumberOfEntries);
                Assert.Equal(3, tree.State.PageCount);
                Assert.Equal(2, tree.State.LeafPages);
                Assert.Equal(1, tree.State.BranchPages);
                Assert.Equal(2, tree.State.Depth);
                Assert.False(tree.State.IsModified);

                using (var it = tree.Iterate(false))
                {
                    for (int i = 0; i < count; i++)
                    {
                        Slice value;
                        Slice.From(tx.Allocator, "test-" + i.ToString("0000"), out value);
                        Assert.True(it.Seek(value));
                        Assert.Equal("test-" + i.ToString("0000"), it.CurrentKey.ToString());
                    }
                }
            }
        }

        [Fact]
        public void AfterRandomPageSplitAllDataIsValid()
        {
            Random r;

            int count = 0;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                r = new Random(123);

                do
                {
                    int value = r.Next() % 100000;

                    tree.Add("test-" + value.ToString("0000000"), value);

                    count++;
                }
                while (tree.State.PageCount == 1);

                Assert.Equal(3, tree.State.PageCount);
                Assert.Equal(2, tree.State.LeafPages);
                Assert.Equal(1, tree.State.BranchPages);
                Assert.Equal(2, tree.State.Depth);
                Assert.True(tree.State.IsModified);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                Assert.Equal(3, tree.State.PageCount);
                Assert.Equal(2, tree.State.LeafPages);
                Assert.Equal(1, tree.State.BranchPages);
                Assert.Equal(2, tree.State.Depth);
                Assert.False(tree.State.IsModified);

                r = new Random(123);

                using (var it = tree.Iterate(false))
                {
                    it.Seek(Slices.BeforeAllKeys);

                    string lastKey = string.Empty;
                    while (it.MoveNext())
                    {
                        string currentKey = it.CurrentKey.ToString();
                        Assert.True(StringComparer.OrdinalIgnoreCase.Compare(lastKey, currentKey) < 0);

                        lastKey = currentKey;
                    }
                }
            }
        }


        [Fact]
        public void CursorMoveNext()
        {
            var values = new HashSet<int>();
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                Random r = new Random(123);

                for (int i = 0; i < 150; i++)
                {
                    int value = r.Next() % 100000;

                    tree.Add("test-" + value.ToString("0000000"), value);

                    values.Add(value);
                }

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.Depth);
                Assert.True(tree.State.IsModified);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.Depth);
                Assert.False(tree.State.IsModified);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                var cursor = new CedarCursor(tx.LowLevelTransaction, tree, root);
                cursor.Seek(Slices.BeforeAllKeys);
                Assert.True(cursor.Key.Same(Slices.BeforeAllKeys));

                int count = 0;

                string lastKey = string.Empty;
                while (cursor.MoveNext())
                {
                    string currentKey = cursor.Key.ToString();
                    Assert.True(StringComparer.OrdinalIgnoreCase.Compare(lastKey, currentKey) < 0);

                    long value = *(long*) cursor.Value;
                    Assert.True(values.Contains((int)value));

                    lastKey = currentKey;

                    count++;
                }

                Assert.True(cursor.Key.Same(Slices.AfterAllKeys));
                Assert.Equal(values.Count, count);
            }
        }

        [Fact (Skip="Not implemented yet.")]
        public void CursorMovePrev()
        {
            var values = new HashSet<int>();
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                Random r = new Random(123);

                for (int i = 0; i < 150; i++)
                {
                    int value = r.Next() % 100000;

                    tree.Add("test-" + value.ToString("0000000"), value);

                    values.Add(value);
                }

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.Depth);
                Assert.True(tree.State.IsModified);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.Depth);
                Assert.False(tree.State.IsModified);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                var cursor = new CedarCursor(tx.LowLevelTransaction, tree, root);
                cursor.Seek(Slices.AfterAllKeys);
                Assert.True(cursor.Key.Same(Slices.AfterAllKeys));

                int count = 0;

                string lastKey = string.Empty;
                while (cursor.MovePrev())
                {
                    string currentKey = cursor.Key.ToString();
                    Assert.True(StringComparer.OrdinalIgnoreCase.Compare(lastKey, currentKey) > 0);

                    long value = *(long*)cursor.Value;
                    Assert.True(values.Contains((int)value));

                    lastKey = currentKey;

                    count++;
                }

                Assert.True(cursor.Key.Same(Slices.BeforeAllKeys));
                Assert.Equal(values.Count, count);
            }
        }




        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        private static string GenerateRandomString(Random generator, int size, int clusteredSize)
        {
            var stringChars = new char[size];

            var clusterStart = generator.Next(10);
            for (int i = 0; i < clusteredSize; i++)
                stringChars[i] = chars[(clusterStart + i) % chars.Length];

            for (int i = clusteredSize; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];

            return new String(stringChars);
        }

        private static uint _seed = 1;

        private static string GenerateRandomString(int size, int clusteredSize)
        {
            var stringChars = new char[size];

            _seed = (_seed * 23 + 7);
            var clusterStart = _seed % 10;
            for (int i = 0; i < clusteredSize; i++)
                stringChars[i] = chars[(int)((clusterStart + i) % chars.Length)];

            for (int i = clusteredSize; i < stringChars.Length; i++)
            {
                _seed = (_seed * 23 + 7);
                stringChars[i] = chars[(int)(_seed % chars.Length)];
            }

            return new String(stringChars);
        }
    }
}
