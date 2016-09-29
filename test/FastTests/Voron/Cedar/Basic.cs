using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;

namespace FastTests.Voron.Cedar
{
    public unsafe class Basic : StorageTest
    {

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;
            base.Configure(options);
        }

        [Fact]
        public void Construction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.False(root.IsBranch); // It is just created, so it is a leaf node.   
                Assert.Equal(256, root.Header.Ptr->Size);
                Assert.Equal(root.Header.Ptr->BlocksPerPage - (root.Header.Ptr->BlocksPerPage % 256), root.Header.Ptr->Capacity);
                Assert.Equal(0,  root.Header.Ptr->Capacity % 256);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(0, root.NumberOfKeys);
                Assert.Equal(0, root.NonZeroSize);
                Assert.Equal(0, root.NonZeroLength);

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);

                CedarDataPtr* ptr;
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tes"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test1"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "a"), out result, out ptr));

                var testSlice = Slice.From(tx.Allocator, "test");

                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, testSlice));
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "aest"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tes"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test1"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "aest1"), out result, out ptr));

                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "aest")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "test")));
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tesa"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tes"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test1"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tesa1"), out result, out ptr));
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tesa"), out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tasa"), out result, out ptr));
                Assert.Equal(3, ptr->Data);

                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tes"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tas1"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tesa1"), out result, out ptr));
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test1234"), out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test12"), out result, out ptr));
                Assert.Equal(3, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test123456"), out result, out ptr));
                Assert.Equal(4, ptr->Data);

                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tes"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tas1"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "tesa1"), out result, out ptr));

                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "test")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "test123456")));
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

                CedarDataPtr* ptr;
                CedarRef result;

                tree.Add("test", 1);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Assert.Equal(1, root.Header.Ptr->NumberOfEntries);

                tree.Add("test", 2);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Assert.Equal(1, root.Header.Ptr->NumberOfEntries);

                tree.Add("test", 3);
                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(3, ptr->Data);
                Assert.Equal(1, root.Header.Ptr->NumberOfEntries);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(1, root.NumberOfKeys);

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(3, ptr->Data);

                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "test")));
            }
        }


        [Fact]
        public void InsertSameMultipleTimesX10000()
        {
            List<Slice> items = new List<Slice>();
            for (int i = 0; i < 100; i++ )
                items.Add(Slice.From(Allocator, GenerateRandomString(5,2)));


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
                    CedarDataPtr* ptr;
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "users/1"), out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "users/2"), out result, out ptr));
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

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "collections/1"), out result, out ptr));
                Assert.Equal(1, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "collections/2"), out result, out ptr));
                Assert.Equal(2, ptr->Data);
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "collections/3"), out result, out ptr));
                Assert.Equal(3, ptr->Data);

                CedarKeyPair resultKey;
                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "collections/1")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "collections/3")));
            }
        }


        [Fact]
        public void CanAddAndRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("b", 2);

                CedarDataPtr* ptr;
                CedarKeyPair resultKey;

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "b")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "b")));

                tree.Add("c", 3);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "b")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "c")));

                tree.Add("a", 1);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "a")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "c")));


                var actual = tree.Read("a");

                using (var it = tree.Iterate(false))
                {
                    Assert.True(it.Seek(Slice.From(tx.Allocator, "a")));
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

                CedarDataPtr* ptr;
                CedarKeyPair resultKey;
                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                Assert.Equal(6, root.Header.Ptr->NumberOfEntries);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "pool")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "progress")));

                long from = 0;
                long len = 0;
                var outputKey = Slice.Create(tx.Allocator, 4096);
                var iterator = root.Successor(Slice.From(tx.Allocator, "pqql"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "prepare")));

                from = 0;
                len = 0;
                iterator = root.Successor(Slice.From(tx.Allocator, "prfze"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "prize")));

                from = 0;
                len = 0;
                iterator = root.PredecessorOrEqual(Slice.From(tx.Allocator, "prize"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "prize")));

                from = 0;
                len = 0;
                iterator = root.PredecessorOrEqual(Slice.From(tx.Allocator, "pria"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "preview")));

                from = 0;
                len = 0;
                iterator = root.PredecessorOrEqual(Slice.From(tx.Allocator, "prfze"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "preview")));

                from = 0;
                len = 0;
                iterator = root.PredecessorOrEqual(Slice.From(tx.Allocator, "prjze"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "prize")));
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

                var outputKey = Slice.Create(tx.Allocator, 4096);

                long from = 0;
                long len = 0;
                var iterator = root.Predecessor(Slice.From(tx.Allocator, "poop"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "pool")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "priz"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "preview")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "pra"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "pool")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "pr"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "pool")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "pre"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "pool")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "prep"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "pool")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "prepare"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "pool")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "producer"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "produce")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "pooa"), ref outputKey, ref from, ref len);
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
                Assert.Equal(root.Header.Ptr->NumberOfEntries, root.NumberOfKeys);
                var outputKey = Slice.Create(tx.Allocator, 4096);

                long from = 0;
                long len = 0;
                var iterator = root.Predecessor(Slice.From(tx.Allocator, "acer"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "ace")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "ace"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "ac")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "ac"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.Success, (int)iterator.Error);
                Assert.True(SliceComparer.Equals(outputKey, Slice.From(tx.Allocator, "a")));
                outputKey.Reset();

                from = 0;
                len = 0;
                iterator = root.Predecessor(Slice.From(tx.Allocator, "a"), ref outputKey, ref from, ref len);
                Assert.Equal((int)CedarResultCode.NoPath, (int)iterator.Error);
                outputKey.Reset();
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

                CedarDataPtr* ptr;
                CedarKeyPair resultKey;
                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "aa")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "ab")));

                tree.Add("ac", 1);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "aa")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "ac")));

                tree.Add("aca", 3);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "aa")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "aca"), out resultKey, out ptr));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "aca")));

                tree.Add("b", 3);

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "aa")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "b")));
            }
        }


        [Fact]
        public void SingleNodeRemove()
        {
            using (var tx = Env.WriteTransaction())
            {
                var poolSlice = Slice.From(tx.Allocator, "pool");

                var tree = tx.CreateTrie("foo");

                tree.Add(poolSlice, 1);

                CedarDataPtr* ptr;
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
                var firstSlice = Slice.From(tx.Allocator, "test");
                var secondSlice = Slice.From(tx.Allocator, "test1234");


                var tree = tx.CreateTrie("foo");

                tree.Add(firstSlice, 1);
                tree.Add(secondSlice, 2);

                CedarDataPtr* ptr;
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

                CedarDataPtr* ptr;
                CedarRef result;

                tree.Delete("test");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(3, root.NumberOfKeys);

                tree.Delete("test1234");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test1234"), out result, out ptr));
                Assert.Equal(2, root.NumberOfKeys);

                tree.Delete("test12");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test12"), out result, out ptr));
                Assert.Equal(1, root.NumberOfKeys);

                tree.Delete("test123456");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test123456"), out result, out ptr));
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

                CedarDataPtr* ptr;
                CedarRef result;


                tree.Delete("test123456");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test123456"), out result, out ptr));

                Assert.Equal(3, root.NumberOfKeys);

                tree.Delete("test1234");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test1234"), out result, out ptr));
                Assert.Equal(2, root.NumberOfKeys);

                tree.Delete("test12");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test12"), out result, out ptr));
                Assert.Equal(1, root.NumberOfKeys);

                tree.Delete("test");
                Assert.NotEqual((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(0, root.NumberOfKeys);
            }
        }

        [Fact]
        public void CanAddAndReadStats()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice key = Slice.From(tx.Allocator, "test");
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
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                do
                {

                    tree.Add("test-" + count.ToString("0000000"), count);

                    count++;
                }
                while (tree.State.PageCount == 1);

                Assert.Equal(count, tree.State.NumberOfEntries);
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
                        Assert.True(it.Seek(Slice.From(tx.Allocator, "test-" + i.ToString("0000000"))));
                        Assert.Equal("test-" + i.ToString("0000000"), it.CurrentKey.ToString());
                        Assert.Equal(i, it.CreateReaderForCurrent().ReadBigEndianInt64());
                    }
                }
            }
        }

        [Fact]
        public void AfterRandomPageSplitAllDataIsValid()
        {
            Random r = new Random(123);

            int count = 0;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");

                do
                {
                    int value = r.Next()%100000;

                    tree.Add("test-" + value.ToString("0000000"), value);

                    count++;
                }
                while (tree.State.PageCount == 1);

                //Assert.Equal(count, tree.State.NumberOfEntries);
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

                //Assert.Equal(count, tree.State.NumberOfEntries);
                Assert.Equal(3, tree.State.PageCount);
                Assert.Equal(2, tree.State.LeafPages);
                Assert.Equal(1, tree.State.BranchPages);
                Assert.Equal(2, tree.State.Depth);
                Assert.False(tree.State.IsModified);

                using (var it = tree.Iterate(false))
                {
                    for (int i = 0; i < count; i++)
                    {
                        Assert.True(it.Seek(Slice.From(tx.Allocator, "test-" + i.ToString("0000000"))));
                        Assert.Equal("test-" + i.ToString("0000000"), it.CurrentKey.ToString());
                        Assert.Equal(i, it.CreateReaderForCurrent().ReadBigEndianInt64());
                    }
                }
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
