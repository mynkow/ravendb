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

                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(1, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(256, root.Header.Ptr->Size);

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

                Assert.Equal(2, root.NumberOfKeys);
                Assert.Equal(5, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(512, root.Header.Ptr->Size);

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
                Assert.Equal(6, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(3, root.NumberOfKeys);
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
                Assert.Equal(12, root.NonZeroSize);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(512, root.Header.Ptr->Size);
                Assert.Equal(4, root.NumberOfKeys);
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
                tree.Add("test", 1);
                tree.Add("test", 2);
                tree.Add("test", 3);

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(1, root.NumberOfKeys);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

                Assert.Equal(1, root.NumberOfKeys);

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal(3, ptr->Data);
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

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

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

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);

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

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetFirst(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "pool")));

                Assert.Equal((int)CedarResultCode.Success, (int)root.GetLast(out resultKey, out ptr));
                Assert.True(SliceComparer.Equals(resultKey.Key, Slice.From(tx.Allocator, "progress")));               
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
        public void AfterPageSplitAllDataIsValid()
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
    }
}
