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
                //Assert.Equal(8, root.NonZeroLength);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");

                var root = new CedarPage(tx.LowLevelTransaction, tree.State.RootPageNumber);
                Assert.Equal(256, root.Header.Ptr->Size);

                Assert.Equal(1, root.NumberOfKeys);
                Assert.Equal(1, root.NonZeroSize);
                //Assert.Equal(8, root.NonZeroLength);

                CedarDataPtr* ptr;
                CedarRef result;
                Assert.Equal((int)CedarResultCode.Success, (int)root.ExactMatchSearch(Slice.From(tx.Allocator, "test"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator,"tes"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator,"test1"), out result, out ptr));
                Assert.Equal((int)CedarResultCode.NoValue, (int)root.ExactMatchSearch(Slice.From(tx.Allocator,"a"), out result, out ptr));
            }
        }

        [Fact]
        public void CanAddAndRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("b", 2);
                tree.Add("c", 3);
                tree.Add("a", 1);
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
