using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using Voron;
using Voron.Global;

namespace FastTests.Voron.Cedar
{
    public class Basic : StorageTest
    {

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;
            base.Configure(options);
        }

        [Fact]
        public void CanAdd()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                tree.Add("test", 1);
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
            const int count = 4096;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTrie("foo");
                for (int i = 0; i < count; i++)
                {
                   tree.Add("test-" + i.ToString("000"), i);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie("foo");
                using (var it = tree.Iterate(false))
                {
                    for (int i = 0; i < count; i++)
                    {
                        Assert.True(it.Seek(Slice.From(tx.Allocator, "test-" + i.ToString("000"))));
                        Assert.Equal("test-" + i.ToString("000"), it.CurrentKey.ToString());
                        Assert.Equal(i, it.CreateReaderForCurrent().ReadBigEndianInt64());
                    }
                }
            }
        }
    }
}
