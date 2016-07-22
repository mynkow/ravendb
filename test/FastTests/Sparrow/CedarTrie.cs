using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Sparrow
{
    public class CedarTrieTests
    {
        [Fact]
        public void Construction()
        {
            var trie = new CedarTrie<int>();

            Assert.Equal(0, trie.NumberOfKeys);
            Assert.Equal(256, trie.Size);
            Assert.Equal(0, trie.NonZeroSize);
            Assert.Equal(0, trie.NonZeroLength);
            Assert.Equal(2048, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(256, trie.Capacity);
        }

        [Fact]
        public void SingleInsert()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            
            Assert.Equal(1, trie.NumberOfKeys);
            Assert.Equal(256, trie.Size);
            Assert.Equal(1, trie.NonZeroSize);
            Assert.Equal(8, trie.NonZeroLength);
            Assert.Equal(2048, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(256, trie.Capacity);
        }
    }
}
