using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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

        [Fact]
        public void InsertSplitAtTheBeginning()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("aest"), 1);

            Assert.Equal(2, trie.NumberOfKeys);
            Assert.Equal(256, trie.Size);
            Assert.Equal(2, trie.NonZeroSize);
            Assert.Equal(16, trie.NonZeroLength);
            Assert.Equal(2048, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(256, trie.Capacity);
        }

        [Fact]
        public void InsertSplitAtTheEnd()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("tesa"), 1);

            Assert.Equal(2, trie.NumberOfKeys);
            Assert.Equal(512, trie.Size);
            Assert.Equal(5, trie.NonZeroSize);
            Assert.Equal(10, trie.NonZeroLength);
            Assert.Equal(4096, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(512, trie.Capacity);
        }

        [Fact]
        public void InsertSplitAtTheEndAndMiddle()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            Assert.Equal(1, trie.NumberOfKeys);
            trie.Update(Encoding.UTF8.GetBytes("tesa"), 1);
            Assert.Equal(2, trie.NumberOfKeys);
            trie.Update(Encoding.UTF8.GetBytes("tasa"), 1);
            Assert.Equal(3, trie.NumberOfKeys);

            Assert.Equal(512, trie.Size);
            Assert.Equal(6, trie.NonZeroSize);
            Assert.Equal(17, trie.NonZeroLength);
            Assert.Equal(4096, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(512, trie.Capacity);
        }

        [Fact]
        public void InsertSelfContained()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("test1234"), 1);
            Assert.Equal(2, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("test12"), 1);
            Assert.Equal(3, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("test123456"), 1);
            Assert.Equal(4, trie.NumberOfKeys);

            Assert.Equal(512, trie.Size);
            Assert.Equal(12, trie.NonZeroSize);
            Assert.Equal(6, trie.NonZeroLength);
            Assert.Equal(4096, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(512, trie.Capacity);
        }

        [Fact]
        public void InsertSameMultipleTimes()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            Assert.Equal(1, trie.NumberOfKeys);
        }

        [Fact]
        public void InsertBasic()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("users/1"), 1);
            trie.Update(Encoding.UTF8.GetBytes("users/2"), 1);
            Assert.Equal(2, trie.NumberOfKeys);
        }

        [Fact]
        public void InsertBasic2()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("collections/1"), 1);
            trie.Update(Encoding.UTF8.GetBytes("collections/2"), 1);
            trie.Update(Encoding.UTF8.GetBytes("collections/3"), 1);
            Assert.Equal(3, trie.NumberOfKeys);
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
                stringChars[i] = chars[(int) ( (clusterStart + i) % chars.Length )];

            for (int i = clusteredSize; i < stringChars.Length; i++)
            {
                _seed = (_seed * 23 + 7);
                stringChars[i] = chars[(int) (_seed % chars.Length)];
            }
                
            return new String(stringChars);
        }


        public static IEnumerable<object[]> TreeSize
        {
            get
            {
                // Or this could read from a file. :)
                return new[]
                {
                    new object[] { 102, 4, 4 },
                    new object[] { 100, 4, 8 },
                    new object[] { 101, 2, 128 },
                    new object[] { 100, 8, 5 },
                    new object[] { 100, 16, 168 },
                    new object[] { 100, 16, 10000 }
                };
            }
        }

        [Theory, MemberData("TreeSize")]
        public void CappedSizeInsertion(int seed, int size, int count)
        {
            var generator = new Random(seed);

            var tree = new CedarTrie<int>();

            var keys = new HashSet<string>();
            for (int i = 0; i < count; i++)
            {
                string key = GenerateRandomString(generator, size, 0);

                tree.Update(Encoding.UTF8.GetBytes(key), 1);
                keys.Add(key);
            }

            Assert.Equal(keys.Count, tree.NumberOfKeys);

            tree.Test();
        }

        //bool @switch = false;

        public void TestCaseGenerator(int seed, int size, int count, int prefixSize = 0)
        {
            var generator = new Random(seed);

            
            List<string> bestCase = null;
            for ( int iter = 0; iter < 1000; iter++ )
            {
                iter = 51;

                var keysInOrder = new List<string>();

                _seed = (uint)iter;

                Console.WriteLine($"Trying with seed {iter}.");

                var tree = new CedarTrie<int>();
                try
                {
                    //int lastEstimatedSize = 0;

                    var keys = new HashSet<string>();
                    for (int i = 0; i < count; i++)
                    {
                        //string key = GenerateSequential();
                        //string key = GenerateRandomString(generator, size, prefixSize);
                        string key = GenerateRandomString(size, prefixSize);

                        keysInOrder.Add(key);
                        tree.Update(Encoding.UTF8.GetBytes(key), 1);
                        keys.Add(key);

                        if (keys.Count != tree.NumberOfKeys)
                            throw new Exception();

                        int d = tree.NonZeroSize; // Forcing the scanning of the tree for it to die. 
                        d = tree.NonZeroLength; // Forcing the scanning of the tree for it to die. 

                        //int treeSize = tree.Size;
                        //int estimatedSize = treeSize * ((Unsafe.SizeOf<node>() / 2) + Unsafe.SizeOf<ninfo>()) + (treeSize >> 8) * Unsafe.SizeOf<block>();

                        //if (lastEstimatedSize != estimatedSize)
                        //{
                        //    if (estimatedSize > 8 * 1024)
                        //    {
                        //        Console.WriteLine($"Keys = {tree.NumberOfKeys - 1}");
                        //        Console.WriteLine($"Size = {tree.Size}");
                        //        Console.WriteLine($"Non Zero Size = {tree.NonZeroSize}");
                        //        Console.WriteLine($"Non Zero Length = {tree.NonZeroLength}");
                        //        Console.WriteLine($"Total Size = {lastEstimatedSize}");
                        //        Console.WriteLine($"UnitSize = {tree.UnitSize}");
                        //        Console.WriteLine($"Capacity = {tree.Capacity}");
                        //    }

                        //    lastEstimatedSize = estimatedSize;
                        //}

                        tree.Test();
                    }

                    //if (@switch)
                    //{
                    //    Console.WriteLine($"Keys = {tree.NumberOfKeys}");
                    //    Console.WriteLine($"Size = {tree.Size}");
                    //    Console.WriteLine($"Non Zero Size = {tree.NonZeroSize}");
                    //    Console.WriteLine($"Non Zero Length = {tree.NonZeroLength}");
                    //    Console.WriteLine($"Total Size = {tree.TotalSize}");
                    //    Console.WriteLine($"UnitSize = {tree.UnitSize}");
                    //    Console.WriteLine($"Capacity = {tree.Capacity}");
                    //}
                }
                catch
                {
                    // We have a new best case.
                    if (bestCase == null || bestCase.Count > keysInOrder.Count )
                    {
                        bestCase = keysInOrder;
                        Console.WriteLine($"New best case of {bestCase.Count} elements with seed {iter}.");
                    }                        
                }                
            }

            if ( bestCase == null )
            {
                Console.WriteLine("All successful");
                Console.ReadLine();
                return;
            }

            Console.WriteLine("Example:");
            for ( int i = 0; i < bestCase.Count; i++ )
                Console.WriteLine(bestCase[i]);

            Console.ReadLine();
        }

        static int i = 0;
        private string GenerateSequential()
        {
            i++;
            return $"collections/{i}";
        }
    }
}
