using Sparrow.Collections;
using Sparrow.Collections.Cedar;
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
        public void SingleInsertAndQuery()
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

            int result;
            Assert.Equal((int)ErrorCode.Success, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out result));
            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("tes"), out result));
            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test1"), out result));
            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("a"), out result));
        }

        [Fact]
        public void InsertSplitAtTheBeginningAndQuery()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("aest"), 2);

            Assert.Equal(2, trie.NumberOfKeys);
            Assert.Equal(256, trie.Size);
            Assert.Equal(2, trie.NonZeroSize);
            Assert.Equal(16, trie.NonZeroLength);
            Assert.Equal(2048, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(256, trie.Capacity);

            int result;
            Assert.Equal((int)ErrorCode.Success, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out result));
            Assert.Equal(1, result);

            Assert.Equal((int)ErrorCode.Success, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("aest"), out result));
            Assert.Equal(2, result);

            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("tes"), out result));
            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test1"), out result));
            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("aest1"), out result));            
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
        public void InsertSelfContainedAndQuery()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("test1234"), 2);
            Assert.Equal(2, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("test12"), 3);
            Assert.Equal(3, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("test123456"), 4);
            Assert.Equal(4, trie.NumberOfKeys);

            Assert.Equal(512, trie.Size);
            Assert.Equal(12, trie.NonZeroSize);
            Assert.Equal(6, trie.NonZeroLength);
            Assert.Equal(4096, trie.TotalSize);
            Assert.Equal(8, trie.UnitSize);
            Assert.Equal(512, trie.Capacity);


            int result;
            Assert.Equal((int)ErrorCode.Success, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test1234"), out result));
            Assert.Equal(2, result);

            Assert.Equal((int)ErrorCode.Success, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test123456"), out result));
            Assert.Equal(4, result);

            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test123"), out result));
            Assert.Equal((int)ErrorCode.NoValue, (int)trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test13"), out result));
        }

        [Fact]
        public void InsertSameMultipleTimes()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("test"), 2);
            Assert.Equal(1, trie.NumberOfKeys);

            int result;
            trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out result);
            Assert.Equal(2, result);
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
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("collections/2"), 1);
            Assert.Equal(2, trie.NumberOfKeys);

            trie.Update(Encoding.UTF8.GetBytes("collections/3"), 1);
            Assert.Equal(3, trie.NumberOfKeys);
        }

        [Fact]
        public void CommonPrefixPredict()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("collections/1"), 1);
            trie.Update(Encoding.UTF8.GetBytes("collections/2"), 2);
            trie.Update(Encoding.UTF8.GetBytes("collections/3"), 3);
            trie.Update(Encoding.UTF8.GetBytes("users/1"), 4);
            trie.Update(Encoding.UTF8.GetBytes("users/2"), 5);
            trie.Update(Encoding.UTF8.GetBytes("users/3"), 6);

            var results = trie.CommonPrefixPredict(Encoding.UTF8.GetBytes("collections"), 4);
            Assert.Equal(3, results.Count);

            results = trie.CommonPrefixPredict(Encoding.UTF8.GetBytes("collections"), 2);
            Assert.Equal(2, results.Count);
        }

        [Fact]
        public void CommonPrefixPredict2()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("test1234"), 2);
            trie.Update(Encoding.UTF8.GetBytes("test12"), 3);
            trie.Update(Encoding.UTF8.GetBytes("test123456"), 4);

            var results = trie.CommonPrefixPredict(Encoding.UTF8.GetBytes("t"), 4);
            Assert.Equal(4, results.Count);
            results = trie.CommonPrefixPredict(Encoding.UTF8.GetBytes("test123"), 4);
            Assert.Equal(2, results.Count);
        }


        [Fact]
        public void RemoveLowerToHigher()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("test1234"), 2);
            trie.Update(Encoding.UTF8.GetBytes("test12"), 3);
            trie.Update(Encoding.UTF8.GetBytes("test123456"), 4);

            int _;
            Assert.Equal(4, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out _));
            Assert.Equal(3, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test12"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test12"), out _));
            Assert.Equal(2, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test1234"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test1234"), out _));
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test123456"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test123456"), out _));
            Assert.Equal(0, trie.NumberOfKeys);
        }

        [Fact]
        public void RemoveHigherToLower()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("test1234"), 2);
            trie.Update(Encoding.UTF8.GetBytes("test12"), 3);
            trie.Update(Encoding.UTF8.GetBytes("test123456"), 4);

            int _;
            Assert.Equal(4, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test123456"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test123456"), out _));
            Assert.Equal(3, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test1234"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test1234"), out _));
            Assert.Equal(2, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test12"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test12"), out _));
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out _));
            Assert.Equal(0, trie.NumberOfKeys);
        }

        [Fact]
        public void SingleNodeRemove()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);

            int _;
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out _));
            Assert.Equal(0, trie.NumberOfKeys);
        }

        [Fact]
        public void DualNodeRemove()
        {
            var trie = new CedarTrie<int>();

            trie.Update(Encoding.UTF8.GetBytes("test"), 1);
            trie.Update(Encoding.UTF8.GetBytes("test1234"), 2);

            int _;
            Assert.Equal(2, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test1234"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test1234"), out _));
            Assert.Equal(1, trie.NumberOfKeys);

            trie.Remove(Encoding.UTF8.GetBytes("test"));
            Assert.NotEqual(ErrorCode.Success, trie.ExactMatchSearch(Encoding.UTF8.GetBytes("test"), out _));
            Assert.Equal(0, trie.NumberOfKeys);
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
                    new object[] { 100, 16, 1000 }
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
                var keysInOrder = new List<string>();

                _seed = (uint)iter;
                //_seed = 120;

                Console.WriteLine($"Trying with seed {iter}.");

                var tree = new CedarTrie<int>();
                try
                {
                    //int lastEstimatedSize = 0;

                    int keyLength = 0;
                    var keys = new HashSet<string>();
                    for (int i = 0; i < count; i++)
                    {
                        //string key = GenerateSequential(prefixSize);
                        //string key = GenerateRandomString(generator, size, prefixSize);
                        string key = GenerateRandomString(size, prefixSize);

                        keyLength = key.Length;

                        keysInOrder.Add(key);
                        tree.Update(Encoding.UTF8.GetBytes(key), 1);
                        keys.Add(key);

                        //if (keys.Count != tree.NumberOfKeys)
                        //    throw new Exception();

                        //int d = tree.NonZeroSize; // Forcing the scanning of the tree for it to die. 
                        //d = tree.NonZeroLength; // Forcing the scanning of the tree for it to die. 


                        if ( i % 100 == 0)
                        {
                            Console.Clear();

                            Console.WriteLine($"Keys = {keys.Count}");
                            Console.WriteLine($"Total Size = {tree.AllocatedSize}");
                            Console.WriteLine($"Total Size (64kb) = {tree.AllocatedSize64Kb}");
                            Console.WriteLine($"BTree Data Size = {(keys.Count * (keyLength + sizeof(short)))}");
                            Console.WriteLine("---------");

                            tree.DumpInformation();
                            Console.WriteLine();
                            tree.DumpInformation64Kb();
                            Console.WriteLine();

                            Console.ReadLine();
                        }

                        //int estimatedSize = tree.AllocatedSize;

                        //if (lastEstimatedSize != estimatedSize)
                        //{
                        //    if (estimatedSize > 8 * 1024)
                        //    {
                        //        Console.Clear();

                        //        Console.WriteLine($"Keys = {tree.NumberOfKeys}");
                        //        Console.WriteLine($"Size = {tree.Size}");
                        //        Console.WriteLine($"Non Zero Size = {tree.NonZeroSize}");
                        //        Console.WriteLine($"Non Zero Length = {tree.NonZeroLength}");
                        //        Console.WriteLine($"UnitSize = {tree.UnitSize}");
                        //        Console.WriteLine($"Capacity = {tree.Capacity}");
                        //        Console.WriteLine("---------");
                        //        Console.WriteLine($"Total Size = {estimatedSize}");
                        //        Console.WriteLine($"Real Data Size = {(tree.NumberOfKeys * (key.Length + sizeof(int)))}");
                        //        tree.DumpInformation();
                        //    }

                        //    lastEstimatedSize = estimatedSize;
                        //}

                        //tree.Test();


                    }

                    Console.Clear();

                    Console.WriteLine($"Keys = {keys.Count}");
                    Console.WriteLine($"Total Size = {tree.AllocatedSize}");
                    Console.WriteLine($"Total Size (64kb) = {tree.AllocatedSize64Kb}");
                    Console.WriteLine($"BTree Data Size = {(keys.Count * (keyLength + sizeof(short)))}");
                    Console.WriteLine("---------");

                    tree.DumpInformation();
                    Console.WriteLine();
                    tree.DumpInformation64Kb();
                    Console.WriteLine();

                    Console.ReadLine();

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

            if (bestCase == null)
            {
                Console.WriteLine("All successful");
                Console.ReadLine();
                return;
            }

            Console.WriteLine("Example:");
            for (int i = 0; i < bestCase.Count; i++)
                Console.WriteLine(bestCase[i]);

            Console.ReadLine();
        }


        private string[] clusteredPrefixes;

        static int i = 0;
        private string GenerateSequential(int clusteredSize)
        {
            if ( clusteredPrefixes == null )
            {
                clusteredPrefixes = new string[10];
                
                for ( int start = 0; start < 10; start++ )
                {
                    var stringChars = new char[clusteredSize];
                    for (int i = 0; i < clusteredSize; i++)
                        stringChars[i] = chars[(int)((start + i) % chars.Length)];

                    clusteredPrefixes[start] = new string(stringChars);
                }
            }

            _seed = (_seed * 23 + 7);
            var cluster = _seed % 10;

            i++;
            return clusteredPrefixes[cluster] + i;
        }
    }
}
