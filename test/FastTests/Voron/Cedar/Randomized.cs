using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow;
using Voron;
using Voron.Global;
using Xunit;
using Voron.Data.BTrees;

namespace FastTests.Voron.Cedar
{
    public class RandomizedCedarTests : StorageTest
    {        
        public static IEnumerable<object[]> InsertionParams
        {
            get
            {
                var r = new Random();
                int seed = r.Next();

                yield return new[] {new InsertionOptions(10, seed)};
                yield return new[] {new InsertionOptions(100, seed)};
                yield return new[] {new InsertionOptions(1000, seed)};
                yield return new[] {new InsertionOptions(10000, seed)};
                yield return new[] { new InsertionOptions(100000, seed) };
                yield return new[] { new InsertionOptions(250000, seed) };
            }
        }

        public class InsertionOptions
        {
            public int TreeSize;
            public int Seed;
            public int KeyLength;
            public int ClusterSize;            

            public InsertionOptions(int size, int seed)
            {
                TreeSize = size;
                Seed = seed;
                KeyLength = 50;
                ClusterSize = 10;
            }
        }

        [Theory]
        [MemberData(nameof(InsertionParams))]
        public void InsertionsSingleTransaction(InsertionOptions options)
        {
            Slice treeName;
            Slice.From(Allocator, "Trie", out treeName);

            var totalPairs = GenerateUniqueRandomCedarPairs(options.TreeSize, options.KeyLength, options.Seed, true, options.ClusterSize);

            using (var tx = Env.WriteTransaction())
            {
                var trie = tx.CreateTrie(treeName);

                int count = 0;
                foreach (var pair in totalPairs)
                {
                    trie.Add(pair.Item1, pair.Item2);
                    count++;

                    if (count == 101)
                    {
                        using (var it = trie.Iterate(false))
                        {
                            Assert.True(it.Seek(Slices.BeforeAllKeys));

                            string lastKey = Slices.Empty.ToString();
                            while (it.MoveNext())
                            {
                                string currentKey = it.CurrentKey.ToString();
                                if (String.Compare(lastKey, currentKey, StringComparison.Ordinal) >= 0)
                                    throw new Exception($"Last key {lastKey} is greater or equal than {currentKey}");

                                lastKey = currentKey;
                            }
                        }
                    }
                }
                   

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTrie(treeName);

                if (totalPairs.Count != tree.State.NumberOfEntries)
                    throw new Exception("Inserted/Deleted and the current amount stored do not match.");

                if (tree.State.IsModified)
                    throw new Exception("Tree state cannot be modified in a read transaction.");

                int count = 0;
                using (var it = tree.Iterate(false))
                {
                    Assert.True(it.Seek(Slices.BeforeAllKeys));

                    string lastKey = Slices.Empty.ToString();
                    while (it.MoveNext())
                    {
                        string currentKey = it.CurrentKey.ToString();
                        if (String.Compare(lastKey, currentKey, StringComparison.Ordinal) >= 0)
                            throw new Exception($"Last key {lastKey} is greater or equal than {currentKey}");

                        lastKey = currentKey;

                        count++;
                    }
                }

                Assert.Equal(totalPairs.Count, count);
            }
        }

        [Theory]
        [MemberData(nameof(InsertionParams))]
        public void InsertionsAndRemovalsSingleTransaction(InsertionOptions options)
        {
            Slice treeName;
            Slice.From(Allocator, $"Trie-{options.Seed}-{options.GetHashCode()}", out treeName);

            var totalPairs = GenerateUniqueRandomCedarPairs(options.TreeSize, 50, options.Seed, true, 20);

            var r = new Random(options.Seed);

            var currentValues = new HashSet<string>();
            using (var tx = Env.WriteTransaction())
            {
                var trie = tx.CreateTrie(treeName);

                var root = new CedarPage(tx.LowLevelTransaction, trie.State.RootPageNumber);

                for (int i = 0; i < totalPairs.Count; i++)
                {
                    var pair = totalPairs[i];
                    trie.Add(pair.Item1, pair.Item2);
                    currentValues.Add(pair.Item1.ToString());

                    if (currentValues.Count != trie.State.NumberOfEntries)
                        throw new Exception("Inserted/Deleted and the current amount stored do not match.");

                    if ( r.Next(2) == 0 )
                    {
                        int tries = 0;
                        int positionToDelete = r.Next(i);
                        while (!currentValues.Contains(totalPairs[positionToDelete].Item1.ToString()) && currentValues.Count != 0 && tries < 10 )
                        {
                            positionToDelete = r.Next(i);
                            tries++;
                        }
                            
                        if (currentValues.Count == 0 || tries >= 10)
                            continue;

                        var toDelete = totalPairs[positionToDelete].Item1;
                        trie.Delete(toDelete);
                        currentValues.Remove(toDelete.ToString());

                        if (currentValues.Count != trie.State.NumberOfEntries)
                            throw new Exception("Inserted/Deleted and the current amount stored do not match.");
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTrie(treeName);

                if (currentValues.Count != tree.State.NumberOfEntries)
                    throw new Exception("Inserted/Deleted and the current amount stored do not match.");

                if (tree.State.IsModified)
                    throw new Exception("Tree state cannot be modified in a read transaction.");

                int count = 0;
                using (var it = tree.Iterate(false))
                {
                    Assert.True(it.Seek(Slices.BeforeAllKeys));

                    string lastKey = Slices.Empty.ToString();
                    while (it.MoveNext())
                    {
                        string currentKey = it.CurrentKey.ToString();

                        if (String.Compare(lastKey, currentKey, StringComparison.Ordinal) >= 0)
                            throw new Exception($"Last key {lastKey} is greater or equal than {currentKey}");
                        if (!currentValues.Contains(currentKey))
                            throw new Exception($"The key {currentKey} should not be in the tree.");

                        lastKey = currentKey;

                        count++;
                    }
                }

                Assert.Equal(currentValues.Count, count);                
            }
        }

        #region Utility Methods

        private static readonly byte[] Chars = Encoding.ASCII.GetBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");

        private static void GenerateRandomString(Random generator, int clusteredSize, byte[] buffer)
        {
            var clusterStart = generator.Next(10);

            for (int i = 0; i < clusteredSize; i++)
            {
                buffer[i] = Chars[(clusterStart + i) % Chars.Length];
            }

            for (int i = clusteredSize; i < buffer.Length; i++)
            {
                buffer[i] = Chars[generator.Next(Chars.Length)];
            }
        }


        public List<Tuple<Slice, Slice>> GenerateUniqueRandomSlicePairs(int amount, int keyLength, int? randomSeed = null)
        {
            // Generate random key value pairs
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            var keyBuffer = new byte[keyLength];

            // This serves to ensure the uniqueness of keys globally (that way
            // we know the exact number of insertions)
            var added = new HashSet<Slice>(SliceComparer.Instance);
            var pairs = new List<Tuple<Slice, Slice>>();

            while (pairs.Count < amount)
            {
                Slice key;
                Slice value;

                generator.NextBytes(keyBuffer);

                ByteStringContext.Scope keyScope = Slice.From(Allocator, keyBuffer, ByteStringType.Immutable, out key);

                if (added.Contains(key))
                {
                    // Release the unused key's memory
                    keyScope.Dispose();
                    continue;
                }

                // Trees are mostly used by Table to store long values. We
                // attempt to emulate that behavior
                long valueBuffer = generator.Next();
                valueBuffer += (long)generator.Next() << 32;
                valueBuffer += (long)generator.Next() << 64;
                valueBuffer += (long)generator.Next() << 96;

                unsafe
                {
                    Slice.From(Allocator, (byte*)&valueBuffer, sizeof(long), ByteStringType.Immutable, out value);
                }

                pairs.Add(new Tuple<Slice, Slice>(key, value));
                added.Add(key);
            }

            return pairs;
        }

        public List<Tuple<Slice, long>> GenerateUniqueRandomCedarPairs(
            int amount,
            int keyLength,
            int? randomSeed = null,
            bool useAscii = false,
            int clusterSize = 0)
        {
            // Generate random key value pairs
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            var keyBuffer = new byte[keyLength];

            // This serves to ensure the uniqueness of keys globally (that way
            // we know the exact number of insertions)
            var added = new HashSet<Slice>(SliceComparer.Instance);
            var pairs = new List<Tuple<Slice, long>>();

            while (pairs.Count < amount)
            {
                Slice key;

                if (useAscii)
                {
                    GenerateRandomString(generator, clusterSize, keyBuffer);
                }
                else
                {
                    generator.NextBytes(keyBuffer);

                    for (int i = 0; i < keyBuffer.Length; i++)
                    {
                        if (keyBuffer[i] == 0)
                        {
                            keyBuffer[i]++;
                        }
                    }
                }

                ByteStringContext.Scope keyScope = Slice.From(Allocator, keyBuffer, ByteStringType.Immutable, out key);

                if (added.Contains(key))
                {
                    // Release the unused key's memory
                    keyScope.Dispose();
                    continue;
                }

                // Trees are mostly used by Table to store long values. We
                // attempt to emulate that behavior
                long value = generator.Next();
                value += (long)generator.Next() << 32;
                value += (long)generator.Next() << 64;
                value += (long)generator.Next() << 96;

                pairs.Add(new Tuple<Slice, long>(key, value));
                added.Add(key);
            }

            pairs.Sort(Comparison);

            return pairs;
        }

        private int Comparison(Tuple<Slice, long> a, Tuple<Slice, long> b)
        {
            return SliceComparer.CompareInline(a.Item1, b.Item1);
        }

        #endregion
    }
}
