using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Sparrow;
using Voron.Data.Tables;


namespace Voron.Benchmark
{
    /// <summary>
    /// All of the code here is used for generation of structures used during benchmarking
    /// Although methodology is not strictly correct (i.e. for generating deletion numbers
    /// and fetching deletion keys), it is good enough for our purposes.
    /// </summary>
    public class Utils
    {
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


        public static List<Tuple<Slice, Slice>> GenerateUniqueRandomSlicePairs(int amount, int keyLength, int? randomSeed = null)
        {
            Debug.Assert(amount > 0);
            Debug.Assert(keyLength > 0);

            // Generate random key value pairs
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            var keyBuffer = new byte[keyLength];

            // This serves to ensure the uniqueness of keys globally (that way
            // we know the exact number of insertions)
            var added = new HashSet<Slice>(SliceComparer.Instance);
            var pairs = new List<Tuple<Slice, Slice>>();
            int i = 0;

            while (pairs.Count < amount)
            {
                Slice key;
                Slice value;

                generator.NextBytes(keyBuffer);

                ByteStringContext.Scope keyScope =
                    Slice.From(Configuration.Allocator, keyBuffer, ByteStringType.Immutable, out key);

                i++;

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
                    Slice.From(Configuration.Allocator, (byte*)&valueBuffer, sizeof(long), ByteStringType.Immutable, out value);
                }

                pairs.Add(new Tuple<Slice, Slice>(key, value));
                added.Add(key);
            }

            return pairs;
        }

        public static List<Tuple<Slice, long>> GenerateUniqueRandomCedarPairs(
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

                ByteStringContext.Scope keyScope =
                    Slice.From(Configuration.Allocator, keyBuffer, ByteStringType.Immutable, out key);

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

            return pairs;
        }

        public static List<Slice> GenerateWornoutTree(
            StorageEnvironment env,
            Slice treeNameSlice,
            int generationTreeSize,
            int generationBatchSize,
            int keyLength,
            double generationDeletionProbability,
            int? randomSeed = null)
        {
            List<Slice> treeKeys = new List<Slice>();
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value): new Random();
            bool hasTree = false;

            using (var tx = env.ReadTransaction())
            {
                hasTree = tx.ReadTree(treeNameSlice) != null;
                tx.Commit();
            }

            if (hasTree)
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.ReadTree(treeNameSlice);

                    using (var it = tree.Iterate(false))
                    {
                        if (it.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                treeKeys.Add(it.CurrentKey.Clone(Configuration.Allocator, ByteStringType.Immutable));
                            } while (it.MoveNext());
                        }
                    }

                    tx.Commit();
                }
            }
            else
            {
                // Create a tree with enough wearing
                using (var tx = env.WriteTransaction())
                {
                    var values = new List<Tuple<Slice, Slice>>();
                    var tree = tx.CreateTree(treeNameSlice);

                    while (tree.State.NumberOfEntries < generationTreeSize)
                    {
                        // Add BatchSize new keys
                        for (int i = 0; i < generationBatchSize; i++)
                        {
                            // We might run out of values while creating the tree, generate more.
                            if (values.Count == 0)
                            {
                                values = GenerateUniqueRandomSlicePairs(
                                    generationTreeSize,
                                    keyLength,
                                    randomSeed);
                            }

                            var pair = values[0];
                            values.RemoveAt(0);

                            // Add it to the tree key set
                            treeKeys.Add(pair.Item1);

                            // Add it to the tree
                            tree.Add(pair.Item1, pair.Item2);
                        }

                        int deletions = Convert.ToInt32(generationDeletionProbability * generationBatchSize);

                        // Delete the number of deletions given by the binomial rv
                        // We may have gone a little bit over the limit during
                        // insertion, but we rebalance here.
                        if (tree.State.NumberOfEntries > generationTreeSize)
                        {
                            while (tree.State.NumberOfEntries > generationTreeSize)
                            {
                                var keyIndex = generator.Next(treeKeys.Count);
                                // TODO: next two lines will run too slow for big datasets
                                tree.Delete(treeKeys[keyIndex]);
                                treeKeys.RemoveAt(keyIndex);
                            }
                        }
                        else
                        {
                            while (deletions > 0 && tree.State.NumberOfEntries > 0)
                            {
                                var keyIndex = generator.Next(treeKeys.Count);
                                // TODO: next two lines will run too slow for big datasets
                                tree.Delete(treeKeys[keyIndex]);
                                treeKeys.RemoveAt(keyIndex);
                                deletions--;
                            }
                        }
                    }

                    tx.Commit();
                }
            }

            env.FlushLogToDataFile();

            return treeKeys;
        }

        public static List<Slice> GenerateWornoutTable(
            StorageEnvironment env,
            Slice tableNameSlice,
            TableSchema schema,
            int generationTableSize,
            int generationBatchSize,
            int keyLength,
            double generationDeletionProbability,
            int? randomSeed = null
            )
        {
            var tableKeys = new List<Slice>();
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            bool hasTable;

            using (var tx = env.ReadTransaction())
            {
                try
                {
                    tx.OpenTable(schema, tableNameSlice);
                    hasTable = true;
                }
                catch (Exception)
                {
                    hasTable = false;
                }

                tx.Commit();
            }

            if (hasTable)
            {
                using (var tx = env.ReadTransaction())
                {
                    var table = tx.OpenTable(schema, tableNameSlice);

                    foreach (var reader in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
                    {
                        Slice key;
                        schema.Key.GetSlice(Configuration.Allocator, reader, out key);
                        tableKeys.Add(key);
                    }

                    tx.Commit();
                }
            }
            else
            {
                // Create a table with enough wearing
                using (var tx = env.WriteTransaction())
                {
                    var values = new List<Tuple<Slice, Slice>>();
                    schema.Create(tx, tableNameSlice, 16);
                    var table = tx.OpenTable(schema, tableNameSlice);

                    while (table.NumberOfEntries < generationTableSize)
                    {
                        // Add BatchSize new keys
                        for (int i = 0; i < generationBatchSize; i++)
                        {
                            // We might run out of values while creating the table, generate more.
                            if (values.Count == 0)
                            {
                                values = GenerateUniqueRandomSlicePairs(
                                    generationTableSize,
                                    keyLength,
                                    randomSeed);
                            }

                            var pair = values[0];
                            values.RemoveAt(0);

                            // Add it to the table key set
                            tableKeys.Add(pair.Item1);

                            // Add it to the table
                            table.Insert(new TableValueBuilder
                                {
                                    pair.Item1,
                                    pair.Item2
                                });
                        }

                        int deletions = Convert.ToInt32(generationDeletionProbability * generationBatchSize);

                        // Delete the number of deletions given by the binomial rv
                        // We may have gone a little bit over the limit during
                        // insertion, but we rebalance here.
                        if (table.NumberOfEntries > generationTableSize)
                        {
                            while (table.NumberOfEntries > generationTableSize)
                            {
                                var keyIndex = generator.Next(tableKeys.Count);
                                // TODO: next two lines will run too slow for big datasets
                                table.DeleteByKey(tableKeys[keyIndex]);
                                tableKeys.RemoveAt(keyIndex);
                            }
                        }
                        else
                        {
                            while (deletions > 0 && table.NumberOfEntries > 0)
                            {
                                var keyIndex = generator.Next(tableKeys.Count);
                                // TODO: next two lines will run too slow for big datasets
                                table.DeleteByKey(tableKeys[keyIndex]);
                                tableKeys.RemoveAt(keyIndex);
                                deletions--;
                            }
                        }
                    }

                    tx.Commit();
                }
            }

            env.FlushLogToDataFile();

            return tableKeys;
        }

        public static List<Slice> GenerateWornoutTrie(
            StorageEnvironment env,
            Slice trieNameSlice,
            int generationTrieSize,
            int generationBatchSize,
            int keyLength,
            double generationDeletionProbability,
            int? randomSeed = null,
            bool useAscii = false,
            int clusterSize = 0)
        {
            List<Slice> trieKeys = new List<Slice>();
            var generator = randomSeed.HasValue ? new Random(randomSeed.Value) : new Random();
            bool hasTrie = false;

            using (var tx = env.ReadTransaction())
            {
                hasTrie = tx.ReadTrie(trieNameSlice) != null;
                tx.Commit();
            }

            if (hasTrie)
            {
                using (var tx = env.ReadTransaction())
                {
                    var trie = tx.ReadTrie(trieNameSlice);

                    using (var it = trie.Iterate(false))
                    {
                        if (it.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                trieKeys.Add(it.CurrentKey.Clone(Configuration.Allocator, ByteStringType.Immutable));
                            } while (it.MoveNext());
                        }
                    }

                    tx.Commit();
                }
            }
            else
            {
                // Create a tree with enough wearing
                using (var tx = env.WriteTransaction())
                {
                    var values = new List<Tuple<Slice, long>>();
                    var trie = tx.CreateTrie(trieNameSlice);

                    while (trie.State.NumberOfEntries < generationTrieSize)
                    {
                        // Add BatchSize new keys
                        for (int i = 0; i < generationBatchSize; i++)
                        {
                            // We might run out of values while creating the tree, generate more.
                            if (values.Count == 0)
                            {
                                values = GenerateUniqueRandomCedarPairs(
                                    generationTrieSize,
                                    keyLength,
                                    randomSeed,
                                    useAscii,
                                    clusterSize);
                            }

                            var pair = values[0];
                            values.RemoveAt(0);

                            // Add it to the tree key set
                            trieKeys.Add(pair.Item1);

                            // Add it to the tree
                            trie.Add(pair.Item1, pair.Item2);
                        }

                        int deletions = Convert.ToInt32(generationDeletionProbability * generationBatchSize);

                        // Delete the number of deletions given by the binomial rv
                        // We may have gone a little bit over the limit during
                        // insertion, but we rebalance here.
                        if (trie.State.NumberOfEntries > generationTrieSize)
                        {
                            while (trie.State.NumberOfEntries > generationTrieSize)
                            {
                                var keyIndex = generator.Next(trieKeys.Count);
                                // TODO: next two lines will run too slow for big datasets
                                trie.Delete(trieKeys[keyIndex]);
                                trieKeys.RemoveAt(keyIndex);
                            }
                        }
                        else
                        {
                            while (deletions > 0 && trie.State.NumberOfEntries > 0)
                            {
                                var keyIndex = generator.Next(trieKeys.Count);
                                // TODO: next two lines will run too slow for big datasets
                                trie.Delete(trieKeys[keyIndex]);
                                trieKeys.RemoveAt(keyIndex);
                                deletions--;
                            }
                        }
                    }

                    tx.Commit();
                }
            }

            env.FlushLogToDataFile();

            return trieKeys;
        }
    }
}