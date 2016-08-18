using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Sparrow;

namespace Voron.Benchmark.Cedar
{
    public class CedarInsertRandom : StorageBenchmark
    {
        private static readonly Slice TrieNameSlice;

        private List<Tuple<Slice, long>>[] _pairs;

        /// <summary>
        /// Size of tree to create in order to write from (in number of nodes).
        /// This is the TOTAL SIZE after deletions
        /// </summary>
        [Params(Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public int GenerationTrieSize { get; set; }

        /// <summary>
        /// Size of batches to divide the insertion into. A lower number will
        /// generate more frequent deletions.
        /// 
        /// Beware of low batch sizes. Even though they generate a good wearing
        /// in the tree, too low of a number here may take a long time to
        /// converge.
        /// </summary>
        [Params(50000)]
        public int GenerationBatchSize { get; set; }

        /// <summary>
        /// Probability that a node will be deleted after insertion.
        /// </summary>
        [Params(0.1)]
        public double GenerationDeletionProbability { get; set; }

        /// <summary>
        /// First component of the array tells if we should use ASCII. The
        /// second one is the amount of clustering in the event that we do
        /// use it.
        /// </summary>
        [Params(0, 3)]
        public long UseAscii { get; set; }
        public bool ShouldUseAscii => (UseAscii & 0x1) > 0;
        public int AsciiClusterSize => (int)((UseAscii & ((long)0xFFFFFFFF << 1)) >> 1);

        static CedarInsertRandom()
        {
            Slice.From(Configuration.Allocator, "CedarInsertRandom", ByteStringType.Immutable, out TrieNameSlice);
        }

        /// <summary>
        /// Ensure we don't have to re-create the Trie between benchmarks
        /// </summary>
        public CedarInsertRandom() : base(true, true, false)
        {

        }

        [Setup]
        public override void Setup()
        {
            base.Setup();

            Utils.GenerateWornoutTrie(
                Env,
                TrieNameSlice,
                GenerationTrieSize,
                GenerationBatchSize,
                KeyLength,
                GenerationDeletionProbability,
                RandomSeed,
                ShouldUseAscii,
                AsciiClusterSize);

            var totalPairs = Utils.GenerateUniqueRandomCedarPairs(
                NumberOfTransactions * NumberOfRecordsPerTransaction,
                KeyLength,
                RandomSeed,
                ShouldUseAscii,
                AsciiClusterSize);

            _pairs = new List<Tuple<Slice, long>>[NumberOfTransactions];

            for (var i = 0; i < NumberOfTransactions; ++i)
            {
                _pairs[i] = totalPairs.Take(NumberOfRecordsPerTransaction).ToList();
                totalPairs.RemoveRange(0, NumberOfRecordsPerTransaction);
            }
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions)]
        public void InsertRandomOneTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var trie = tx.CreateTrie(TrieNameSlice);

                for (var i = 0; i < NumberOfTransactions; i++)
                {
                    foreach (var pair in _pairs[i])
                    {
                        trie.Add(pair.Item1, pair.Item2);
                    }
                }

                tx.Commit();
            }
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions)]
        public void InsertRandomMultipleTransactions()
        {
            for (var i = 0; i < NumberOfTransactions; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var trie = tx.CreateTrie(TrieNameSlice);

                    foreach (var pair in _pairs[i])
                    {
                        trie.Add(pair.Item1, pair.Item2);
                    }

                    tx.Commit();
                }
            }
        }
    }
}