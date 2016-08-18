using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using BenchmarkDotNet.Attributes;
using Sparrow;

namespace Voron.Benchmark.Cedar
{
    public class CedarFillRandom : StorageBenchmark
    {
        private static readonly Slice TrieNameSlice;

        /// <summary>
        /// We have one list per Transaction to carry out. Each one of these 
        /// lists has exactly the number of items we want to insert, with
        /// distinct keys for each one of them.
        /// 
        /// It is important for them to be lists, this way we can ensure the
        /// order of insertions remains the same throughout runs.
        /// </summary>
        private List<Tuple<Slice, long>>[] _pairs;

        /// <summary>
        /// First component of the array tells if we should use ASCII. The
        /// second one is the amount of clustering in the event that we do
        /// use it.
        /// </summary>
        [Params(0, 3)]
        public long UseAscii { get; set; }
        public bool ShouldUseAscii => (UseAscii & 0x1) > 0;
        public int AsciiClusterSize => (int)((UseAscii & ((long)0xFFFFFFFF << 1)) >> 1);

        static CedarFillRandom()
        {
            Slice.From(Configuration.Allocator, "CedarFillRandom", ByteStringType.Immutable, out TrieNameSlice);
        }

        [Setup]
        public override void Setup()
        {
            base.Setup();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTrie(TrieNameSlice);
                tx.Commit();
            }

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
        public void FillRandomOneTransaction()
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
        public void FillRandomMultipleTransactions()
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