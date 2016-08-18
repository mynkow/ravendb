using System.Collections.Generic;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Sparrow;

namespace Voron.Benchmark.Cedar
{
    public class CedarReadAndIterate : StorageBenchmark
    {
        private static readonly Slice TrieNameSlice;
        private readonly Dictionary<int, List<Slice>> _keysPerThread = new Dictionary<int, List<Slice>>();
        private readonly Dictionary<int, List<Slice>> _sortedKeysPerThread = new Dictionary<int, List<Slice>>();

        /// <summary>
        /// Size of tree to create in order to read from (in number of nodes).
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

        [Params(1, 2)]
        public int ReadParallelism { get; set; }

        [Params(100)]
        public int ReadBufferSize { get; set; }

        /// <summary>
        /// First component of the array tells if we should use ASCII. The
        /// second one is the amount of clustering in the event that we do
        /// use it.
        /// </summary>
        [Params(0, 3)]
        public long UseAscii { get; set; }
        public bool ShouldUseAscii => (UseAscii & 0x1) > 0;
        public int AsciiClusterSize => (int)((UseAscii & ((long)0xFFFFFFFF << 1)) >> 1);

        static CedarReadAndIterate()
        {
            Slice.From(Configuration.Allocator, "CedarReadAndIterate", ByteStringType.Immutable, out TrieNameSlice);
        }

        /// <summary>
        /// Ensure we don't have to re-create the Trie between benchmarks
        /// </summary>
        public CedarReadAndIterate() : base(true, true, false)
        {

        }

        [Setup]
        public override void Setup()
        {
            base.Setup();

            var trieKeys = Utils.GenerateWornoutTrie(
                Env,
                TrieNameSlice,
                GenerationTrieSize,
                GenerationBatchSize,
                KeyLength,
                GenerationDeletionProbability,
                RandomSeed,
                ShouldUseAscii,
                AsciiClusterSize);

            // Distribute work amount, each one of the buckets is sorted
            for (var i = 0; i < ReadParallelism; i++)
            {
                _keysPerThread[i] = new List<Slice>();
                _sortedKeysPerThread[i] = new List<Slice>();
            }

            int trieKeyIndex = 0;

            foreach (var key in trieKeys)
            {
                _keysPerThread[trieKeyIndex % ReadParallelism].Add(key);
                _sortedKeysPerThread[trieKeyIndex % ReadParallelism].Add(key);
                trieKeyIndex++;
            }

            // TODO: parallell maybe?
            for (var i = 0; i < ReadParallelism; i++)
            {
                _sortedKeysPerThread[i].Sort(SliceComparer.Instance);
            }
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void ReadRandomOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;

                    using (var tx = Env.ReadTransaction())
                    {
                        var trie = tx.ReadTrie(TrieNameSlice);

                        foreach (var key in _keysPerThread[currentThreadIndex])
                        {
                            trie.Read(key);
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void ReadSeqOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;

                    using (var tx = Env.ReadTransaction())
                    {
                        var trie = tx.ReadTrie(TrieNameSlice);

                        foreach (var key in _sortedKeysPerThread[currentThreadIndex])
                        {
                            trie.Read(key);
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        // TODO: this is specially bad in this case, since the operations are actually *Parallelism
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void IterateAllKeysOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    int localSizeCount = 0;

                    using (var tx = Env.ReadTransaction())
                    {
                        var trie = tx.ReadTrie(TrieNameSlice);

                        using (var it = trie.Iterate(false))
                        {
                            if (it.Seek(Slices.BeforeAllKeys))
                            {
                                do
                                {
                                    localSizeCount += it.CurrentKey.Size;
                                } while (it.MoveNext());
                            }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void IterateThreadKeysOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;
                    int localSizeCount = 0;

                    using (var tx = Env.ReadTransaction())
                    {
                        var trie = tx.ReadTrie(TrieNameSlice);

                        using (var it = trie.Iterate(false))
                        {
                            if (it.Seek(_sortedKeysPerThread[currentThreadIndex][0]))
                            {
                                do
                                {
                                    localSizeCount += it.CurrentKey.Size;
                                } while (it.MoveNext());
                            }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }
    }
}