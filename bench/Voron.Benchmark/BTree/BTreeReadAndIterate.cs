using System.Collections.Generic;
using System.Threading;
//using BenchmarkDotNet.Attributes;
using Sparrow;

namespace Voron.Benchmark.BTree
{
    public class BTreeReadAndIterate : StorageBenchmark
    {
        private static readonly Slice TreeNameSlice;

        private readonly Dictionary<int, List<Slice>> _keysPerThread = new Dictionary<int, List<Slice>>();
        private readonly Dictionary<int, List<Slice>> _sortedKeysPerThread = new Dictionary<int, List<Slice>>();

        /// <summary>
        /// Size of tree to create in order to read from (in number of nodes).
        /// This is the TOTAL SIZE after deletions
        /// </summary>
        //[Params(Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public int GenerationTreeSize { get; set; } = Configuration.RecordsPerTransaction * Configuration.Transactions / 2;

        /// <summary>
        /// Size of batches to divide the insertion into. A lower number will
        /// generate more frequent deletions.
        /// 
        /// Beware of low batch sizes. Even though they generate a good wearing
        /// in the tree, too low of a number here may take a long time to
        /// converge.
        /// </summary>
        //[Params(50000)]
        public int GenerationBatchSize { get; set; } = 50000;

        /// <summary>
        /// Probability that a node will be deleted after insertion.
        /// </summary>
        //[Params(0.1)]
        public double GenerationDeletionProbability { get; set; } = 0.1;

        //[Params(1, 2)]
        public int ReadParallelism { get; set; } = 1;

        //[Params(100)]
        public int ReadBufferSize { get; set; } = 100;

        static BTreeReadAndIterate()
        {
            Slice.From(Configuration.Allocator, "BTreeReadAndIterate", ByteStringType.Immutable, out TreeNameSlice);
        }

        /// <summary>
        /// Ensure we don't have to re-create the BTree between benchmarks
        /// </summary>
        public BTreeReadAndIterate() : base(true, true, false)
        {

        }

        //[Setup]
        public override void Setup()
        {
            base.Setup();

            var treeKeys = Utils.GenerateWornoutTree(
                Env,
                TreeNameSlice,
                GenerationTreeSize,
                GenerationBatchSize,
                KeyLength,
                GenerationDeletionProbability,
                RandomSeed);

            // Distribute work amount, each one of the buckets is sorted
            for (var i = 0; i < ReadParallelism; i++)
            {
                _keysPerThread[i] = new List<Slice>();
                _sortedKeysPerThread[i] = new List<Slice>();
            }

            int treeKeyIndex = 0;

            foreach (var key in treeKeys)
            {
                _keysPerThread[treeKeyIndex % ReadParallelism].Add(key);
                _sortedKeysPerThread[treeKeyIndex % ReadParallelism].Add(key);
                treeKeyIndex++;
            }

            // TODO: parallell maybe?
            for (var i = 0; i < ReadParallelism; i++)
            {
                _sortedKeysPerThread[i].Sort(SliceComparer.Instance);
            }
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        //[Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void ReadRandomOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;
                    var buffer = new byte[ReadBufferSize];

                    using (var tx = Env.ReadTransaction())
                    {
                        var tree = tx.ReadTree(TreeNameSlice);

                        foreach (var key in _keysPerThread[currentThreadIndex])
                        {
                            var reader = tree.Read(key).Reader;

                            while (reader.Read(buffer, 0, buffer.Length) != 0) { }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        //[Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void ReadSeqOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;
                    var buffer = new byte[ReadBufferSize];

                    using (var tx = Env.ReadTransaction())
                    {
                        var tree = tx.ReadTree(TreeNameSlice);

                        foreach (var key in _sortedKeysPerThread[currentThreadIndex])
                        {
                            var reader = tree.Read(key).Reader;

                            while (reader.Read(buffer, 0, buffer.Length) != 0) { }
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
        //[Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction*Configuration.Transactions / 2)]
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
                        var tree = tx.ReadTree(TreeNameSlice);

                        using (var it = tree.Iterate(false))
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
        //[Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
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
                        var tree = tx.ReadTree(TreeNameSlice);

                        using (var it = tree.Iterate(false))
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