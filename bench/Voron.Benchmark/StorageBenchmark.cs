using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Columns;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;

namespace Voron.Benchmark
{
    [Config(typeof(Config))]
    public class StorageBenchmark
    {
        protected StorageEnvironment Env;

        public virtual bool DeleteBeforeSuite { get; protected set; } = true;
        public virtual bool DeleteAfterSuite { get; protected set; } = true;
        public virtual bool DeleteBeforeEachBenchmark { get; protected set; } = true;

        /// <summary>
        /// Path to store the benchmark database into.
        /// </summary>
        public const string Path = Configuration.Path;

        /// <summary>
        /// Number of Transactions to use per test. The default uses the global
        /// configuration, but this may be changed on a per-test basis by doing
        /// the same as below.
        /// </summary>
        [Params(Configuration.Transactions)]
        public int NumberOfTransactions { get; set; } = Configuration.Transactions;

        /// <summary>
        /// Number of Records per Transaction to use per test. The default uses
        /// the global configuration, but this may be changed on a per-test
        /// basis by doing the same as below.
        /// </summary>
        [Params(Configuration.RecordsPerTransaction)]
        public int NumberOfRecordsPerTransaction { get; set; } = Configuration.RecordsPerTransaction;

        /// <summary>
        /// Length of the keys (in bytes) to be inserted. This length is used
        /// both on initialization and testing.
        /// </summary>
        [Params(100)]
        public int KeyLength { get; set; } = 100;

        /// <summary>
        /// Random seed used to generate values. If -1, uses time for seeding.
        /// TODO: See https://github.com/PerfDotNet/BenchmarkDotNet/issues/271. Fix RandomSeed.
        /// 
        /// </summary>
        [Params(-1)]
        public int ActualRandomSeed { get; set; } = -1;

        public int? RandomSeed
        {
            get { return ActualRandomSeed; }
            set { ActualRandomSeed = value ?? -1; }
        }

        /// <summary>
        /// This is the job configuration for storage benchmarks. Changing this
        /// will affect all benchmarks done.
        /// </summary>
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job
                {
                    Env =
                    {
                        Runtime = Runtime.Core,
                        Platform = BenchmarkDotNet.Environments.Platform.X64,
                        Jit = Jit.RyuJit
                    },
                    Run =
                    {
                        LaunchCount = 1,
                        WarmupCount = 1,
                        TargetCount = 1,
                        InvocationCount = 1,
                        UnrollFactor = 1
                    },
                    // TODO: Next line is just for testing. Fine tune parameters.
                });

                // Exporters for data
                Add(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                Add(RPlotExporter.Default);

                Add(StatisticColumn.AllStatistics);

                Add(BaselineValidator.FailOnError);
                Add(JitOptimizationsValidator.FailOnError);
                // TODO: Uncomment next line. See https://github.com/PerfDotNet/BenchmarkDotNet/issues/272
                //Add(ExecutionValidator.FailOnError);
                Add(EnvironmentAnalyser.Default);
            }
        }

        public StorageBenchmark(bool deleteBeforeSuite = true, bool deleteAfterSuite = true, bool deleteBeforeEachBenchmark = true)
        {
            DeleteBeforeSuite = deleteBeforeSuite;
            DeleteBeforeEachBenchmark = deleteBeforeEachBenchmark;
            DeleteAfterSuite = deleteAfterSuite;

            if (DeleteBeforeSuite)
            {
                DeleteStorage();
            }

            if (!DeleteBeforeEachBenchmark)
            {
                var options = StorageEnvironmentOptions.ForPath(Path);
                options.ManualFlushing = true;
                Env = new StorageEnvironment(options);
            }
        }

        ~StorageBenchmark()
        {
            if (!DeleteBeforeEachBenchmark)
            {
                Env.Dispose();
            }

            if (DeleteAfterSuite)
            {
                DeleteStorage();
            }
        }

        [Setup]
        public virtual void Setup()
        {
            if (DeleteBeforeEachBenchmark)
            {
                DeleteStorage();

                var options = StorageEnvironmentOptions.ForPath(Path);
                options.ManualFlushing = true;
                Env = new StorageEnvironment(options);
            }
        }

        [Cleanup]
        public virtual void Cleanup()
        {
            if (DeleteBeforeEachBenchmark)
            {
                Env.Dispose();
            }
        }

        private void DeleteStorage()
        {
            if (!Directory.Exists(Path)) return;

            for (var i = 0; i < 10; ++i)
            {
                try
                {
                    Directory.Delete(Path, true);
                    break;
                }
                catch (DirectoryNotFoundException)
                {
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(20);
                }
            }
        }
    }
}