using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Regression.PageLocator;

namespace Micro.Benchmark
{
    [Config(typeof(Config))]
    public class PageLocatorBenchmark
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job
                {
                    Runtime = Runtime.Core,
                    Platform = BenchmarkDotNet.Jobs.Platform.X64,
                    Jit = Jit.RyuJit,
                    // TODO: Next line is just for testing. Fine tune parameters.
                    Mode = Mode.SingleRun,
                    LaunchCount = 1,
                    WarmupCount = 1,
                    TargetCount = 1,
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

        private const int NumberOfOperations = 10000;

        [Params(1, 2, 4, 8, 16, 32, 64, 128)]
        public int CacheSize { get; set; }

        [Params(5)]
        public int RandomSeed { get; set; }

        private List<long> _pageNumbers = new List<long>();
        private PageLocatorV3 _cache;

        [Setup]
        public void Setup()
        {
            _cache = new PageLocatorV3(null, CacheSize);
            var generator = new Random(RandomSeed);

            for (int i = 0; i < NumberOfOperations; i++)
            {
                long valueBuffer = generator.Next();
                valueBuffer += (long)generator.Next() << 32;
                valueBuffer += (long)generator.Next() << 64;
                valueBuffer += (long)generator.Next() << 96;

                _pageNumbers.Add(valueBuffer);
            }
        }

        [Benchmark(OperationsPerInvoke = NumberOfOperations)]
        void BasicReadOnlyBenchmark()
        {
            foreach (var pageNumber in _pageNumbers)
            {
                _cache.GetReadOnlyPage(pageNumber);
            }
        }
    }
}