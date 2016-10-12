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
                    Platform = Platform.X64,
                    Jit = Jit.RyuJit,
                    // TODO: Next line is just for testing. Fine tune parameters.
                    Mode = Mode.SingleRun,
                    LaunchCount = 1,
                    WarmupCount = 1,
                    TargetCount = 3,
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

        [Params(8, 16, 32, 64, 128)]
        //[Params(4, 8, 16, 32)]
        public int CacheSize { get; set; }

        [Params(5)]
        public int RandomSeed { get; set; }

        private List<long> _pageNumbers;

        private PageLocatorV1 _cacheV1;
        private PageLocatorV2 _cacheV2;
        private PageLocatorV3 _cacheV3;
        private PageLocatorV4 _cacheV4;

        [Setup]
        public void Setup()
        {
            _cacheV1 = new PageLocatorV1(null, CacheSize);
            _cacheV2 = new PageLocatorV2(null, CacheSize);
            _cacheV3 = new PageLocatorV3(null, CacheSize);
            _cacheV4 = new PageLocatorV4(null, CacheSize);

            var generator = new Random(RandomSeed);

            _pageNumbers = new List<long>();
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
        public void Basic_PageLocatorV1()
        {
            foreach (var pageNumber in _pageNumbers)
            {
                _cacheV1.GetReadOnlyPage(pageNumber);
            }
        }

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void Basic_PageLocatorV2()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV2.GetReadOnlyPage(pageNumber);
        //    }
        //}

        [Benchmark(OperationsPerInvoke = NumberOfOperations)]
        public void Basic_PageLocatorV3()
        {
            foreach (var pageNumber in _pageNumbers)
            {
                _cacheV3.GetReadOnlyPage(pageNumber);
            }
        }

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void Basic_PageLocatorV4()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV4.GetReadOnlyPage(pageNumber);
        //    }
        //}
    }
}