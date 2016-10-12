using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using Micro.Benchmark.Tests;

namespace Micro.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var p = new PageLocatorBenchmark();
            p.CacheSize = 32;
            p.RandomSeed = 5;
            p.Setup();
            p.Basic_PageLocatorV4();

            //var t = new PageLocatorTests();
            //t.TestGetReadonly(8);

            BenchmarkRunner.Run<PageLocatorBenchmark>();
        }
    }
}
