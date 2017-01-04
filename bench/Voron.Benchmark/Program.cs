//using BenchmarkDotNet.Running;
using Voron.Benchmark.Cedar;

namespace Voron.Benchmark
{
    public class Program
    {
        public static void Main()
        {
            //BenchmarkRunner.Run<BTree.BTreeFillRandom>();
            //BenchmarkRunner.Run<BTree.BTreeFillSequential>();
            //BenchmarkRunner.Run<BTree.BTreeReadAndIterate>();
            //BenchmarkRunner.Run<BTree.BTreeInsertRandom>();
            //BenchmarkRunner.Run<Table.TableFillSequential>();
            //BenchmarkRunner.Run<Table.TableFillRandom>();
            //BenchmarkRunner.Run<Table.TableReadAndIterate>();
            //BenchmarkRunner.Run<Table.TableInsertRandom>();
            //BenchmarkRunner.Run<Cedar.CedarFillRandom>();
            //BenchmarkRunner.Run<Cedar.CedarFillSequential>();
            //BenchmarkRunner.Run<Cedar.CedarInsertRandom>();
            //BenchmarkRunner.Run<Cedar.CedarReadAndIterate>();

            var p = new CedarFillRandom();
            p.UseAscii = 3;
            p.Setup();
            p.FillRandomMultipleTransactions();
        }
    }
}
