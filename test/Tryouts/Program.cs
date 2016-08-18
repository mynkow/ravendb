using FastTests.Voron.Cedar;
using System;
using System.Diagnostics;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            var b = new Basic();
            b.AfterSequentialPageSplitAllDataIsValid();

            //var p = new RandomizedCedarTests();
            //foreach( var param in RandomizedCedarTests.InsertionParams)
            //{
            //    p.InsertionsAndRemovalsSingleTransaction((RandomizedCedarTests.InsertionOptions)param[0]);
            //}

            //for ( int i = 0; i < 1000; i++ )
            //{
            //    var o = new RandomizedCedarTests.InsertionOptions(100, i);
            //    p.InsertionsAndRemovalsSingleTransaction(o);
            //}

            //for (int i = 0; i < 1000; i++)
            //{
            //    Console.WriteLine(i);
            //    var sp = Stopwatch.StartNew();
            //    using (var x = new FastTests.Voron.Compaction.StorageCompactionTests())
            //    {
            //        x.ShouldDeleteCurrentJournalEvenThoughItHasAvailableSpace();
            //    }
            //    Console.WriteLine(sp.Elapsed);
            //}
        }
    }

}

