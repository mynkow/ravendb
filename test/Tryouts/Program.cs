using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using FastTests;
using Sparrow.Collections;

namespace Tryouts
{

    public class Program
    {
        static void Main(string[] args)
        {
            //using (var streamwriter = new StreamWriter(new FileStream("managed.txt", FileMode.Create)))
            //{
            //    streamwriter.AutoFlush = true;
            //    Console.SetOut(streamwriter);
            //    Console.SetError(streamwriter);

            //    var p = new FastTests.Sparrow.CedarTrieTests();
            //    p.SingleInsertAndQuery();
            //}

            try
            {
                using (var streamwriter = new StreamWriter(new FileStream("voron.txt", FileMode.Create)))
                {
                    streamwriter.AutoFlush = true;
                    Console.SetOut(streamwriter);
                    Console.SetError(streamwriter);

                    var b = new FastTests.Voron.Cedar.Basic(); ;
                    b.AfterPageSplitAllDataIsValid();
                }
            }
            catch {}


            //CedarTrie<int> trie = new CedarTrie<int>();
            // p.TestCaseGenerator(100, 60, 1002400, 45);


        }
    }
}

