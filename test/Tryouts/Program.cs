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
            //CedarTrie<int> trie = new CedarTrie<int>();
            var p = new FastTests.Sparrow.CedarTrieTests();
            p.TestCaseGenerator(100, 60, 1002400, 45);
        }
    }
}

