﻿using System;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Tests;
using SlowTests.Voron;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine($"{i} started");
                using (var a = new SlowTests.MailingList.LastModifiedMetadataTest())
                {
                    a.Can_index_and_query_metadata2();
                }
                Console.WriteLine($"{i} finished");
            }

        }
    }
}

