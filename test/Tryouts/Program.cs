using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using SlowTests.Client.Attachments;
using SlowTests.Client.Counters;
//using SlowTests.Tests.Sorting;
//using Sparrow;
//using Sparrow.Json;
//using Sparrow.Json.Parsing;
using Xunit;

namespace Tryouts
{
    public static class Program
    {
        /*
        public static async Task Main(string[] args)
        {
        }
        */

        public static void Main(string[] args)
        {
            //Span<byte> by = new Span<byte>();

            //var allocator = Allocator.Create(
            //    Allocators.Pool
            //        .WithConfig(Allocators.Pools.Small)
            //);

            //allocator = Allocator.Create(
            //    Allocators.Pool
            //        .WithConfig<CustomPoolOptions>()
            //);

            //allocator = new Allocator.Create(
            //    Allocators.Arena
            //        .WithConfig<CustomArenaOptions>()
            //        .NonDisposable());

            //allocator = new Allocator.Create(
            //    Allocators.Arena
            //        .WithConfig<CustomArenaOptions>()
            //        .Renewable(owner));
        }
    }
}
