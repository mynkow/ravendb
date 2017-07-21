using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Documents;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;

namespace Tryouts
{
    public class Program : RavenTestBase
    {
        public static void Main(string[] args)
        {

            var p = new Program();
            p.Execute().Wait();
        }

        private async Task Execute()
        {
            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new {Name = "Fitzchak"});
                        await session.StoreAsync(new {Name = "Arek"});
                        await session.StoreAsync(new {Name = "Arek"});
                        await session.StoreAsync(new {Name = "Arek"});
                        await session.StoreAsync(new {Name = "Arek"});
                        await session.StoreAsync(new {Name = "Arek"});
                        await session.StoreAsync(new {Name = "Arek"});

                        await session.SaveChangesAsync();
                    }
                }
            }
        }
    }
}
