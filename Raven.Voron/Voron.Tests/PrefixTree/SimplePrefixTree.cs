using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Data.Compact;
using Xunit;

namespace Voron.Data.Compact.Tests
{
    public class SimplePrefixTree : PrefixTreeStorageTests
    {
        private string Name = "MyTree";

        [Fact]
        public void Construction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.PrefixTreeFor(Name);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.PrefixTreeFor(Name);

                Assert.Equal(0, tree.Count);
                Assert.Null(tree.FirstKeyOrDefault());
                Assert.Null(tree.LastKeyOrDefault());

                StructuralVerify(tree);
            }
        }
    }
}
