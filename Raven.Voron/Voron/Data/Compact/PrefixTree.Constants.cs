using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    partial class PrefixTree
    {
        public unsafe static class Constants
        {
            public const long InvalidNode = -1;
            public const int L1CacheSize = 16 * 1024;

            public static int NodesPerPage = 4096 * 1024 / sizeof(PrefixTree.Node);
            public static int NodesPerCache = L1CacheSize / sizeof(PrefixTree.Node);
        }
    }
}
