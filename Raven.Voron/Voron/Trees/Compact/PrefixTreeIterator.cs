using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{        
    public sealed unsafe class PrefixTreeIterator
    {
        private PrefixTree _owner;

        private PrefixTreeLeafNode* _start;
        private PrefixTreeLeafNode* _end;
        private PrefixTreeLeafNode* _current;

        internal PrefixTreeIterator(PrefixTree prefixTree, PrefixTreeLeafNode* start, PrefixTreeLeafNode* end)
        {
            this._owner = prefixTree;
            this._start = start;
            this._end = end;

            this._current = start;
        }
    }
}
