using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public class CedarPageSplitter
    {
        private LowLevelTransaction _llt;

        private CedarTree _tree;
        private CedarCursor _cursor;
        

        public CedarPageSplitter(LowLevelTransaction llt, CedarTree tree, CedarCursor cursor)
        {
            this._llt = llt;
            this._tree = tree;
            this._cursor = cursor;
        }

        public CedarCursor Execute()
        {
            throw new NotImplementedException();
        }
    }
}
