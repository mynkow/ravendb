using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.FreeSpace;

namespace Voron.Data.BTrees
{
    public class CedarPageSplitter
    {
        private readonly LowLevelTransaction _tx;
        private readonly CedarTree _tree;
        private readonly Slice _keyToInsert;

        private CedarCursor _cursor;
        

        public CedarPageSplitter(LowLevelTransaction llt, CedarTree tree, CedarCursor cursor, Slice keyToInsert)
        {
            this._tx = llt;
            this._tree = tree;
            this._cursor = cursor;
            this._keyToInsert = keyToInsert;
        }

        public CedarCursor Execute()
        {
            var page = _cursor.CurrentPage;

            if (_cursor.PageCount == 0) // we are splitting the root
            {
                
            }
            else
            {
                                
            }

            // We invalidate all the recent found pages. 
            if (page.IsLeaf)
            {
                _tree.ClearRecentFoundPages();
            }

            if (_cursor.IsLast(_keyToInsert))
            {
                // when we get a split at the end of the page, we take that as a hint that the user is doing 
                // sequential inserts, at that point, we are going to keep the current page as is and create a new 
                // page, this will allow us to do minimal amount of work to get the best density.

                page.SearchLast();

            }
            else
            {
                // Split in half. 

            }


            throw new NotImplementedException();
        }


    }
}
