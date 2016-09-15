using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.FreeSpace;

namespace Voron.Data.BTrees
{
    public unsafe class CedarPageSplitter
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

            CedarPage parentPage;
            if (_cursor.PageDepth == 0) // we are splitting the root
            {
                CedarPage newRootPage = CedarPage.Allocate(_tx, _tree.Layout, TreePageFlags.Branch);
                _cursor.Push(newRootPage);
                _tree.State.RootPageNumber = newRootPage.PageNumber;
                _tree.State.Depth++;

                // now add implicit left page
                newRootPage.AddBranchRef(Slices.BeforeAllKeys, page.PageNumber);
                parentPage = newRootPage;
            }
            else
            {
                parentPage = _cursor.ParentPage;                
            }

            // We invalidate all the recent found pages. 
            if (page.IsLeaf)
            {
                _tree.ClearRecentFoundPages();

                CedarKeyPair keyPair;
                CedarDataPtr* dataPtr;
                CedarResultCode errorCode = page.GetLast(out keyPair, out dataPtr);
                Debug.Assert(errorCode == CedarResultCode.Success, "Calling GetLast on an empty tree cannot happen on split.");

                if (SliceComparer.Equals(keyPair.Key, _keyToInsert))
                {
                    // when we get a split at the end of a leaf page, we take that as a hint that the user is doing 
                    // sequential inserts, at that point, we are going to keep the current page as is and create a new 
                    // page, this will allow us to do minimal amount of work to get the best density.

                    throw new NotImplementedException();

                    // Remove the last key we promoted to the leaf page.
                    //page.Remove(keyPair.Key);

                    //return _cursor;
                }
            }

            // Either we are splitting a branch page or we are trying to split a page with a random insertion (not in order)
            // Therefore the usual split in half method will be used.

            throw new NotImplementedException();
        }


    }
}
