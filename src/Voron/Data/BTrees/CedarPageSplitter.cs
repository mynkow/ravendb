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

            // When splitting parents are always branch nodes.
            Debug.Assert(parentPage.IsBranch);

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
                    var rightPage = CedarPage.Allocate(_tx, page.Layout, TreePageFlags.Leaf);

                    if (parentPage.AddBranchRef(keyPair.Key, rightPage.PageNumber) == CedarActionStatus.NotEnoughSpace )
                    {
                        _cursor.Pop(); // We pop the current page we are going to split. 

                        // We recursively split the parent if necessary. 
                        var splitter = new CedarPageSplitter(_tx, _tree, _cursor, keyPair.Key);
                        _cursor = splitter.Execute();

                        // The parent page may have changed depending on which side the key would end up be stored. 
                        parentPage = _cursor.CurrentPage;
                        if ( parentPage.AddBranchRef(keyPair.Key, rightPage.PageNumber) != CedarActionStatus.Success )
                            throw new InvalidOperationException("This is an splitting bug. It cannot happen that a branch page will not have space after an split.");

                        // We restore the cursor to the current page to continue with the insert process.
                        _cursor.Push(page);                                                
                    }

                    // We promote the last key of the left page into the next page. 
                    CedarDataPtr* rightData;
                    rightPage.Update(keyPair.Key, dataPtr->DataSize, out rightData);

                    // Update the cursor to point to the right page and select the inserted key.

                    _cursor.Pop(); // We pop the left page.
                    _cursor.Push(rightPage);
                    _cursor.Seek(keyPair.Key);

                    // Remove the promoted key from the leaf page.
                    page.Remove(keyPair.Key);

                    return _cursor;
                }
            }

            // Either we are splitting a branch page or we are trying to split a page with a random insertion (not in order)
            // Therefore the usual split in half method will be used.

            throw new NotImplementedException();
        }


    }
}
