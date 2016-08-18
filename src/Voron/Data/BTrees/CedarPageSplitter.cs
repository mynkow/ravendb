using System;
using System.Diagnostics;
using Voron.Impl;
using Voron.Impl.Paging;

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

        public void Execute()
        {
            var page = _cursor.CurrentPage;

            CedarPage parentPage;
            if (_cursor.PageDepth == 1) // we are splitting the root
            {
                _cursor.Pop();

                CedarPage newRootPage = CedarPage.Allocate(_tx, _tree.Layout, TreePageFlags.Branch);                

                _tree.State.PageCount++;
                _tree.State.BranchPages++;
                _tree.State.RootPageNumber = newRootPage.PageNumber;
                _tree.State.Depth++;

                // now add implicit left page
                newRootPage.AddBranchRef(Slices.BeforeAllKeys, page.PageNumber);
                parentPage = newRootPage;

                // Setup the cursor. 
                _cursor.Push(newRootPage);
                _cursor.Push(page);
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
                CedarKeyPair keyPair;
                CedarDataNode* dataPtr;
                CedarResultCode errorCode = page.GetLast(out keyPair, out dataPtr);
                Debug.Assert(errorCode == CedarResultCode.Success, "Calling GetLast on an empty tree cannot happen on split.");

                // The insert key is bigger than the last key
                if (SliceComparer.Compare(_keyToInsert, keyPair.Key) >= 0)
                {
                    // when we get a split at the end of a leaf page, we take that as a hint that the user is doing 
                    // sequential inserts, at that point, we are going to keep the current page as is and create a new 
                    // page, this will allow us to do minimal amount of work to get the best density.
                    var rightPage = CedarPage.Allocate(_tx, page.Layout, TreePageFlags.Leaf);                

                    if (parentPage.AddBranchRef(keyPair.Key, rightPage.PageNumber) == CedarActionStatus.NotEnoughSpace )
                    {
                        _cursor.Pop(); // We pop the current page because we are going to split the parent. 

                        // We recursively split the parent if necessary. 
                        var splitter = new CedarPageSplitter(_tx, _tree, _cursor, keyPair.Key);
                        splitter.Execute();

                        // The parent page may have changed depending on which side the key would end up be stored. 
                        parentPage = _cursor.CurrentPage;
                        if ( parentPage.AddBranchRef(keyPair.Key, rightPage.PageNumber) != CedarActionStatus.Success )
                            throw new InvalidOperationException("This is an splitting bug. It cannot happen that a branch page will not have space after an split.");

                        // We restore the cursor to the current page to continue with the insert process.
                        _cursor.Push(page);                                                
                    }

                    // We promote the last key of the left page into the next page. 
                    CedarDataNode* rightData;
                    rightPage.Update(keyPair.Key, dataPtr->DataSize, out rightData);
                    *rightData = *dataPtr; // Copy the data

                    // Update the cursor to point to the right page and select the inserted key.

                    _cursor.Pop(); // We pop the left page.
                    _cursor.Push(rightPage);

                    // Remove the promoted key from the leaf page.
                    ushort _;
                    page.Remove(keyPair.Key, out _);

                    _tree.State.PageCount++;
                    _tree.State.LeafPages++;

                    return;
                }
            }

            // Either we are splitting a branch page or we are trying to split a page with a random insertion (not in order)
            // Therefore the usual split in half method will be used. In the case of a branch pages, given the cardinality achieved
            // in the tree, not doing the half splitting would be a disservice to the whole tree structure.
            SplitPageInHalf(page, parentPage);
        }

        private void SplitPageInHalf(CedarPage leftPage, CedarPage parentPage)
        {
            int entries = leftPage.Header.Ptr->NumberOfEntries;

            // We remove the current page from the cursor.
            _cursor.Pop();
           
            Slice key;
            CedarPage temp;
            using (Slice.Create(_tx.Allocator, AbstractPager.MaxKeySize, out key)) // We allocate in memory the biggest key we can handle.
            using (CedarPage.CloneInMemory(_tx, leftPage, out temp)) // We allocate a copy of the current page we are going to use.
            {
                long p = 0;
                long root = 0;
                long from = root;

                CedarPage.IteratorValue b;

                int num = 0;
                for (b = temp.Begin(key, ref from, ref p); num <= entries / 2; num++)
                {
                    Debug.Assert(b.Error == CedarResultCode.Success);

                    key.Reset();
                    b = temp.Next(key, ref from, ref p, root);
                }

                key.Reset();
                b = temp.Next(key, ref from, ref p, root);
                key.SetSize(b.Length);
                Debug.Assert(b.Error == CedarResultCode.Success);

                Slice firstRightKey = key.Clone(_tx.Allocator);

                // Allocate the new page.
                var rightPage = CedarPage.Allocate(_tx, leftPage.Layout, leftPage.Header.Ptr->TreeFlags);

                if (leftPage.Header.Ptr->IsBranchPage)
                    _tree.State.BranchPages++;
                else
                    _tree.State.LeafPages++;

                do
                {
                    // Addition in the right page.
                    CedarDataNode* ptr = temp.Data.DirectRead(b.Value);
                    Debug.Assert(ptr != null);

                    CedarDataNode* newPtr;
                    CedarActionStatus status = rightPage.Update(key, ptr->DataSize, out newPtr);
                    *newPtr = *ptr; // Copy the structure

                    if (status == CedarActionStatus.NotEnoughSpace)
                        throw new InvalidOperationException("This is an splitting bug. It cannot happen that a splitting page will not have space after an split.");

                    // Remove in the left page.
                    ushort _;
                    status = leftPage.Remove(key, out _);

                    if (status != CedarActionStatus.Success)
                        throw new InvalidOperationException("This is an splitting bug. It cannot happen that a splitting page will not succeed when deleting an already found key.");

                    key.Reset();
                    b = temp.Next(key, ref from, ref p, root);
                    key.SetSize(b.Length); // We shrink the key.      
                }
                while (b.Error == CedarResultCode.Success);

                leftPage.ShrinkTail(); // Compress the tail data. 

                if (parentPage.AddBranchRef(firstRightKey, rightPage.PageNumber) == CedarActionStatus.NotEnoughSpace)
                {
                    // We recursively split the parent if necessary (we are already pointing into it)
                    var splitter = new CedarPageSplitter(_tx, _tree, _cursor, firstRightKey);
                    splitter.Execute();

                    // The parent page may have changed depending on which side the key would end up be stored. 
                    parentPage = _cursor.CurrentPage;
                    if (parentPage.AddBranchRef(firstRightKey, rightPage.PageNumber) != CedarActionStatus.Success)
                        throw new InvalidOperationException("This is an splitting bug. It cannot happen that a branch page will not have space after an split.");
                }

                if (SliceComparer.Compare(_keyToInsert, firstRightKey) < 0)
                    _cursor.Push(leftPage);
                else
                    _cursor.Push(rightPage);

                _tree.State.PageCount++;
            }
        }
    }
}
