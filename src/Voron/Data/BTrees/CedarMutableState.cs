using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public unsafe class CedarMutableState
    {
        private readonly LowLevelTransaction _tx;
        private bool _isModified;

        public long RootPageNumber;

        public long PageCount;
        public long BranchPages;
        public long LeafPages;
        public long OverflowPages;

        public int Depth;

        public long NumberOfEntries;
        public TreeFlags Flags;

        public bool InWriteTransaction;

        public CedarMutableState(LowLevelTransaction tx)
        {
            _tx = tx;
        }

        public bool IsModified
        {
            get { return _isModified; }
            set
            {
                if (InWriteTransaction == false)
                    throw new InvalidOperationException("Invalid operation outside of a write transaction");
                _isModified = value;
            }
        }

        public void CopyTo(CedarRootHeader* header)
        {
            header->RootObjectType = RootObjectType.CedarTree;
            header->Flags = Flags;
            header->BranchPages = BranchPages;
            header->Depth = Depth;
            header->LeafPages = LeafPages;
            header->OverflowPages = OverflowPages;
            header->PageCount = PageCount;
            header->NumberOfEntries = NumberOfEntries;
            header->RootPageNumber = RootPageNumber;
        }

        public CedarMutableState Clone()
        {
            return new CedarMutableState(_tx)
            {
                BranchPages = BranchPages,
                Depth = Depth,
                NumberOfEntries = NumberOfEntries,
                LeafPages = LeafPages,
                OverflowPages = OverflowPages,
                PageCount = PageCount,
                Flags = Flags,
                RootPageNumber = RootPageNumber,
            };
        }

        public void RecordNewPage(CedarPage p, int num)
        {
            PageCount += num;

            if (p.IsBranch)
            {
                BranchPages++;
            }
            else if (p.IsLeaf)
            {
                LeafPages++;
            }
        }

        public void RecordFreedPage(CedarPage p, int num)
        {
            PageCount -= num;
            Debug.Assert(PageCount >= 0);

            if (p.IsBranch)
            {
                BranchPages--;
                Debug.Assert(BranchPages >= 0);
            }
            else if (p.IsLeaf)
            {
                LeafPages--;
                Debug.Assert(LeafPages >= 0);
            }
        }

        public override string ToString()
        {
            return string.Format(@" Pages: {1:#,#}, Entries: {2:#,#}
    Depth: {0}, TreeFlags: {3}
    Root Page: {4}
    Leafs: {5:#,#} Overflow: {6:#,#} Branches: {7:#,#}
    Size: {8:F2} Mb", Depth, PageCount, NumberOfEntries, Flags, RootPageNumber, LeafPages, OverflowPages, BranchPages, ((float)(PageCount * Constants.Storage.PageSize) / (1024 * 1024)));
        }
    }
}
