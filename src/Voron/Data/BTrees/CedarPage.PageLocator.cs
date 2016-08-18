using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Sparrow;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public partial class CedarPage
    {

        internal unsafe struct FixedPageLocator
        {
            private const long Invalid = -1;

            [StructLayout(LayoutKind.Explicit, Size = 32)]
            private struct PageData
            {
                [FieldOffset(0)]
                public long PageNumber;
                [FieldOffset(8)]
                public Page Page;
                [FieldOffset(16)]
                public byte* DataPointer;
                [FieldOffset(24)]
                public bool IsWritable;           
            }

            private readonly LowLevelTransaction _llt;
            private readonly ByteString _cacheMemory;
            private readonly PageData* _table;
            private readonly long _pageOffset;

            public FixedPageLocator(LowLevelTransaction llt, long pageOffset)
            {
                this._llt = llt;
                this._pageOffset = pageOffset;

                this._llt.Allocator.Allocate(CedarRootHeader.TotalNumberOfPages * sizeof(PageData), out _cacheMemory);
                _table = (PageData*)_cacheMemory.Ptr;

                for (int i = 0; i < CedarRootHeader.TotalNumberOfPages; i++)
                    _table[i].PageNumber = Invalid;
            }

            public void AddWritable(Page page)
            {
                long pageNumber = page.PageNumber;
                Debug.Assert(pageNumber >= _pageOffset);

                long position = pageNumber - _pageOffset;
                Debug.Assert(position < CedarRootHeader.TotalNumberOfPages);

                PageData* n = &_table[position];
                n->PageNumber = pageNumber;
                n->Page = page;
                n->DataPointer = n->Page.DataPointer;
                n->IsWritable = true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Page GetReadOnlyPage(long pageNumber)
            {
                Debug.Assert(pageNumber >= _pageOffset);

                long position = pageNumber - _pageOffset;
                Debug.Assert(position < CedarRootHeader.TotalNumberOfPages);

                PageData* n = &_table[position];
                if (n->PageNumber != Invalid)
                {
                    return n->Page;
                }
                else
                {
                    n->PageNumber = pageNumber;
                    n->Page = _llt.GetPage(pageNumber);
                    n->DataPointer = n->Page.DataPointer;
                    n->IsWritable = false;

                    return n->Page;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Page GetWritablePage(long pageNumber)
            {
                Debug.Assert(pageNumber >= _pageOffset);

                long position = pageNumber - _pageOffset;
                Debug.Assert(position < CedarRootHeader.TotalNumberOfPages);

                PageData* n = &_table[position];
                if (n->PageNumber != Invalid && n->IsWritable)
                {
                    return n->Page;
                }
                else
                {
                    n->PageNumber = pageNumber;
                    n->Page = _llt.ModifyPage(pageNumber);
                    n->DataPointer = n->Page.DataPointer;
                    n->IsWritable = true;

                    return n->Page;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public byte* GetReadOnlyDataPointer(long pageNumber)
            {
                Debug.Assert(pageNumber >= _pageOffset);

                long position = pageNumber - _pageOffset;
                Debug.Assert(position < CedarRootHeader.TotalNumberOfPages);

                PageData* n = &_table[position];
                if (n->PageNumber != Invalid)
                {
                    return n->DataPointer;
                }
                else
                {
                    n->PageNumber = pageNumber;
                    n->Page = _llt.GetPage(pageNumber);
                    n->DataPointer = n->Page.DataPointer;
                    n->IsWritable = false;

                    return n->DataPointer;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public byte* GetWritableDataPointer(long pageNumber)
            {
                Debug.Assert(pageNumber >= _pageOffset);

                long position = pageNumber - _pageOffset;
                Debug.Assert(position < CedarRootHeader.TotalNumberOfPages);

                PageData* n = &_table[position];
                if (n->PageNumber != Invalid && n->IsWritable)
                {
                    return n->DataPointer;
                }
                else
                {
                    n->PageNumber = pageNumber;
                    n->Page = _llt.ModifyPage(pageNumber);
                    n->DataPointer = n->Page.DataPointer;
                    n->IsWritable = true;

                    return n->DataPointer;
                }
            }
        }
    }
}
