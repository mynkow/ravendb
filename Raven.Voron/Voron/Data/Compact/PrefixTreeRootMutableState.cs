using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.Compact
{
    public unsafe class PrefixTreeRootMutableState
    {
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeRootHeader* _header;

        private bool _isModified;

        public PrefixTreeRootMutableState(LowLevelTransaction tx, Page page)
        {
            Debug.Assert(tx != null);
            Debug.Assert(page != null);
            Debug.Assert(page.Pointer != null);

            this._tx = tx;
            this._header = (PrefixTreeRootHeader*)(page.Pointer + sizeof(PageHeader));

            this.PageNumber = page.PageNumber;
        }

        public long PageNumber { get; private set; }

        /// <summary>
        /// The root header page for the tree. 
        /// </summary>
        public long Root
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Root; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Root = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The table header page for the tree.
        /// </summary>
        public long Table
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Table; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Table = value;
                IsModified = true;
            }
        }

        public bool IsModified
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _isModified; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (_tx.Flags != TransactionFlags.ReadWrite)
                    throw new InvalidOperationException("Invalid operation outside of a write transaction");
                _isModified = value;
            }
        }

        public bool IsAllocated => Root <= 0 && Table <= 0;
    }
}
