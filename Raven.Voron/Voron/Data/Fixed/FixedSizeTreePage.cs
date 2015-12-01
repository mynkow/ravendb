using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron.Data.Fixed
{
    public unsafe class FixedSizeTreePage
    {
        public readonly byte* Pointer;
        public readonly int PageSize;
        public readonly string Source;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        private FixedSizeTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (FixedSizeTreePageHeader*)Pointer; }
        }

        public FixedSizeTreePage(byte* pointer, string source, int pageSize)
        {
            Pointer = pointer;
            Source = source;
            PageSize = pageSize;
        }


        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PageNumber = value; }
        }

        public FixedSizeTreePageFlags FixedTreeFlags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->TreeFlags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->TreeFlags = value; }
        }

        public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & FixedSizeTreePageFlags.Leaf) == FixedSizeTreePageFlags.Leaf; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & FixedSizeTreePageFlags.Branch) == FixedSizeTreePageFlags.Branch; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int PageMaxSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return PageSize - Constants.FixedSizeTreePageHeaderSize; }
        }


        public ushort NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->NumberOfEntries; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->NumberOfEntries = value; }
        }

        public ushort StartPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->StartPosition; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->StartPosition = value; }
        }

        public ushort ValueSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->ValueSize; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->ValueSize = value; }
        }

        public override string ToString()
        {
            return "#" + PageNumber + " (count: " + NumberOfEntries + ") " + FixedTreeFlags;
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Flags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Flags = value; }
        }
    }
}