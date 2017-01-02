using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.BTrees.Cedar;
using Voron.Global;

namespace Voron.Data.BTrees
{
    unsafe partial class CedarPage
    {
        public struct HeaderAccessor
        {
            private readonly CedarPage _page;

            private bool _isWritable;
            public CedarPageHeader* Ptr;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public HeaderAccessor(CedarPage page)
            {
                _page = page;

                Ptr = (CedarPageHeader*)_page.GetHeaderPagePointer(false);
                _isWritable = false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public HeaderAccessor(CedarPage page, byte* ptr)
            {
                _page = page;

                Ptr = (CedarPageHeader*)ptr;
                _isWritable = true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SetWritable()
            {
                if (_isWritable) return;

                Ptr = (CedarPageHeader*)_page.GetHeaderPagePointer(true);
                _isWritable = true;
            }
        }

        public struct CedarDataNodeReadPtr
        {
            private readonly CedarPage _page;
            private readonly int _offset;

            public CedarDataNodeReadPtr(CedarPage page, int offset)
            {
                this._page = page;
                this._offset = offset;
            }

            public CedarDataNode* this[int index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return _page.GetDataNodeReadPointer(index + _offset); }
            }

            public static CedarDataNodeReadPtr operator ++(CedarDataNodeReadPtr node)
            {
                return new CedarDataNodeReadPtr(node._page, node._offset + 1);
            }

            public static CedarDataNodeReadPtr operator +(CedarDataNodeReadPtr node, int i)
            {
                return new CedarDataNodeReadPtr(node._page, node._offset + i);
            }

            public static CedarDataNodeReadPtr operator -(CedarDataNodeReadPtr node, int i)
            {
                return new CedarDataNodeReadPtr(node._page, node._offset - i);
            }

            public static implicit operator CedarDataNode* (CedarDataNodeReadPtr node)
            {
                return node._page.GetDataNodeReadPointer(node._offset);
            }
        }

        public struct CedarDataNodeWritePtr
        {
            private readonly CedarPage _page;
            private readonly int _offset;

            public CedarDataNodeWritePtr(CedarPage page, int offset)
            {
                this._page = page;
                this._offset = offset;
            }

            public static CedarDataNodeWritePtr operator ++(CedarDataNodeWritePtr node)
            {
                return new CedarDataNodeWritePtr(node._page, node._offset + 1);
            }

            public static CedarDataNodeWritePtr operator +(CedarDataNodeWritePtr node, int i)
            {
                return new CedarDataNodeWritePtr(node._page, node._offset + i);
            }

            public static CedarDataNodeWritePtr operator -(CedarDataNodeWritePtr node, int i)
            {
                return new CedarDataNodeWritePtr(node._page, node._offset - i);
            }

            public static implicit operator CedarDataNode* (CedarDataNodeWritePtr node)
            {
                return node._page.GetDataNodeWritePointer(node._offset);
            }
        }

        public struct DataAccessor
        {
            private readonly CedarPage _page;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public DataAccessor(CedarPage page)
            {
                _page = page;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public CedarDataNodeReadPtr DirectRead(long i = 0)
            {
                Debug.Assert(i >= 0);

                return new CedarDataNodeReadPtr(_page, (int)i);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public CedarDataNodeWritePtr DirectWrite(long i = 0)
            {
                Debug.Assert(i >= 0);

                return new CedarDataNodeWritePtr(_page, (int)i);
            }

            internal int NextFree
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)] get
                {
                    return _page.Header.Ptr->NextFreeDataNode;
                }
                [MethodImpl(MethodImplOptions.AggressiveInlining)] set
                {
                    _page.Header.SetWritable();
                    _page.Header.Ptr->NextFreeDataNode = value;
                }
            }

            public bool CanAllocateNode()
            {
                // If there are no more allocable nodes, we fail.
                if (NextFree == -1)
                    return false;

                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int AllocateNode()
            {                
                // If there are no more allocable nodes, we fail.
                if (NextFree == -1)
                    throw new InvalidOperationException("Cannot allocate more nodes. There is no enough space. This cannot happen.");

                // We need write access.                
                int index = NextFree;
                Debug.Assert(index >= 0, "Index cannot be negative.");
                Debug.Assert(index < _page.Header.Ptr->DataNodesTotalCount, "Index cannot be bigger than the quantity of nodes available to use.");

                var accessor = new CedarDataNodeWritePtr(_page, index);

                var ptr = (CedarDataNode*)accessor;
                Debug.Assert(ptr->IsFree);

                // We will store in the data pointer the next free.
                NextFree = (int)ptr->Data;
                ptr->IsFree = false;

                return index;
            }

            public void FreeNode(int index)
            {
                Debug.Assert(index >= 0, "Index cannot be negative.");
                Debug.Assert(index < _page.Header.Ptr->DataNodesTotalCount, "Index cannot be bigger than the quantity of nodes available to use.");

                var accessor = new CedarDataNodeWritePtr(_page, index);
                var ptr = (CedarDataNode*)accessor;
                if (ptr->IsFree)
                    throw new InvalidOperationException("Pointer cannot be freed while it is already free.");

                int currentFree = NextFree;

                ptr->IsFree = true;
                ptr->Data = currentFree;

                NextFree = index;
            }

            public void Initialize()
            {
                CedarDataNode* node;

                int count = 0;                 
                int pages = _page.Header.Ptr->DataNodesPageCount;
                for ( int page = 0; page < pages; page++)
                {
                    node = _page.GetDataNodeWritePointer(page * CedarPageHeader.DataNodesPerPage);
                    for (int i = 0; i < CedarPageHeader.DataNodesPerPage; i++, count++, node++)
                    {
                        // Link the current node to the next.
                        node->Header = CedarDataNode.FreeNode;
                        node->Data = count + 1;                                                                        
                    }
                }

                // Close the linked list.
                node = _page.GetDataNodeWritePointer(pages * CedarPageHeader.DataNodesPerPage - 1);
                node->Header = CedarDataNode.FreeNode;
                node->Data = -1;

                Debug.Assert(this.DirectRead(_page.Header.Ptr->DataNodesTotalCount - 1)[0]->IsFree, "Last node is not free.");
                Debug.Assert(this.DirectRead(_page.Header.Ptr->DataNodesTotalCount - 1)[0]->Data == -1, "Free node linked list does not end.");

                this.NextFree = 0;
            }
        }

        internal struct NodesReadPtr
        {
            private readonly CedarPage _page;
            private readonly int _offset;

            private int _pageIndexLow;
            private byte* _dataPtr;

            public NodesReadPtr(CedarPage page, long offset) : this(page, (int)offset)
            { }

            public NodesReadPtr(CedarPage page, int offset = 0)
            {
                Debug.Assert(page != null);
                Debug.Assert(offset >= 0);

                this._page = page;
                this._pageIndexLow = int.MaxValue;
                this._dataPtr = null;
                this._offset = offset;
            }

            public Node* this[int index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    int current = _offset + index;
                    if (current >= _pageIndexLow && current < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                    {
                        current = current % CedarPageHeader.BlocksPerPage;
                        return (Node*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * current);
                    }

                    return _page.GetNodesReadPointer(current, out _dataPtr, out _pageIndexLow);
                }
            }

            public Node* this[long index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    int current = _offset + (int)index;
                    if (current >= _pageIndexLow && current < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                    {
                        current = current % CedarPageHeader.BlocksPerPage;
                        return (Node*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * current);
                    }

                    return _page.GetNodesReadPointer(current, out _dataPtr, out _pageIndexLow);
                }
            }
        }

        internal struct NodesWritePtr
        {
            private readonly CedarPage _page;
            private readonly int _offset;

            private int _pageIndexLow;
            private byte* _dataPtr;
            private bool _isWritable;


            public NodesWritePtr(CedarPage page, long offset) : this(page, (int)offset)
            { }

            public NodesWritePtr(CedarPage page, int offset = 0)
            {
                Debug.Assert(page != null);
                Debug.Assert(offset >= 0);

                this._page = page;
                this._pageIndexLow = int.MaxValue;
                this._dataPtr = null;
                this._offset = offset;
                this._isWritable = false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Node* Read(int index)
            {
                int current = _offset + index;
                if (current >= _pageIndexLow && current < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                {
                    current = current % CedarPageHeader.BlocksPerPage;
                    return (Node*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * current);
                }

                _isWritable = false;
                return _page.GetNodesReadPointer(current, out _dataPtr, out _pageIndexLow);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Node* Read(long index)
            {
                return Read((int)index);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Node* Write(int index)
            {
                int current = _offset + index;
                if (_isWritable && current >= _pageIndexLow && current < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                {
                    current = current % CedarPageHeader.BlocksPerPage;
                    return (Node*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * current);
                }

                _isWritable = true;
                return _page.GetNodesWritePointer(current, out _dataPtr, out _pageIndexLow);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Node* Write(long index)
            {
                return Write((int)index);
            }
        }

        internal struct NodesInfoReadPtr
        {
            private readonly CedarPage _page;

            private int _pageIndexLow;
            private byte* _dataPtr;

            public NodesInfoReadPtr(CedarPage page)
            {
                Debug.Assert(page != null);
                this._page = page;
                this._pageIndexLow = int.MaxValue;
                this._dataPtr = null;
            }

            public NodeInfo* this[int index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    if (index >= _pageIndexLow && index < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                    {
                        long offset = index % CedarPageHeader.BlocksPerPage;
                        return (NodeInfo*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * offset + sizeof(Node));
                    }

                    return _page.GetNodesInfoReadPointer(index, out _dataPtr, out _pageIndexLow);
                }
            }

            public NodeInfo* this[long index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    if (index >= _pageIndexLow && index < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                    {
                        long offset = index % CedarPageHeader.BlocksPerPage;
                        return (NodeInfo*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * offset + sizeof(Node));
                    }

                    return _page.GetNodesInfoReadPointer((int)index, out _dataPtr, out _pageIndexLow);
                }
            }
        }

        internal struct NodesInfoWritePtr
        {
            private readonly CedarPage _page;

            private int _pageIndexLow;
            private byte* _dataPtr;
            private bool _isWritable;

            public NodesInfoWritePtr(CedarPage page)
            {
                Debug.Assert(page != null);
                this._page = page;
                this._pageIndexLow = int.MaxValue;
                this._dataPtr = null;
                this._isWritable = false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NodeInfo* Write(int index)
            {
                if (_isWritable && index >= _pageIndexLow && index < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                {
                    long offset = index % CedarPageHeader.BlocksPerPage;
                    return (NodeInfo*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * offset + sizeof(Node));
                }

                _isWritable = true;
                return _page.GetNodesInfoWritePointer(index, out _dataPtr, out _pageIndexLow);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NodeInfo* Write(long index)
            {
                return Write((int)index);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NodeInfo* Read(int index)
            {
                if (_isWritable && index >= _pageIndexLow && index < _pageIndexLow + CedarPageHeader.BlocksPerPage)
                {
                    long offset = index % CedarPageHeader.BlocksPerPage;
                    return (NodeInfo*)(_dataPtr + (sizeof(NodeInfo) + sizeof(Node)) * offset + sizeof(Node));
                }

                _isWritable = false;
                return _page.GetNodesInfoReadPointer(index, out _dataPtr, out _pageIndexLow);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NodeInfo* Read(long index)
            {
                return Read((int)index);
            }

        }

        internal struct BlockMetadataReadPtr
        {
            private readonly CedarPage _page;

            public BlockMetadataReadPtr(CedarPage page)
            {
                Debug.Assert(page != null);

                this._page = page;
            }

            public BlockMetadata* Read(int index)
            {
                var dataPtr = _page._pageLocator.GetReadOnlyDataPointer(_page.PageNumber);
                return (BlockMetadata*)(dataPtr + CedarPageHeader.MetadataOffset) + index;
            }
        }

        internal struct BlockMetadataWritePtr
        {
            private readonly CedarPage _page;

            public BlockMetadataWritePtr(CedarPage page)
            {
                Debug.Assert(page != null);
                this._page = page;
            }

            public BlockMetadata* Write(int index)
            {
                var dataPtr = _page._pageLocator.GetWritableDataPointer(_page.PageNumber);
                return (BlockMetadata*)(dataPtr + CedarPageHeader.MetadataOffset) + index;
            }

            public BlockMetadata* Read(int index)
            {
                var dataPtr = _page._pageLocator.GetReadOnlyDataPointer(_page.PageNumber);
                return (BlockMetadata*)(dataPtr + CedarPageHeader.MetadataOffset) + index;
            }
        }

        internal struct BlocksAccessor
        {
            private readonly CedarPage _page;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public BlocksAccessor(CedarPage page)
            {
                _page = page;
            }

            /// <summary>
            /// Returns the first <see cref="Node"/>. CAUTION: Can only be used for reading, use DirectWrite if you need to write to it.
            /// </summary>
            internal NodesReadPtr Nodes
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return new NodesReadPtr(_page); }
            }

            /// <summary>
            /// Returns the first <see cref="NodeInfo"/>. CAUTION: Can only be used for reading, use DirectWrite if you need to write to it.
            /// </summary>
            internal NodesInfoReadPtr NodesInfo
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return new NodesInfoReadPtr(_page); }
            }

            /// <summary>
            /// Returns the first <see cref="BlockMetadata"/>. CAUTION: Can only be used for reading, use DirectWrite if you need to write to it.
            /// </summary>
            internal BlockMetadataReadPtr Metadata
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return new BlockMetadataReadPtr(_page); }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public T DirectWrite<T>() where T : struct
            {
                if (typeof(T) == typeof(BlockMetadataWritePtr))
                {
                    return (T)(object)new BlockMetadataWritePtr(_page);
                }

                if (typeof(T) == typeof(NodesInfoWritePtr))
                {
                    return (T)(object)new NodesInfoWritePtr(_page);
                }

                if (typeof(T) == typeof(NodesWritePtr))
                {
                    return (T)(object)new NodesWritePtr(_page);
                }

                return ThrowNotSupportedException<T>();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public T DirectWrite<T>(long i) where T : struct
            {
                // This is the only one that will ever use the indexed access. 
                if (typeof(T) == typeof(NodesWritePtr))
                {
                    return (T)(object)new NodesWritePtr(_page, i);
                }

                return ThrowNotSupportedException<T>();
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private T ThrowNotSupportedException<T>()
            {
                throw new NotSupportedException("Access type not supported by this accessor.");
            }
        }

        internal struct Tail0Accessor
        {
            private bool _isWritable;
            private CedarPage _page;
            private int* _dataPtr;


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Tail0Accessor(CedarPage page)
            {
                _page = page;

                _dataPtr = (int*)(_page.GetHeaderPagePointer(false) + CedarPageHeader.Tail0Offset);
                _isWritable = false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Tail0Accessor(CedarPage page, byte* ptr)
            {
                _page = page;

                _dataPtr = (int*)(ptr + CedarPageHeader.Tail0Offset);
                _isWritable = true;
            }

            public int Length
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this[0]; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this[0] = value; }
            }

            public int this[long i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    return *(_dataPtr + i);
                }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    SetWritable();
                   
                    *(_dataPtr + i) = value;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SetWritable()
            {
                if (_isWritable) return;
                _dataPtr = (int*)(_page.GetHeaderPagePointer(true) + CedarPageHeader.Tail0Offset);
                _isWritable = true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                SetWritable();

                int length = this[0];
                Memory.SetInline((byte*)_dataPtr, 0, (length + 1) * sizeof(int));
            }
        }

        internal struct TailAccessor
        {
            private readonly CedarPage _page;

            public TailAccessor(CedarPage page)
            {
                this._page = page;
            }

            public int Length
            {
                get { return *(int*)_page.GetTailReadPointer(0); }
                set { *(int*)_page.GetTailWritePointer(0) = value; } 
            }

            public int TotalBytes => (Constants.Storage.PageSize - PageHeader.SizeOf) * _page.Layout.TailPages;

            public byte this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return Read<byte>(i); }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public T Read<T>(int pos = 0) where T : struct
            {
                // IMPORTANT: Tail pages must deal with the posibility that a short value could be written in between 2 pages.
                //            In those cases the upper part will be written in page P and the lower part will be written in page P+1
                if (typeof(T) == typeof(short))
                {
                    byte* end;
                    byte* ptr = _page.GetTailReadPointer(pos, out end);
                    if (end - ptr >= sizeof(short))
                    {
                        // Likely case we just write and be done with it.
                        short value = *(short*)ptr;
                        return (T)(object)value;
                    }

                    return (T)(object)UnlikelyReadShort(pos, ptr);
                }

                if (typeof(T) == typeof(byte))
                {
                    byte value = *_page.GetTailReadPointer(pos);
                    return (T)(object)value;
                }

                return ThrowNotSupportedException<T>();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Write<T>(int pos, T value) where T : struct
            {
                // IMPORTANT: Tail pages must deal with the posibility that a short value could be written in between 2 pages.
                //            In those cases the upper part will be written in page P and the lower part will be written in page P+1
                if (typeof(T) == typeof(short))
                {
                    byte* end;
                    byte* ptr = _page.GetTailWritePointer(pos, out end);
                    if (end - ptr >= sizeof(short))
                    {
                        // Likely case we just write and be done with it.
                        *(short*)ptr = (short)(object)value;
                    }
                    else
                    {
                        UnlikelyWriteShort(pos, (short)(object)(value), ptr);
                    }
                }
                else if (typeof(T) == typeof(byte))
                {
                    *_page.GetTailWritePointer(pos) = (byte)(object)value;
                }
                else
                {
                    ThrowNotSupportedException();
                }
            }


            [MethodImpl(MethodImplOptions.NoInlining)]
            private T ThrowNotSupportedException<T>()
            {
                throw new NotSupportedException("Access type not supported by this accessor.");
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private void ThrowNotSupportedException()
            {
                throw new NotSupportedException("Access type not supported by this accessor.");
            }

            private short UnlikelyReadShort(int pos, byte* ptr)
            {
                // Unlikely and more costly case.
                byte* nextPtr = _page.GetTailReadPointer(pos + 1);
                short svalue = (short)((*nextPtr << 8) + *ptr);
                return svalue;
            }

            private void UnlikelyWriteShort(int pos, short value, byte* ptr)
            {
                // OPTIMIZE: Verify for inlining if it would make sense for this branch to be a method call. 

                // Unlikely and more costly case.
                byte* nextPtr = _page.GetTailWritePointer(pos + 1);

                *ptr = (byte)value;
                *nextPtr = (byte)(value >> 8);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public T Read<T>(long pos) where T : struct
            {
                return Read<T>((int)pos);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Write<T>(long pos, T value) where T : struct
            {
                Write((int)pos, value);
            }

            public int StrLength(int offset)
            {
                byte* end;
                byte* ptr = _page.GetTailReadPointer(offset, out end);

                int length = 0;
                while (*ptr != 0)
                {
                    ptr++;
                    length++;
                    offset++;

                    if (ptr != end)
                        continue;
                    Debug.Assert(offset % CedarPageHeader.TailBytesPerPage == 0);
                    ptr = _page.GetTailReadPointer(offset, out end);
                }

                return length;
            }       

            public int Compare(int offset, byte* p2, int length, out int distance)
            {
                int leftToProcess = length;

                distance = 0;
                while (leftToProcess > 0)
                {
                    byte* end;
                    byte* ptr = _page.GetTailReadPointer(offset, out end);

                    int newLength = Math.Min(leftToProcess, (int)(end - ptr));

                    int newPosition;
                    int r = Memory.Compare(ptr, p2, newLength, out newPosition);
                    if (r != 0)
                    {
                        distance += newPosition;
                        return r;
                    }

                    distance += newLength;
                    leftToProcess -= newLength;
                    offset += newLength;
                    p2 += newLength;
                }

                return 0;
            }

            public int Compare(int offset, byte* p2, int length)
            {
                int leftToProcess = length;
                while (leftToProcess > 0)
                {
                    byte* end;
                    byte* ptr = _page.GetTailReadPointer(offset, out end);

                    int newLength = Math.Min(leftToProcess, (int)(end - ptr));
                    int r = Memory.Compare(ptr, p2, newLength);
                    if (r != 0)
                        return r;

                    leftToProcess -= newLength;
                    offset += newLength;
                    p2 += newLength;
                }

                return 0;
            }


            public void Set(byte value)
            {
                CedarPageHeader* header = _page.Header.Ptr;

                int pageSize = CedarPageHeader.TailBytesPerPage;
                int tailPageCount = header->TailPageCount;
                long tailPageNumber = header->TailPageNumber;

                for (int i = 0; i < tailPageCount; i++)
                {
                    var tailPage = _page._pageLocator.GetReadOnlyPage(tailPageNumber + i);
                    Memory.SetInline(tailPage.DataPointer, value, pageSize);
                }
            }

            public void CopySectionTo(byte* dest)
            {
                CedarPageHeader* header = _page.Header.Ptr;

                int pageSize = Constants.Storage.PageSize;
                int tailPageCount = header->TailPageCount;
                long tailPageNumber = header->TailPageNumber;

                for (int i = 0; i < tailPageCount; i++)
                {
                    var tailPage = _page._pageLocator.GetReadOnlyPage(tailPageNumber + i);
                    Memory.CopyInline(dest, tailPage.Pointer, pageSize);

                    dest += pageSize;
                }                                
            }

            public int CopyDataTo(int offset, byte* dest)
            {
                byte* end = null;
                byte* ptr = null;

                int i = 0;
                do
                {
                    if (ptr == end)
                        ptr = _page.GetTailReadPointer(offset, out end);

                    dest[i] = *ptr;

                    offset++;
                    i++;
                }
                while (*ptr++ != 0);

                if (end - ptr >= sizeof(short))
                {
                    // Likely case we just write and be done with it.
                    *(short*)&dest[i] = *(short*)ptr;
                }
                else
                {
                    // This Read<short> statement will include the offset. 
                    *(short*)&dest[i] = Read<short>(offset);
                }

                return i + sizeof(short);
            }

            public void CopyFrom(int offset, int startOffset, byte* src, int length)
            {
                offset += startOffset;

                int leftToProcess = length;
                while (leftToProcess > 0)
                {
                    byte* end;
                    byte* ptr = _page.GetTailWritePointer(offset, out end);

                    int newLength = Math.Min(leftToProcess, (int)(end - ptr));
                    Memory.CopyInline(ptr, src, newLength);

                    leftToProcess -= newLength;
                    offset += newLength;
                    src += newLength;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void CopyFrom(byte* src, int length)
            {
                CopyFrom(0, 0, src, length);
            }

            public int CopyKeyTo(int offset, byte* dest)
            {
                byte* end;
                byte* ptr = _page.GetTailReadPointer(offset, out end);

                int i = 0;
                while (*ptr != 0)
                {
                    dest[i] = *ptr;

                    offset++;
                    ptr++;
                    i++;

                    if (ptr == end)
                    {
                        Debug.Assert(offset % CedarPageHeader.TailBytesPerPage == 0);
                        ptr = _page.GetTailReadPointer(offset, out end);
                    }
                }
                
                return i;
            }

            public int CopyTo(int offset, byte* dest, int length)
            {
                int leftToProcess = length;
                while (leftToProcess > 0)
                {
                    byte* end;
                    byte* ptr = _page.GetTailReadPointer(offset, out end);

                    int newLength = Math.Min(leftToProcess, (int)(end - ptr));
                    Memory.CopyInline(dest, ptr, newLength);

                    leftToProcess -= newLength;
                    offset += newLength;
                }

                return length;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* GetHeaderPagePointer( bool writable )
        {
            if (writable)
                return _pageLocator.GetWritablePage(this.PageNumber).Pointer;
            else
                return _pageLocator.GetReadOnlyPage(this.PageNumber).Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private NodeInfo* GetNodesInfoReadPointer(int index, out byte* basePtr, out int firstOffset)
        {
            Debug.Assert(index < Header.Ptr->BlocksTotalCount - (Header.Ptr->BlocksTotalCount % BlockSize));

            int pageOffset = index / CedarPageHeader.BlocksPerPage;
            long pageNumber = Header.Ptr->BlocksPageNumber + pageOffset;
            firstOffset = pageOffset * CedarPageHeader.BlocksPerPage;

            Debug.Assert(pageNumber < Header.Ptr->BlocksPageNumber + Header.Ptr->BlocksPageCount);

            basePtr = _pageLocator.GetReadOnlyDataPointer(pageNumber);

            int offset = index - firstOffset;
            return (NodeInfo*)(basePtr + (sizeof(NodeInfo) + sizeof(Node)) * offset + sizeof(Node));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private NodeInfo* GetNodesInfoWritePointer(int index, out byte* basePtr, out int firstOffset)
        {
            Debug.Assert(index < Header.Ptr->BlocksTotalCount - (Header.Ptr->BlocksTotalCount % BlockSize));

            int pageOffset = index / CedarPageHeader.BlocksPerPage;
            long pageNumber = Header.Ptr->BlocksPageNumber + pageOffset;
            firstOffset = pageOffset * CedarPageHeader.BlocksPerPage;

            Debug.Assert(pageNumber < Header.Ptr->BlocksPageNumber + Header.Ptr->BlocksPageCount);

            basePtr = _pageLocator.GetWritableDataPointer(pageNumber);

            int offset = index - firstOffset;
            return (NodeInfo*)(basePtr + (sizeof(NodeInfo) + sizeof(Node)) * offset + sizeof(Node));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Node* GetNodesReadPointer(int index, out byte* basePtr, out int firstOffset)
        {
            Debug.Assert(index < Header.Ptr->BlocksTotalCount - (Header.Ptr->BlocksTotalCount % BlockSize));

            int pageOffset = index / CedarPageHeader.BlocksPerPage;
            long pageNumber = Header.Ptr->BlocksPageNumber + pageOffset;
            firstOffset = pageOffset * CedarPageHeader.BlocksPerPage;

            Debug.Assert(pageNumber < Header.Ptr->BlocksPageNumber + Header.Ptr->BlocksPageCount);

            basePtr = _pageLocator.GetReadOnlyDataPointer(pageNumber);

            int offset = index - firstOffset;
            return (Node*)(basePtr + (sizeof(NodeInfo) + sizeof(Node)) * offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Node* GetNodesWritePointer(int index, out byte* basePtr, out int firstOffset)
        {
            Debug.Assert(index < Header.Ptr->BlocksTotalCount - (Header.Ptr->BlocksTotalCount % BlockSize));

            int pageOffset = index / CedarPageHeader.BlocksPerPage;
            long pageNumber = Header.Ptr->BlocksPageNumber + pageOffset;
            firstOffset = pageOffset * CedarPageHeader.BlocksPerPage;

            Debug.Assert(pageNumber < Header.Ptr->BlocksPageNumber + Header.Ptr->BlocksPageCount);

            basePtr = _pageLocator.GetWritableDataPointer(pageNumber);

            int offset = index - firstOffset;
            return (Node*)(basePtr + (sizeof(NodeInfo) + sizeof(Node)) * offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CedarDataNode* GetDataNodeReadPointer(int index)
        {
            Debug.Assert(index < Header.Ptr->DataNodesTotalCount);

            int pageOffset = index / CedarPageHeader.DataNodesPerPage;
            long pageNumber = Header.Ptr->DataNodesPageNumber + pageOffset;
            int offset = index - pageOffset * CedarPageHeader.DataNodesPerPage;

            var dataPtr = _pageLocator.GetReadOnlyDataPointer(pageNumber);

            return ((CedarDataNode*)dataPtr) + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CedarDataNode* GetDataNodeWritePointer(int index)
        {
            Debug.Assert(index < Header.Ptr->DataNodesTotalCount);

            int pageOffset = index / CedarPageHeader.DataNodesPerPage;
            long pageNumber = Header.Ptr->DataNodesPageNumber + pageOffset;
            int offset = index - pageOffset * CedarPageHeader.DataNodesPerPage;

            var dataPtr = _pageLocator.GetWritableDataPointer(pageNumber);

            return ((CedarDataNode*)dataPtr) + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetTailReadPointer(int offset)
        {
            Debug.Assert(offset < Header.Ptr->TailTotalBytes);

            int pageOffset = offset / CedarPageHeader.TailBytesPerPage;
            long pageNumber = Header.Ptr->TailPageNumber + pageOffset;
            offset = offset - pageOffset * CedarPageHeader.TailBytesPerPage;

            var dataPtr = _pageLocator.GetReadOnlyDataPointer(pageNumber);

            return dataPtr + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetTailReadPointer(int offset, out byte* end)
        {
            Debug.Assert(offset < Header.Ptr->TailTotalBytes);

            // The pointer will also return where it ends. 
            // It is the responsability of the caller to deal with the algorithmics required to perform multi-byte operations.
            int pageOffset = offset / CedarPageHeader.TailBytesPerPage;
            long pageNumber = Header.Ptr->TailPageNumber + pageOffset;
            offset = offset - pageOffset * CedarPageHeader.TailBytesPerPage;

            var dataPtr = _pageLocator.GetReadOnlyDataPointer(pageNumber);

            end = dataPtr + CedarPageHeader.TailBytesPerPage;
            return dataPtr + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetTailWritePointer(int offset)
        {
            Debug.Assert(offset < Header.Ptr->TailTotalBytes);

            int pageOffset = offset / CedarPageHeader.TailBytesPerPage;
            long pageNumber = Header.Ptr->TailPageNumber + pageOffset;
            offset = offset - pageOffset * CedarPageHeader.TailBytesPerPage;

            var dataPtr = _pageLocator.GetWritableDataPointer(pageNumber);
            return dataPtr + offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetTailWritePointer(int offset, out byte* end)
        {
            Debug.Assert(offset < Header.Ptr->TailTotalBytes);

            // The pointer will also return where it ends. 
            // It is the responsability of the caller to deal with the algorithmics required to perform multi-byte operations.
            int pageOffset = offset / CedarPageHeader.TailBytesPerPage;
            long pageNumber = Header.Ptr->TailPageNumber + pageOffset;
            offset = offset - pageOffset * CedarPageHeader.TailBytesPerPage;

            var dataPtr = _pageLocator.GetWritableDataPointer(pageNumber);

            end = dataPtr + CedarPageHeader.TailBytesPerPage;
            return dataPtr + offset;
        }
    }
}
