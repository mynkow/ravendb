using System;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public unsafe class TreePageIterator : IIterator
    {
        private readonly TreePage _page;
        private SlicePointer _currentKey = null;
        private SlicePointer _currentInternalKey;
        private bool _disposed;

        public TreePageIterator(TreePage page)
        {
            _page = page;
        }

        public void Dispose()
        {
            _disposed = true;

            OnDisposal?.Invoke(this);
        }

        public bool Seek<T>(T key)
            where T : class, ISlice
        {
            if(_disposed)
                throw new ObjectDisposedException("PageIterator");
            var current = _page.Search(key);
            if (current == null)
                return false;

            _page.SetNodeKey(current, ref _currentInternalKey);
            _currentKey = _currentInternalKey;

            return this.ValidateCurrentKey(current, _page);
        }

        public TreeNodeHeader* Current
        {
            get
            {

                if (_disposed)
                    throw new ObjectDisposedException("PageIterator");
                if (_page.LastSearchPosition< 0  || _page.LastSearchPosition >= _page.NumberOfEntries)
                    throw new InvalidOperationException("No current page was set");
                return _page.GetNode(_page.LastSearchPosition);
            }
        }


        public SlicePointer CurrentKey
        {
            get
            {

                if (_disposed)
                    throw new ObjectDisposedException("PageIterator");
                if (_page.LastSearchPosition < 0 || _page.LastSearchPosition >= _page.NumberOfEntries)
                    throw new InvalidOperationException("No current page was set");
                return _currentKey;
            }
        }
        public int GetCurrentDataSize()
        {
            if (_disposed)
                throw new ObjectDisposedException("PageIterator");
            return Current->DataSize;
        }


        public SliceArray RequiredPrefix { get; set; }
        public SliceArray MaxKey { get; set; }

        public bool MoveNext()
        {
            _page.LastSearchPosition++;
            return TrySetPosition();
        }

        public bool MovePrev()
        {
            _page.LastSearchPosition--;

            return TrySetPosition();

        }

        public bool Skip(int count)
        {
            _page.LastSearchPosition += count;
            
            return TrySetPosition();
        }

        private bool TrySetPosition()
        {

            if (_disposed)
                throw new ObjectDisposedException("PageIterator");
            if (_page.LastSearchPosition < 0 || _page.LastSearchPosition >= _page.NumberOfEntries)
                return false;

            var current = _page.GetNode(_page.LastSearchPosition);
            if (this.ValidateCurrentKey(current, _page) == false)
            {
                return false;
            }
            _page.SetNodeKey(current, ref _currentInternalKey);
            _currentKey = _currentInternalKey;
            return true;
        }

        public ValueReader CreateReaderForCurrent()
        {
            var node = Current;
            return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
        }

        public event Action<IIterator> OnDisposal;
    }
}
