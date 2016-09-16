using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The Cedar cursor is able to navigate inside all the keys on a page and also move to the next page to continue moving forward.
    /// </summary>
    public unsafe class CedarCursor : IDisposable
    {
        private readonly LowLevelTransaction _llt;        
        private readonly List<long> _path;
        private readonly CedarTree _tree;

        private CedarPage _currentPage;

        public CedarCursor(LowLevelTransaction llt, CedarTree tree, CedarPage currentPage, List<long> pathFromRoot)
        {
            this._llt = llt;
            this._tree = tree;
            this._currentPage = currentPage;
            this._path = new List<long>(pathFromRoot);
        }

        public CedarCursor(LowLevelTransaction llt, CedarTree tree, CedarPage currentPage)
            : this(llt, tree, currentPage, new List<long> {})
        {            
        }

        public CedarPage CurrentPage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _currentPage; }
        }

        public CedarPage ParentPage
        {
            get
            {
                if ( _path.Count == 0 )
                    throw new InvalidOperationException("Cannot request the ParentPage of a root node.");

                return _tree.GetPage(_path[_path.Count - 1]);
            }
        }


        public Slice Key { get; internal set; }

        public int PageDepth => _path.Count;

        public ushort NodeVersion
        {
            get
            {
                if (Pointer == null)
                    throw new InvalidOperationException();

                return Pointer->Version;
            }
        }

        public int ValueSize
        {
            get
            {
                if (Pointer == null)
                    throw new InvalidOperationException();

                return Pointer->DataSize;
            }
        }

        public byte* Value
        {
            get
            {
                if (Pointer == null)
                    throw new InvalidOperationException();

                return (byte*) &Pointer->Data;
            }
        }

        public CedarDataPtr* Pointer { get; private set; }

        public CedarTuple Result;

        #region IDisposable Support

        private bool _disposed = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                _disposed = true;
            }
        }

        ~CedarCursor()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);

            // TODO: uncomment the following line if the finalizer is overridden above.
            GC.SuppressFinalize(this);
        }

        public void Push(CedarPage page)
        {
            this._path.Add(_currentPage.PageNumber);
            _currentPage = page;
        }

        public CedarPage Pop()
        {
            var result = _currentPage;

            _currentPage = _tree.GetPage(_path[_path.Count - 1]);
            this._path.RemoveAt(_path.Count - 1);

            return result;
        }

        #endregion

        public void Seek(Slice key)
        {
            if (key.Options == SliceOptions.Key)
            {
                Lookup(key);
            }
            else if (key.Options == SliceOptions.BeforeAllKeys)
            {
                LookupFirst();
            }
            else if (key.Options == SliceOptions.AfterAllKeys)
            {
                LookupLast();
            }
        }

        private void LookupLast()
        {
            Key = Slices.AfterAllKeys;
            Pointer = null;

            if (_currentPage.IsLeaf)
                return;

            throw new NotImplementedException();
        }

        private void LookupFirst()
        {
            Key = Slices.BeforeAllKeys;
            Pointer = null;

            if (_currentPage.IsLeaf)
                return;

            throw new NotImplementedException();
        }

        private void Lookup(Slice key)
        {
            if (_currentPage.IsLeaf)
            {
                CedarDataPtr* ptr;

                if (_currentPage.ExactMatchSearch(key, out Result, out ptr) == CedarResultCode.Success)
                {
                    Key = key;
                    Pointer = ptr;
                }
                else
                {
                    Key = Slices.Empty;
                    Pointer = null;
                }

                return;
            }

            throw new NotImplementedException();
        }
    }
}
