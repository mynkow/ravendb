using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
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
        private Slice _outputKey;

        public CedarCursor(LowLevelTransaction llt, CedarTree tree, CedarPage currentPage, List<long> pathFromRoot)
        {
            this._llt = llt;
            this._tree = tree;
            this._currentPage = currentPage;
            this._path = new List<long>(pathFromRoot);

            if (this._path.Last() != currentPage.PageNumber)
                this._path.Add(currentPage.PageNumber);

            this._outputKey = Slice.Create(llt.Allocator, 4096);
        }

        public CedarCursor(LowLevelTransaction llt, CedarTree tree, CedarPage currentPage)
            : this(llt, tree, currentPage, new List<long> { currentPage.PageNumber })
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
                if (_path.Count == 0)
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

                return (byte*)&Pointer->Data;
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
            this._currentPage = page;
            this._path.Add(page.PageNumber);
        }

        public CedarPage Pop()
        {
            var result = _currentPage;

            this._currentPage = _tree.GetPage(_path[_path.Count - 1]);
            this._path.RemoveAt(_path.Count - 1);

            if (this._path.Count == 0)
                _currentPage = null;

            return result;
        }

        #endregion

        /// <summary>
        /// The difference between Seek and Lookup is that we dont care about finding the actual data node, only the page where it can reside. 
        /// </summary>
        /// <param name="key">The key we want to find the leaf page for.</param>
        /// <returns>true if successful, false otherwise.</returns>
        public bool Lookup(Slice key)
        {
            if (key.Options == SliceOptions.Key)
            {
                while (_currentPage.IsBranch)
                {
                    long from = 0;
                    long keyLength = 0;
                    CedarPage.IteratorValue iterator = _currentPage.PredecessorOrEqual(key, ref _outputKey, ref @from, ref keyLength);

                    // We do not have a key that matches the range.
                    CedarDataPtr* ptr;
                    if (iterator.Error == CedarResultCode.Success)
                    {
                        ptr = _currentPage.Data.DirectRead(iterator.Value);
                    }
                    else if (iterator.Error == CedarResultCode.NoPath)
                    {
                        // We check if there is an implicit node.    
                        short implicitBeforeNode = _currentPage.Header.Ptr->ImplicitBeforeAllKeys;
                        if (implicitBeforeNode != CedarPageHeader.InvalidImplicitKey)
                            ptr = _currentPage.Data.DirectRead(implicitBeforeNode);
                        else
                            goto FAIL;
                    }
                    else
                        goto FAIL;

                    Debug.Assert(ptr != null);
                    Debug.Assert(!ptr->IsFree && ptr->Flags == CedarNodeFlags.Branch);

                    var nextPage = _tree.GetPage(ptr->Data);
                    Debug.Assert(nextPage.PageNumber == ptr->Data, $"Requested Page: #{ptr->Data}. Got CurrentPage: #{nextPage.PageNumber}");

                    Push(nextPage);
                }

                Debug.Assert(_currentPage.IsLeaf, "At the end of the process the current page must be a leaf");

                Key = Slices.BeforeAllKeys;
                Pointer = null;

                return true;
            }

            if (key.Options == SliceOptions.BeforeAllKeys)
            {
                Key = Slices.BeforeAllKeys;
                Pointer = null;
            }
            else if (key.Options == SliceOptions.AfterAllKeys)
            {
                Key = Slices.AfterAllKeys;
                Pointer = null;

            }
            return true;

            FAIL:
            Key = Slices.Empty;
            Pointer = null;

            return false;
        }

        public bool Seek(Slice key)
        {
            if (key.Options == SliceOptions.Key)
            {
                // Prepare the temporary output key to be reused.
                _outputKey.Reset();

                long from;
                long keyLength;

                CedarPage.IteratorValue iterator;
                while (_currentPage.IsBranch)
                {
                    from = 0;
                    keyLength = 0;
                    iterator = _currentPage.PredecessorOrEqual(key, ref _outputKey, ref from, ref keyLength);

                    // We do not have a key that matches the range.
                    CedarDataPtr* ptr;
                    if (iterator.Error == CedarResultCode.Success)
                    {
                        ptr = _currentPage.Data.DirectRead(iterator.Value);
                    }
                    else if (iterator.Error == CedarResultCode.NoPath)
                    {
                        // We check if there is an implicit node.    
                        short implicitBeforeNode = _currentPage.Header.Ptr->ImplicitBeforeAllKeys;
                        if (implicitBeforeNode != CedarPageHeader.InvalidImplicitKey)
                            ptr = _currentPage.Data.DirectRead(implicitBeforeNode);
                        else
                            goto FAIL;
                    }
                    else
                        goto FAIL;

                    Debug.Assert(ptr != null);
                    Debug.Assert(!ptr->IsFree && ptr->Flags == CedarNodeFlags.Branch);

                    var nextPage = _tree.GetPage(ptr->Data);
                    Debug.Assert(nextPage.PageNumber == ptr->Data, $"Requested Page: #{ptr->Data}. Got CurrentPage: #{nextPage.PageNumber}");

                    Push(nextPage);
                }

                Debug.Assert(_currentPage.IsLeaf, "At the end of the process the current page must be a leaf");

                from = 0;
                keyLength = 0;
                iterator = _currentPage.Successor(key, ref _outputKey, ref from, ref keyLength);

                if (iterator.Error == CedarResultCode.Success)
                {
                    _outputKey.SetSize(iterator.Length);                   

                    Key = _outputKey.Clone(_llt.Allocator);
                    Pointer = _currentPage.Data.DirectRead(iterator.Value);
                }
                else if (iterator.Error == CedarResultCode.NoPath)
                {
                    Key = Slices.AfterAllKeys;
                    Pointer = null;

                    return MoveNext();
                }
            }
            else if (key.Options == SliceOptions.BeforeAllKeys)
            {
                Key = Slices.BeforeAllKeys;
                Pointer = null;
            }
            else if (key.Options == SliceOptions.AfterAllKeys)
            {
                Key = Slices.AfterAllKeys;
                Pointer = null;

            }
            return true;

            FAIL:
            Key = Slices.Empty;
            Pointer = null;

            return false;
        }

        public void Reset()
        {
            // We are already in the root of the tree.
            if (this._path.Count == 1)
                return;

            throw new NotImplementedException();
        }

        public bool MoveNext()
        {
            if (Key.Options == SliceOptions.Key)
            {
                // Prepare the temporary output key to be reused.
                _outputKey.Reset();

                throw new NotImplementedException();
            }
            else if (Key.Options == SliceOptions.BeforeAllKeys)
            {
                // Get the first from the root.

                throw new NotImplementedException();
            }

            return false;
        }

        public bool MovePrev()
        {
            if (Key.Options == SliceOptions.Key)
            {
                // Prepare the temporary output key to be reused.
                _outputKey.Reset();

                throw new NotImplementedException();
            }
            else if (Key.Options == SliceOptions.AfterAllKeys)
            {
                // Get the last from the root.

                throw new NotImplementedException();
            }

            return false;
        }
    }
}
