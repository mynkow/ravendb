using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The Cedar cursor is able to navigate inside all the keys on a page and also move to the next page to continue moving forward.
    /// </summary>
    public unsafe class CedarCursor : IDisposable
    {
        // TODO: This is a class now, ensure we have a local copy to avoid dealing with the list every time we need to access it        
        public class State
        {
            public readonly long PageNumber;
            public long From;
            public long KeyLength;
            public SliceOptions KeyState;

            public State(long pageNumber, int keyLength = 0, long from = 0)
            {
                this.PageNumber = pageNumber;
                this.KeyLength = keyLength;
                this.From = from;
                this.KeyState = SliceOptions.BeforeAllKeys;
            }
        }

        private readonly LowLevelTransaction _llt;
        private readonly List<State> _path;
        private readonly CedarTree _tree;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;

        private CedarPage _currentPage;

        private State _outputState;
        private Slice _outputKey;

        public CedarCursor(LowLevelTransaction llt, CedarTree tree, CedarPage currentPage, List<State> pathFromRoot)
        {
            this._llt = llt;
            this._tree = tree;
            this._currentPage = currentPage;
            this._path = new List<State>(pathFromRoot);

            _outputState = _path[_path.Count - 1];
            if (_outputState.PageNumber != currentPage.PageNumber)
                this._path.Add(new State(currentPage.PageNumber));
  
            _scope = Slice.Create(_llt.Allocator, 4096, out _outputKey);
        }

        public CedarCursor(LowLevelTransaction llt, CedarTree tree, CedarPage currentPage)
            : this(llt, tree, currentPage, new List<State>(8) { new State (currentPage.PageNumber) })
        {
        }

        public List<State> Path => _path;
        public bool IsRoot => _path.Count <= 1;

        public CedarPage CurrentPage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _currentPage; }
        }
        
        public CedarPage ParentPage
        {
            get
            {
                if (_path.Count <= 1)
                    throw new InvalidOperationException("Cannot request the ParentPage of a root node.");

                return _tree.GetPage(_path[_path.Count - 2].PageNumber);
            }
        }


        public Slice Key
        {
            get
            {
                // Be careful here as asking for the Key actually returns a mutable reference that will change 
                // when moving the cursor around.
                switch (_outputState.KeyState)
                {
                    case SliceOptions.Key:
                        return _outputKey;
                    case SliceOptions.BeforeAllKeys:
                        return Slices.BeforeAllKeys;
                    case SliceOptions.AfterAllKeys:
                        return Slices.AfterAllKeys;
                    default:
                        return Slices.Empty;
                }
            }
        }

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

        public CedarDataNode* Pointer { get; private set; }

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
                    _scope.Dispose();
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
            this._outputState = new State(page.PageNumber);
            this._path.Add(_outputState);
        }

        public CedarPage Pop()
        {
            var result = _currentPage;

            if (this._path.Count > 1)
            {
                this._outputState = _path[_path.Count - 2];
                this._currentPage = _tree.GetPage(_outputState.PageNumber);
                this._path.RemoveAt(_path.Count - 1);
            }
            else
            {
                this._currentPage = null;
                this._outputState = null;
            }

            return result;
        }

        #endregion

        public void Seek(Slice key)
        {
            _outputState.KeyState = SliceOptions.BeforeAllKeys;
            _outputState.From = 0;
            _outputState.KeyLength = 0;

            _outputKey.Reset();

            if (key.Options == SliceOptions.Key)
            {
                // Prepare the temporary output key to be reused.

                CedarPage.IteratorValue iterator;

                if (_currentPage.IsBranch)
                {
                    iterator = _currentPage.PredecessorOrEqual(key, ref _outputKey, ref _outputState.From, ref _outputState.KeyLength);

                    // We do not have a key that matches the range.
                    if (iterator.Error == CedarResultCode.Success)
                    {
                        _outputKey.SetSize(iterator.Length);
                        _outputState.KeyState = SliceOptions.Key;

                        Pointer = _currentPage.Data.DirectRead(iterator.Value);
                        Debug.Assert(!Pointer->IsFree && Pointer->Flags == CedarNodeFlags.Branch);
                    }
                    else if (iterator.Error == CedarResultCode.NoPath)
                    {
                        // We check if there is an implicit node.    
                        short implicitBeforeNode = _currentPage.Header.Ptr->ImplicitBeforeAllKeys;
                        if (implicitBeforeNode != CedarPageHeader.InvalidImplicitKey)
                        {
                            _outputKey.SetSize(0);
                            _outputState.KeyState = SliceOptions.Key;

                            Pointer = _currentPage.Data.DirectRead(implicitBeforeNode);
                            Debug.Assert(!Pointer->IsFree && Pointer->Flags == CedarNodeFlags.Branch);
                        }
                        else
                        {
                            _outputState.KeyState = SliceOptions.BeforeAllKeys;
                        }
                    }
                    else throw new InvalidOperationException("This cannot happen.");
                }
                else
                {
                    Debug.Assert(_currentPage.IsLeaf);

                    // We reset the state of the leaf node.
                    iterator = _currentPage.SuccessorOrEqual(key, ref _outputKey, ref _outputState.From, ref _outputState.KeyLength);

                    if (iterator.Error == CedarResultCode.Success)
                    {
                        _outputKey.SetSize(iterator.Length);
                        _outputState.KeyState = SliceOptions.Key;

                        Pointer = _currentPage.Data.DirectRead(iterator.Value);
                        Debug.Assert(!Pointer->IsFree && Pointer->Flags == CedarNodeFlags.Data);
                    }
                    else if (iterator.Error == CedarResultCode.NoPath)
                    {
                        _outputState.KeyState = SliceOptions.AfterAllKeys;
                        Pointer = null;
                    }
                    else throw new InvalidOperationException("This cannot happen.");
                }
            }
            else if (key.Options == SliceOptions.BeforeAllKeys)
            {
                // We check if there is an implicit node.    
                short implicitBeforeNode = _currentPage.Header.Ptr->ImplicitBeforeAllKeys;
                if (implicitBeforeNode != CedarPageHeader.InvalidImplicitKey)
                {
                    _outputKey.SetSize(0);
                    _outputState.KeyState = SliceOptions.Key;

                    Pointer = _currentPage.Data.DirectRead(implicitBeforeNode);
                    Debug.Assert(!Pointer->IsFree && Pointer->Flags == CedarNodeFlags.Branch);
                }
                else
                {
                    _outputState.KeyState = SliceOptions.BeforeAllKeys;
                    Pointer = null;
                }
            }
            else if (key.Options == SliceOptions.AfterAllKeys)
            {
                _outputState.KeyState = SliceOptions.AfterAllKeys;
                Pointer = null;
            }
        }

        public void Reset()
        {
            _outputState.KeyState = SliceOptions.BeforeAllKeys;
            _outputState.From = 0;
            _outputState.KeyLength = 0;

            _outputKey.Reset();
        }

        public bool MoveNext()
        {
            _outputKey.Reset();

            // We retrieve the state of the branch node.       
            CedarPage.IteratorValue iterator;

            switch (_outputState.KeyState)
            {
                default: // SliceOptions.Invalid, SliceOptions.AfterAllKeys
                    return false;

                case SliceOptions.BeforeAllKeys:
                    _outputState.From = 0;
                    _outputState.KeyLength = 0;
                    iterator = _currentPage.Begin(_outputKey, ref _outputState.From, ref _outputState.KeyLength);
                    break;
                    
                case SliceOptions.Key:
                    iterator = _currentPage.Next(_outputKey, ref _outputState.From, ref _outputState.KeyLength);
                    break;
            }

            if (iterator.Error == CedarResultCode.Success)
            {
                _outputState.KeyState = SliceOptions.Key;

                _outputKey.SetSize((int)_outputState.KeyLength);
                Pointer = _currentPage.Data.DirectRead(iterator.Value);
                return true;
            }

            _outputState.KeyState = SliceOptions.AfterAllKeys;
            _outputKey.SetSize(0);
            Pointer = null;

            return false;
        }

        public bool MovePrev()
        {
            _outputKey.Reset();

            // We retrieve the state of the branch node.       
            CedarPage.IteratorValue iterator;

            switch (_outputState.KeyState)
            {
                default: // SliceOptions.Invalid, SliceOptions.BeforeAllKeys
                    return false;

                case SliceOptions.AfterAllKeys:
                    _outputState.From = 0;
                    _outputState.KeyLength = 0;
                    iterator = _currentPage.End(_outputKey, ref _outputState.From, ref _outputState.KeyLength);
                    break;

                case SliceOptions.Key:
                    iterator = _currentPage.Previous(_outputKey, ref _outputState.From, ref _outputState.KeyLength);
                    break;
            }

            if (iterator.Error == CedarResultCode.Success)
            {
                _outputState.KeyState = SliceOptions.Key;

                _outputKey.SetSize((int)_outputState.KeyLength);
                Pointer = _currentPage.Data.DirectRead(iterator.Value);
                return true;
            }

            _outputState.KeyState = SliceOptions.AfterAllKeys;
            _outputKey.SetSize(0);
            Pointer = null;

            return false;
        }
    }
}
