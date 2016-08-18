using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Sparrow;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The iteration allow us to iterate and seek over a Cedar Tree.
    /// </summary>
    public unsafe class CedarTreeIterator : IIterator
    {
        private readonly CedarTree _tree;
        private readonly LowLevelTransaction _tx;
        private readonly bool _prefetch;

        public event Action<IIterator> OnDisposal;

        private CedarCursor _cursor;        
        private Slice _currentKey = default(Slice);

        public CedarTreeIterator(CedarTree tree, LowLevelTransaction tx, bool prefetch)
        {
            _tree = tree;
            _tx = tx;
            _prefetch = prefetch;
        }

        public bool Seek(Slice key)
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(CedarTreeIterator)} {_tree.Name}");

            // We look for the leaf page that is going to host this data. 
            _cursor = _tree.FindPageFor(key);
            _cursor.Seek(key);
            
            // Returning an AfterAllKeys on a cursor means that the current node doesnt contains the key and we seek along the branch path for the entire tree.
            if (_cursor.Key.Same(Slices.AfterAllKeys))
            {
                // The key is not found in the db, but we are Seek()ing for equals or starts with.
                // We know that the exact value isn't there, but it is possible that the next page has values 
                // that is actually greater than the key, so we need to check it as well.
                return MoveNext();
            }

            if (_cursor.Key.Same(Slices.BeforeAllKeys))
                return true;

            _currentKey = _cursor.Key;

            if (DoRequireValidation)
                return this.ValidateCurrentKey(_cursor.Key);

            return true;
        }

        public bool MoveNext()
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(CedarTreeIterator)} {_tree.Name}");

            if (_cursor.CurrentPage.IsLeaf)
            {
                if (_cursor.MoveNext())
                {
                    _currentKey = _cursor.Key;
                    return true;
                }                    

                // This is the root as leaf. There are no more records available.
                if (_cursor.PageDepth == 1) 
                    return false;
            }

            // Now we need to backtrack. 
            _cursor.Pop();
            while (_cursor.CurrentPage.IsBranch)
            {
                if (_cursor.MoveNext())
                {
                    // We found an open branch, go straight to it.
                    Debug.Assert(_cursor.Pointer != null && !_cursor.Pointer->IsFree);
                    Debug.Assert(_cursor.Pointer->Flags == CedarNodeFlags.Branch);

                    var childPage = _tree.GetPage(_cursor.Pointer->PageNumber);
                    _cursor.Push(childPage);

                    if (childPage.IsLeaf)
                    {
                        _cursor.Seek(Slices.BeforeAllKeys);
                        _cursor.MoveNext();

                        _currentKey = _cursor.Key;

                        Debug.Assert(_cursor.Pointer != null && !_cursor.Pointer->IsFree);
                        Debug.Assert(_cursor.Pointer->Flags == CedarNodeFlags.Data);

                        return true;
                    }

                    continue;
                }

                // We have no more capability to move up the tree. 
                if (_cursor.PageDepth == 1)
                    break;

                _cursor.Pop();
            }

            return false;
        }

        public bool MovePrev()
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(CedarTreeIterator)} {_tree.Name}");
           
            throw new NotImplementedException();
        }


        public bool Skip(int count)
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(CedarTreeIterator)} {_tree.Name}");

            if (count != 0)
            {
                var moveMethod = (count > 0) ? (Func<bool>)MoveNext : MovePrev;

                for (int i = 0; i < Math.Abs(count); i++)
                {
                    if (!moveMethod())
                        break;
                }
            }

            if (DoRequireValidation)
                return _cursor != null && this.ValidateCurrentKey(_cursor.Key);

            return _cursor != null;
        }


        public Slice CurrentKey
        {
            get
            {
                if (_disposed)
                    throw new ObjectDisposedException($"{nameof(CedarTreeIterator)} {_tree.Name}");

                return _currentKey;
            }
        }

        private bool _requireValidation;
        public bool DoRequireValidation => _requireValidation;

        private Slice _requiredPrefix;
        public Slice RequiredPrefix
        {
            get { return _requiredPrefix; }
            set
            {
                _requiredPrefix = value;
                _requireValidation = _maxKey.HasValue || _requiredPrefix.HasValue;
            }
        }

        private Slice _maxKey;
        public Slice MaxKey
        {
            get { return _maxKey; }
            set
            {
                _maxKey = value;
                _requireValidation = _maxKey.HasValue || _requiredPrefix.HasValue;
            }
        }


        public ValueReader CreateReaderForCurrent()
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(CedarTreeIterator)} {_tree.Name}");

            throw new NotImplementedException();
        }

        #region IDisposable Support

        private bool _disposed = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _cursor?.Dispose();
                _disposed = true;

                OnDisposal?.Invoke(this);
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
