using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly long[] _path;

        private CedarPage _currentPage;

        public CedarCursor(LowLevelTransaction llt, CedarPage currentPage, long[] pathFromRoot)
        {
            this._llt = llt;
            this._currentPage = currentPage;
            this._path = pathFromRoot;
        }

        public Slice Key
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public ushort NodeVersion
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int ValueSize
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public byte* Value
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        internal CedarDataPtr* Pointer
        {
            get
            {
                throw new NotImplementedException();
            }
        }


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

        public void Push(CedarPage p)
        {
            throw new NotImplementedException();
        }

        public CedarPage Pop()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
