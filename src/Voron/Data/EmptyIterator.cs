using System;
using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Voron.Data
{
    public unsafe class EmptyIterator : IIterator
    {
        public bool Seek<T>(T key) where T : class, ISlice
        {
            return false;
        }

        public SlicePointer CurrentKey
        {
            get { throw new InvalidOperationException("No current page"); }
        }

        public int GetCurrentDataSize()
        {
            throw new InvalidOperationException("No current page");
        }

        public bool Skip(int count)
        {
            throw new InvalidOperationException("No records");
        }

        public ValueReader CreateReaderForCurrent()
        {
            throw new InvalidOperationException("No current page");
        }


        public event Action<IIterator> OnDisposal;

        public IEnumerable<string> DumpValues()
        {
            yield break;
        }

        public unsafe TreeNodeHeader* Current
        {
            get
            {
                throw new InvalidOperationException("No current page");
            }
        }

        public SliceArray MaxKey { get; set; }

        public SliceArray RequiredPrefix
        {
            get;
            set;
        }

        public bool MoveNext()
        {
            return false;
        }

        public bool MovePrev()
        {
            return false;
        }

        public void Dispose()
        {
            var action = OnDisposal;
            if (action != null)
                action(this);
        }
    }
}
