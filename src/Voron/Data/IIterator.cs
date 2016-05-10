using System;

namespace Voron.Data
{
    public interface IIterator : IDisposable
    {
        SlicePointer CurrentKey { get; }
        SliceArray RequiredPrefix { get; set; }
        SliceArray MaxKey { get; set; }        

        bool Seek<T>(T key) where T : ISlice;
        bool MoveNext();
        bool MovePrev();
        bool Skip(int count);

        ValueReader CreateReaderForCurrent();
        int GetCurrentDataSize();

        event Action<IIterator> OnDisposal;
    }
}
