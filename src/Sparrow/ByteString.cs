#define VALIDATE

using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow
{
    [Flags]
    public enum ByteStringType : byte
    {
        Immutable = 0x00, // This is a shorthand for an internal-immutable string. 
        Mutable = 0x01,
        External = 0x80,        
    }

    // We use sequential to ensure that validation build is handled apropriately when we implement ByteStringExternalStorage. (DO NOT CHANGE)
    [StructLayout(LayoutKind.Sequential)] 
    unsafe struct ByteStringStorage
    {
        /// <summary>
        /// This is the pointer to the start of the byte stream. 
        /// </summary>
        public byte* Ptr;

        /// <summary>
        /// The actual type for the byte string
        /// </summary>
        public ByteStringType Flags;

#if VALIDATE

        public const ulong NullKey = unchecked((ulong)-1);

        /// <summary>
        /// The validation key for the storage value.
        /// </summary>
        public ulong Key;
#endif

        /// <summary>
        /// The actual length of the byte string
        /// </summary>
        public int Length;

        /// <summary>
        /// This is the total storage size for this byte string. Length will always be smaller than Size - 1.
        /// </summary>
        public int Size;
    }

    public unsafe struct ByteString
    {
        internal ByteStringStorage* _pointer;

#if VALIDATE
        internal ByteString(ByteStringStorage* ptr)
        {
            this._pointer = ptr;
            this.Key = ptr->Key; // We store the storage key
        }

        internal readonly ulong Key;
#else
        internal ByteString(ByteStringStorage* ptr)
        {
            this._pointer = ptr;
        }
#endif
        public ByteStringType Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return _pointer->Flags;
            }
        }

        public bool IsMutable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return (_pointer->Flags & ByteStringType.Mutable) != 0;
            }
        }

        public bool IsExternal
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return (_pointer->Flags & ByteStringType.External) != 0;
            }
        }

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_pointer == null)
                    return 0;

                EnsureIsNotBadPointer();

                return _pointer->Length;
            }
        }

        public bool HasValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pointer != null; }
        }

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return *(_pointer->Ptr + (sizeof(byte) * index));
            }
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            Debug.Assert(HasValue);

            if (from + count > _pointer->Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

            EnsureIsNotBadPointer();
            Memory.CopyInline(dest + offset, _pointer->Ptr + from, count);
        }

        public void CopyTo(byte* dest)
        {
            Debug.Assert(HasValue);

            EnsureIsNotBadPointer();
            Memory.CopyInline(dest, _pointer->Ptr, _pointer->Length);
        }

        public void CopyTo(byte[] dest)
        { 
            Debug.Assert(HasValue);

            EnsureIsNotBadPointer();
            fixed (byte* p = dest)
            {
                Memory.CopyInline(p, _pointer->Ptr, _pointer->Length);
            }
        }

#if VALIDATE

        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointer()
        {
            if (_pointer->Ptr == null)
                throw new InvalidOperationException("The inner storage pointer is not initialized. This is a defect on the implementation of the ByteStringContext class");

            if (_pointer->Key == ByteStringStorage.NullKey)
                throw new InvalidOperationException("The memory referenced has already being released. This is a dangling pointer. Check your .Release() statements and aliases in the calling code.");

            if ( this.Key != _pointer->Key)
            {
                if (this.Key >> 16 != _pointer->Key >> 16)
                    throw new InvalidOperationException("The owner context for the ByteString and the unmanaged storage are different. This is a defect on the implementation of the ByteStringContext class");

                Debug.Assert((this.Key & 0x0000000FFFFFFFF) != (_pointer->Key & 0x0000000FFFFFFFF));
                throw new InvalidOperationException("The key for the ByteString and the unmanaged storage are different. This is a dangling pointer. Check your .Release() statements and aliases in the calling code.");                                    
            }
        }

#else
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointer() { }
#endif

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            Debug.Assert(HasValue);

            if (from + count > _pointer->Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
            if (offset + count > dest.Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer");

            EnsureIsNotBadPointer();
            fixed (byte* p = dest)
            {
                Memory.CopyInline(p + offset, _pointer->Ptr + from, count);
            }
        }

        public override string ToString()
        {
            if (!HasValue)
                return string.Empty;

            EnsureIsNotBadPointer();

            return new string((char*)_pointer->Ptr, 0, _pointer->Length);
        }
    }




    public unsafe class ByteStringContext : IDisposable
    {
        class SegmentInformation
        {
            public bool CanDispose; 

            public byte* Start;
            public byte* Current;
            public byte* End;

            public int Size
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (int)(End - Start); }
            }

            public int SizeLeft
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (int)(End - Current); }
            }
        }

        public const int MinBlockSizeInBytes = 64 * 1024; // If this is changed, we need to change also LogMinBlockSize.
        private const int LogMinBlockSize = 16;

        public const int DefaultAllocationBlockSizeInBytes = 2 * MinBlockSizeInBytes;
        public const int MinReusableBlockSizeInBytes = 8;

        private readonly int _allocationBlockSize;
        private readonly int[] _reusableStringPoolCount;
        private readonly Stack<IntPtr>[] _reusableStringPool;

        /// <summary>
        /// This list keeps all the segments already instantiated in order to release them after context finalization. 
        /// </summary>
        private readonly List<SegmentInformation> _wholeSegments;

        /// <summary>
        /// This list keeps the hot segments released for use. It is important to note that we will never put into this list
        /// a segment with less space than the MinBlockSize value.
        /// </summary>
        private readonly List<SegmentInformation> _readyToUseMemorySegments;

        private SegmentInformation _current;

        public ByteStringContext(int allocationBlockSize = DefaultAllocationBlockSizeInBytes)
        {
            if (allocationBlockSize < MinBlockSizeInBytes)
                throw new ArgumentException($"It is not a good idea to allocate chunks of less than the {nameof(MinBlockSizeInBytes)} value of {MinBlockSizeInBytes}");

            this._allocationBlockSize = allocationBlockSize;

            this._wholeSegments = new List<SegmentInformation>();
            this._readyToUseMemorySegments = new List<SegmentInformation>();            

            this._reusableStringPool = new Stack<IntPtr>[LogMinBlockSize];
            this._reusableStringPoolCount = new int[LogMinBlockSize];

            this._current = AllocateSegment(allocationBlockSize);

            PrepareForValidation();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteString Allocate(int length)
        {
            return AllocateInternal(length, ByteStringType.Mutable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetPoolIndexFromChunkSize(int size)
        {
            return Bits.CeilLog2(size) - 1; // x^0 = 1 therefore we start counting at 1 instead.
        }

        private ByteString AllocateInternal(int length, ByteStringType type)
        {
            Debug.Assert((type & ByteStringType.External) == 0, "This allocation routine is only for use with internal storage byte strings.");
            type &= ~ByteStringType.External; // We are allocating internal, so we will force it (even if we are checking for it in debug).

            int allocationSize = length + sizeof(ByteStringStorage);

            // This is even bigger than the configured allocation block size. There is no reason why we shouldnt
            // allocate it directly. When released (if released) this will be reused as a segment, ensuring that the context
            // could handle that.
            if (allocationSize > _allocationBlockSize)
                return AllocateWholeSegment(length, type); // We will pass the lenght because this is a whole allocated segment able to hold a length size ByteString.

            int reusablePoolIndex = GetPoolIndexFromChunkSize(allocationSize); 
            int allocationUnit = Bits.NextPowerOf2(allocationSize);

            // The allocation unit is bigger than MinBlockSize (therefore it wont be 2^n aligned).
            // Then we will 64bits align the allocation.
            if (allocationUnit > MinBlockSizeInBytes)
                allocationUnit += sizeof(long) - allocationUnit % sizeof(long);

            // All allocation units are 32 bits aligned. If not we will have a performance issue.
            Debug.Assert(allocationUnit % sizeof(int) == 0);    

            // If we can reuse... we retrieve those.
            if (allocationSize <= MinBlockSizeInBytes && _reusableStringPoolCount[reusablePoolIndex] != 0)
            {
                // This is a stack because hotter memory will be on top. 
                Stack<IntPtr> pool = _reusableStringPool[reusablePoolIndex];

                _reusableStringPoolCount[reusablePoolIndex]--;
                void* ptr = pool.Pop().ToPointer();

                return Create(ptr, length, allocationUnit, type);
            }
            else
            {
                int currentSizeLeft = _current.SizeLeft;
                if (allocationUnit > currentSizeLeft) // This shouldnt happen that much, if it does you should increse your default allocation block. 
                {                   
                    SegmentInformation segment = null;
                    
                    // We will try to find a hot segment with enough space if available.
                    // Older (colder) segments are at the front of the list. That's why we would start scanning backwards.
                    for ( int i = _readyToUseMemorySegments.Count - 1; i >= 0; i--)
                    {
                        var segmentValue = _readyToUseMemorySegments[i];
                        if (segmentValue.SizeLeft >= allocationUnit)
                        {
                            // Put the last where this one is (if it is the same, this is a no-op) and remove it from the list.
                            _readyToUseMemorySegments[i] = _readyToUseMemorySegments[_readyToUseMemorySegments.Count - 1];
                            _readyToUseMemorySegments.RemoveAt(_readyToUseMemorySegments.Count - 1);

                            segment = segmentValue;
                            break;
                        }                            
                    }

                    // If the size left is bigger than MinBlockSize, we release current as a reusable segment
                    if (currentSizeLeft > MinBlockSizeInBytes)
                    {
                        byte* start = _current.Current;
                        byte* end = start + currentSizeLeft;

                        _readyToUseMemorySegments.Add(new SegmentInformation{ Start = start, Current = start, End = end, CanDispose = false });
                    }
                    else if ( currentSizeLeft > sizeof(ByteStringType) + MinReusableBlockSizeInBytes)
                    {
                        // The memory chunk left is big enough to make sense to reuse it.
                        reusablePoolIndex = GetPoolIndexFromChunkSize(currentSizeLeft);

                        Stack<IntPtr> pool = this._reusableStringPool[reusablePoolIndex];
                        if (pool == null)
                        {
                            pool = new Stack<IntPtr>();
                            this._reusableStringPool[reusablePoolIndex] = pool;
                        }

                        pool.Push(new IntPtr(_current.Current));
                        this._reusableStringPoolCount[reusablePoolIndex]++;
                    }

                    // Use the segment and if there is no segment available that matches the request, just get a new one.
                    this._current = segment ?? AllocateSegment(_allocationBlockSize);
                }                    

                var byteString = Create(_current.Current, length, allocationUnit, type);                
                _current.Current += byteString._pointer->Size;                

                return byteString;
            }
        }


        private ByteString Create(void* ptr, int length, int size, ByteStringType type = ByteStringType.Immutable)
        {
            Debug.Assert(length <= size - sizeof(ByteStringStorage));

            var basePtr = (ByteStringStorage*)ptr;
            basePtr->Flags = type;
            basePtr->Length = length;
            basePtr->Size = size;
            basePtr->Ptr = (byte*)ptr + sizeof(ByteStringStorage);

            RegisterForValidation(basePtr);

            return new ByteString(basePtr);
        }

        private ByteString AllocateWholeSegment(int length, ByteStringType type)
        {
            // The allocation is big, therefore we will just allocate the segment and move on.                
            var segment = AllocateSegment(length + sizeof(ByteStringStorage));

            var byteString = Create(segment.Current, length, segment.Size, type);
            segment.Current += byteString._pointer->Size;
            _wholeSegments.Add(segment);

            return byteString;
        }

        public void Release(ref ByteString value)
        {
            Debug.Assert(value._pointer != null, "Pointer cannot be null. You have a defect in your code.");
            if (value._pointer == null)
                return;

            // We are releasing, therefore we should validate among other things if an immutable string changed and if we are the owners.
            Validate(value);

            int reusablePoolIndex = GetPoolIndexFromChunkSize(value._pointer->Size);

            if (value._pointer->Size <= MinBlockSizeInBytes)
            {
                Stack<IntPtr> pool = this._reusableStringPool[reusablePoolIndex];
                if (pool == null)
                {
                    pool = new Stack<IntPtr>();
                    this._reusableStringPool[reusablePoolIndex] = pool;
                }

                pool.Push(new IntPtr(value._pointer));
                this._reusableStringPoolCount[reusablePoolIndex]++;
            }
            else  // The released memory is big enough, we will just release it as a new segment. 
            {
                byte* start = (byte*)value._pointer;
                byte* end = start + value._pointer->Size;

                var segment = new SegmentInformation { Start = start, Current = start, End = end, CanDispose = false };
                _readyToUseMemorySegments.Add(segment);
            }

#if VALIDATE
            // Setting the null key ensures that in between we can validate that no further deallocation
            // happens on this memory segment.
            value._pointer->Key = ByteStringStorage.NullKey;
#endif

            // WE WANT it to happen, no matter what. 
            value._pointer = null;
        }

        private SegmentInformation AllocateSegment(int size)
        {
            byte* start = (byte*)Marshal.AllocHGlobal(size).ToPointer();
            byte* end = start + size;

            var segment = new SegmentInformation { Start = start, Current = start, End = end, CanDispose = true };
            _wholeSegments.Add(segment);

            return segment;
        }

        public ByteString Skip(ByteString value, int bytesToSkip, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value._pointer != null, "ByteString cant be null.");

            if (bytesToSkip < 0)
                throw new ArgumentException($"'{nameof(bytesToSkip)}' cannot be smaller than 0.");

            if (bytesToSkip > value.Length)
                throw new ArgumentException($"'{nameof(bytesToSkip)}' cannot be bigger than '{nameof(value)}.Length' 0.");

            // TODO: If origin and destination are immutable, we can create external references.

            int size = value.Length - bytesToSkip;
            var result = AllocateInternal(size, type);
            Memory.CopyInline(result._pointer->Ptr, value._pointer->Ptr + bytesToSkip, size);

            RegisterForValidation(result);
            return result;
        }

        public ByteString Clone(ByteString value, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value._pointer != null, "ByteString cant be null.");

            // TODO: If origin and destination are immutable, we can create external references.

            var result = AllocateInternal(value.Length, type);
            Memory.CopyInline(result._pointer->Ptr, value._pointer->Ptr, value._pointer->Length);

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(string value, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value != null, "string cant be null.");

            byte[] utf8 = Encoding.UTF8.GetBytes(value);

            var result = AllocateInternal(utf8.Length, type);
            fixed (byte* ptr = utf8)
            {
                Memory.Copy(result._pointer->Ptr, ptr, utf8.Length);
            }

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(string value, Encoding encoding, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value != null, "string cant be null.");

            byte[] encodedBytes = encoding.GetBytes(value);

            var result = AllocateInternal(encodedBytes.Length, type);
            fixed (byte* ptr = encodedBytes)
            {
                Memory.Copy(result._pointer->Ptr, ptr, encodedBytes.Length);
            }

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(byte[] value, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value != null, "array cant be null.");

            var result = AllocateInternal(value.Length, type);
            fixed (byte* ptr = value)
            {
                Memory.Copy(result._pointer->Ptr, ptr, value.Length);
            }

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(int value, ByteStringType type = ByteStringType.Mutable)
        {
            var result = AllocateInternal(sizeof(int), type);
            ((int*)result._pointer->Ptr)[0] = value;

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(long value, ByteStringType type = ByteStringType.Mutable)
        {
            var result = AllocateInternal(sizeof(long), type);
            ((long*)result._pointer->Ptr)[0] = value;

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(short value, ByteStringType type = ByteStringType.Mutable)
        {
            var result = AllocateInternal(sizeof(short), type);
            ((short*)result._pointer->Ptr)[0] = value;

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(byte value, ByteStringType type = ByteStringType.Mutable)
        {
            var result = AllocateInternal(1, type);
            result._pointer->Ptr[0] = value;

            RegisterForValidation(result);
            return result;
        }

        public ByteString From(byte* valuePtr, int size, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(valuePtr != null, "array cant be null.");
            Debug.Assert((type & ByteStringType.External) == 0, "we cannot support yet external requests, so we will essentially create an internal ByteString");

            var result = AllocateInternal(size, type);
            Memory.Copy(result._pointer->Ptr, valuePtr, size);

            RegisterForValidation(result);
            return result;
        }

#if VALIDATE

        private static int globalContextId;
        private int _allocationCount;

        protected int ContextId;

        private void PrepareForValidation()
        {
            this.ContextId = Interlocked.Increment(ref globalContextId);
        }

        private void RegisterForValidation(ByteStringStorage* storage)
        {
            // There shouldnt be reuse for the storage, unless we have a different allocation on reused memory.
            // Therefore, monotonically increasing the key we ensure that we can check when we have dangling pointers in our code.
            // We use interlocked in order to avoid validation bugs when validating (we are playing it safe).
            storage->Key = (ulong)(((long)this.ContextId << 32) + Interlocked.Increment(ref _allocationCount)); ;
        }

        private void RegisterForValidation(ByteString value)
        {
            value.EnsureIsNotBadPointer();

            if (!value.IsMutable)
                throw new NotImplementedException("Validation still not implemented for immutable Byte Strings");
        }

        private void Validate(ByteString value)
        {
            value.EnsureIsNotBadPointer();

            if (value._pointer->Key == ByteStringStorage.NullKey)
                throw new InvalidOperationException("Trying to release an alias of an already removed object. You have a dangling pointer in hand.");

            if (value._pointer->Key >> 32 != (ulong)this.ContextId)
                throw new InvalidOperationException("The owner of the ByteString is a different context. You are mixing contexts, which has undefined behavior.");

            if (!value.IsMutable)
                throw new NotImplementedException("Validation still not implemented for immutable Byte Strings");

        }

#else
        [Conditional("VALIDATE")]
        private void PrepareForValidation() { }

        [Conditional("VALIDATE")]
        private void RegisterForValidation(ByteStringStorage* _) { }

        [Conditional("VALIDATE")]
        private void RegisterForValidation(ByteString _) { }

        [Conditional("VALIDATE")]
        private void Validate(ByteString _) { }

#endif

        #region IDisposable

        private bool isDisposed = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                foreach (var segment in _wholeSegments)
                {
                    if ( segment.CanDispose)
                        Marshal.FreeHGlobal(new IntPtr(segment.Start));
                }

                _wholeSegments.Clear();
                _readyToUseMemorySegments.Clear();

                isDisposed = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
         ~ByteStringContext()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
#endregion
    }
}
