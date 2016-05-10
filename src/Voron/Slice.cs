using Sparrow;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Voron
{
    public unsafe interface ISlice
    {
        byte this[int index] { get; }
        SliceOptions Options { get; }
        ushort Size { get; }
        bool HasValue { get; }

        void CopyTo(int from, byte* dest, int offset, int count);
        void CopyTo(byte* dest);
        void CopyTo(byte[] dest);
        void CopyTo(int from, byte[] dest, int offset, int count);

        T Clone<T>() where T : ISlice;
        T Skip<T>(ushort bytesToSkip) where T : ISlice;
    }

    // TODO: Implement a debug view for the slice.
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct SlicePointer : ISlice
    {
        public static SlicePointer AfterAllKeys;
        public static SlicePointer BeforeAllKeys;
        public static SlicePointer Empty = new SlicePointer();        

        private static readonly uint[] _lookup32 = CreateLookup32();

        private static uint[] CreateLookup32()
        {
            var result = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                string s = i.ToString("X2");
                result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
            }
            return result;
        }

        internal readonly ushort _size;
        internal readonly SliceOptions _options;
        internal readonly byte* Value;

        static SlicePointer()
        {
            Empty = new SlicePointer();
            BeforeAllKeys = new SlicePointer(SliceOptions.BeforeAllKeys);
            AfterAllKeys = new SlicePointer(SliceOptions.AfterAllKeys);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SlicePointer(SliceOptions options)
        {
            if (options == SliceOptions.Key)
                throw new ArgumentException($"{nameof(SlicePointer)} cannot be a key if no value is set.");

            Value = Empty.Value;
            _size = 0;
            _options = options;
        }

        // TODO: Probably move this to TreeNodeHeader
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SlicePointer(TreeNodeHeader* node)
        {
            Debug.Assert(node != null);

            Value = (byte*)node + Constants.NodeHeaderSize;
            _size = node->KeySize;
            _options = SliceOptions.Key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SlicePointer(void* pointer, ushort size)
        {
            Debug.Assert(pointer != null);

            Value = (byte*)pointer;
            _size = size;
            _options = SliceOptions.Key;
        }

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (Value == null)
                    throw new InvalidOperationException("Uninitialized slice!");

                return *(Value + (sizeof(byte) * index));
            }
        }

        public ushort Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _size; }
        }

        public SliceOptions Options
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _options; }
        }

        public bool HasValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _options != SliceOptions.Uninitialized; }
        }

        public override int GetHashCode()
        {
            // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
            // (less than 20 bytes) and to win big on the bigger ones. 
            //
            // After 24 bytes the gain is 10%
            // After 64 bytes the gain is 2x
            // After 128 bytes the gain is 4x.
            //
            // We should control the distribution of this over time.

            return (int)Hashing.XXHash32.CalculateInline(Value, Size);
        }

        public override string ToString()
        {
            if (Size > 0 && Value[0] == 0)
            {
                return BytePointerToHexViaLookup32(Value, Size);
            }

            var temp = new byte[Size];
            CopyTo(temp);
            return Encoding.UTF8.GetString(temp, 0, Size);
        }

        private static string BytePointerToHexViaLookup32(byte* bytes, int count)
        {
            var lookup32 = _lookup32;
            var result = new char[count * 2];
            for (int i = 0; i < count; i++)
            {
                var val = lookup32[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }
            return new string(result);
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            if (from + count > Size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

            Memory.CopyInline(dest + offset, Value + from, count);
        }

        public void CopyTo(byte* dest)
        {
            Memory.CopyInline(dest, Value, Size);
        }

        public void CopyTo(byte[] dest)
        {
            fixed (byte* p = dest)
            {
                Memory.CopyInline(p, Value, Size);
            }
        }

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            if (from + count > Size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
            if (offset + count > dest.Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer");

            fixed (byte* p = dest)
            {
                Memory.CopyInline(p + offset, Value + from, count);
            }
        }

        public T Skip<T>(ushort bytesToSkip) where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                // The JIT will evict the boxing because the casting in this case is idempotent.
                if (bytesToSkip == 0)
                    return (T)(object)new SlicePointer(Value, Size);

                return (T)(object)new SlicePointer(Value + bytesToSkip, (ushort)(Size - bytesToSkip));
            }

            if (typeof(T) == typeof(SliceArray))
            {
                throw new NotSupportedException($"Changing the type T for Skip<T> is not supported. Use T as {nameof(SlicePointer)}.");
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException("The type is not supported.");
        }

        public T Clone<T>() where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                throw new NotImplementedException();
            }

            if (typeof(T) == typeof(SliceArray))
            {
                var tmp = new byte[this._size];
                fixed( byte* tmpPtr = tmp)
                {
                    Memory.CopyInline(tmpPtr, this.Value, _size);
                }
                return (T)(object) new SliceArray(tmp);
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException("The type is not supported.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueReader CreateReader()
        {
            return new ValueReader(Value, Size);
        }
    }

    // TODO: Implement a debug view for the slice.
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct SliceArray : ISlice
    {
        public static readonly SliceArray AfterAllKeys = new SliceArray(SliceOptions.AfterAllKeys);
        public static readonly SliceArray BeforeAllKeys = new SliceArray(SliceOptions.BeforeAllKeys);
        public static readonly SliceArray Empty = new SliceArray(new byte[0]);

        private static readonly uint[] _lookup32 = CreateLookup32();

        private static uint[] CreateLookup32()
        {
            var result = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                string s = i.ToString("X2");
                result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
            }
            return result;
        }

        internal readonly byte[] Value;
        internal readonly ushort _size;
        internal readonly SliceOptions _options;

        static SliceArray()
        {            
            Empty = new SliceArray();
            BeforeAllKeys = new SliceArray(SliceOptions.BeforeAllKeys);
            AfterAllKeys = new SliceArray(SliceOptions.AfterAllKeys);
        }

        public SliceArray(SliceOptions options)
        {
            if (options == SliceOptions.Key)
                throw new ArgumentException($"{nameof(SliceArray)} cannot be a key if no value is set.");

            Value = Empty.Value;
            _size = 0;
            _options = options;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SliceArray(byte[] value)
        {
            Debug.Assert(value != null);

            this.Value = value;
            this._size = (ushort)value.Length;
            this._options = SliceOptions.Key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SliceArray(byte[] value, ushort size)
        {
            Debug.Assert(value != null);

            this.Value = value;
            this._size = size;
            this._options = SliceOptions.Key;
        }

        public SliceArray(string key) : this(Encoding.UTF8.GetBytes(key))
        { }

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Value[index]; }
        }

        public ushort Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (Value == null)
                    return 0;
                return (ushort)Value.Length;
            }
        }

        public SliceOptions Options
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _options; }
        }

        public bool HasValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _options != SliceOptions.Uninitialized; }
        }

        public override int GetHashCode()
        {
            // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
            // (less than 20 bytes) and to win big on the bigger ones. 
            //
            // After 24 bytes the gain is 10%
            // After 64 bytes the gain is 2x
            // After 128 bytes the gain is 4x.
            //
            // We should control the distribution of this over time.

            fixed (byte* arrayPtr = Value)
            {
                return (int)Hashing.XXHash32.CalculateInline(arrayPtr, _size);
            }
        }

        public static implicit operator SliceArray(string value)
        {
            return new SliceArray(Encoding.UTF8.GetBytes(value));
        }

        public override string ToString()
        {
            if (_size > 0 && Value[0] == 0)
            {
                return ByteArrayToHexViaLookup32(Value);
            }
            return Encoding.UTF8.GetString(Value, 0, _size);
        }

        private static string ByteArrayToHexViaLookup32(byte[] bytes)
        {
            var lookup32 = _lookup32;
            var result = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var val = lookup32[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }
            return new string(result);
        }

        public void CopyTo(byte* dest)
        {
            fixed (byte* a = Value)
            {
                Memory.CopyInline(dest, a, _size);
            }
        }

        public void CopyTo(byte[] dest)
        {
            Buffer.BlockCopy(Value, 0, dest, 0, _size);
        }

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            if (from + count > _size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
            if (offset + count > _size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer");

            Buffer.BlockCopy(Value, from, dest, offset, count);
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            if (from + count > _size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

            fixed (byte* p = Value)
            {
                Memory.CopyInline(dest + offset, p + from, count);
            }
        }

        public T Skip<T>(ushort bytesToSkip) where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                throw new NotSupportedException($"Changing the type T for Skip<T> is not supported. Use T as {nameof(SliceArray)}.");
            }

            if (typeof(T) == typeof(SliceArray))
            {
                int tempSize = _size - bytesToSkip;
                var temp = new byte[tempSize];
                fixed (byte* src = this.Value)
                fixed (byte* dest = temp)
                {
                    Memory.CopyInline(dest, src + bytesToSkip, tempSize);
                }

                // The JIT will evict the boxing because the casting in this case is idempotent.
                return (T)(object)new SliceArray(temp);
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException("The type is not supported.");
        }

        public T Clone<T>() where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                throw new NotImplementedException();
            }

            if (typeof(T) == typeof(SliceArray))
            {
                if (_size == 0)
                    return (T)(object)Empty;

                var tmp = new byte[_size];
                fixed ( byte* tmpPtr = tmp )
                fixed ( byte* valuePtr = Value)
                {
                    Memory.CopyInline(tmpPtr, valuePtr, _size);
                }
                return (T)(object)new SliceArray(tmp);
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        }
    }

    public static class Slices
    {
        public static T GetBeforeAllKeys<T>() where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                return (T)(object)SlicePointer.BeforeAllKeys;
            }

            if (typeof(T) == typeof(SliceArray))
            {
                return (T)(object)SliceArray.BeforeAllKeys;
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for GetBeforeAllKeys<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        }

        public static T GetAfterAllKeys<T>() where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                return (T)(object)SlicePointer.AfterAllKeys;
            }

            if (typeof(T) == typeof(SliceArray))
            {
                return (T)(object)SliceArray.AfterAllKeys;
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for GetAfterAllKeys<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        }

        public static T GetEmpty<T>() where T : ISlice
        {
            // This pattern while it require us to write more code is extremely efficient because the
            // JIT will treat the condition as constants when it generates the code. Therefore, the
            // only code that will survive is the intended code for the proper type. 
            if (typeof(T) == typeof(SlicePointer))
            {
                return (T)(object)SlicePointer.Empty;
            }

            if (typeof(T) == typeof(SliceArray))
            {
                return (T)(object)SliceArray.Empty;
            }

            if (typeof(T) == typeof(ISlice))
            {
                throw new InvalidOperationException($"The type for GetEmpty<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
            }

            throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        }
    }

    /// <summary>
    /// This is a marker class. 
    /// </summary>
    public abstract class SliceRef
    {
        public abstract ISlice GetInnerSlice();
    }

    // TODO: Implement a debug view for the slice.
    public class SliceRef<T> : SliceRef, ISlice where T : ISlice
    {
        private T _value;
        private bool _hasValue;

        public bool HasValue
        {
            get { return _hasValue; }
        }

        public T Value
        {
            get { return _value; }
        }

        public SliceRef ()
        {
            this._hasValue = false;
        }

        public SliceRef( T slice )
        {
            this._value = slice;
            this._hasValue = true;
        }

        public void Set(T value)
        {
            this._value = value;
            this._hasValue = true;
        }

        public void Clear()
        {
            this._value = default(T);
            this._hasValue = false;
        }

        public override ISlice GetInnerSlice()
        {
            if (!_hasValue)
                return null;

            return _value;            
        }

        //TODO: Implement if necessary
        public SliceOptions Options
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public ushort Size
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public byte this[int index]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public SliceRef<T> Clone ()
        {
            if (_hasValue)
                return new SliceRef<T>(_value.Clone<T>());
            else
                return new SliceRef<T>();
        }

        public unsafe void CopyTo(int from, byte* dest, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public unsafe void CopyTo(byte* dest)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(byte[] dest)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public T1 Clone<T1>() where T1 : ISlice
        {
            throw new NotImplementedException();
        }

        public T1 Skip<T1>(ushort bytesToSkip) where T1 : ISlice
        {
            throw new NotImplementedException();
        }
    }





    //public unsafe class Slice<T> where T : ISlice
    //{
    //    protected readonly T Storage;

    //    public SliceOptions Options;
    //    public ushort Size
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return Storage.Size; }
    //    }

    //    protected Slice(T storage)
    //    {
    //        this.Storage = storage;
    //    }

    //    protected Slice(SliceOptions options, T storage)
    //    {
    //        this.Options = options;
    //        this.Storage = storage;
    //    }

    //    public bool Equals(Slice other)
    //    {
    //        return Compare(other) == 0;
    //    }

    //    public override bool Equals(object obj)
    //    {
    //        if (ReferenceEquals(null, obj)) return false;
    //        if (ReferenceEquals(this, obj)) return true;
    //        if (obj.GetType() != GetType()) return false;
    //        return Equals((Slice)obj);
    //    }

    //    public override int GetHashCode()
    //    {
    //        return this.Storage.GetHashCode();
    //    }

    //    public byte this[int index]
    //    {
    //        get
    //        {
    //            if (index < 0 || index > Size)
    //                throw new ArgumentOutOfRangeException(nameof(index));

    //            return this.Storage[index];
    //        }
    //    }

    //    public override string ToString()
    //    {
    //        return Storage.ToString();
    //    }

    //    //private int CompareData(Slice other, ushort size)
    //    //{
    //    //    if (Array != null)
    //    //    {
    //    //        fixed (byte* a = Array)
    //    //        {
    //    //            if (other.Array != null)
    //    //            {
    //    //                fixed (byte* b = other.Array)
    //    //                {
    //    //                    return Memory.CompareInline(a, b, size);
    //    //                }
    //    //            }
    //    //            else return Memory.CompareInline(a, other.Pointer, size);
    //    //        }
    //    //    }

    //    //    if (other.Array != null)
    //    //    {
    //    //        fixed (byte* b = other.Array)
    //    //        {
    //    //            return Memory.CompareInline(Pointer, b, size);
    //    //        }
    //    //    }
    //    //    else return Memory.CompareInline(Pointer, other.Pointer, size);
    //    //}      

    //    public Slice ToSlice()
    //    {
    //        return new Slice(this, Size);
    //    }



    //    public Slice Clone()
    //    {
    //        throw new NotImplementedException();

    //        //var buffer = new byte[Size];
    //        //if (Array == null)
    //        //{
    //        //    fixed (byte* dest = buffer)
    //        //    {
    //        //        Memory.Copy(dest, Pointer, Size);
    //        //    }
    //        //}
    //        //else
    //        //{
    //        //    Buffer.BlockCopy(Array, 0, buffer, 0, Size);
    //        //}

    //        //return new Slice(buffer);
    //    }


    //    public int Compare(Slice other)
    //    {
    //        Debug.Assert(Options == SliceOptions.Key);
    //        Debug.Assert(other.Options == SliceOptions.Key);

    //        var srcKey = this.Size;
    //        var otherKey = other.Size;
    //        var length = srcKey <= otherKey ? srcKey : otherKey;

    //        var r = CompareData(other, length);
    //        if (r != 0)
    //            return r;

    //        return srcKey - otherKey;
    //    }

    //    public bool StartsWith(Slice other)
    //    {
    //        if (Size < other.Size)
    //            return false;

    //        return CompareData(other, other.Size) == 0;
    //    }
    //}
}