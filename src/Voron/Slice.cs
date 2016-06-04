using Sparrow;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Data.BTrees;
using Voron.Impl;
using System.IO;

namespace Voron
{
    //public unsafe interface ISlice
    //{
    //    byte this[int index] { get; }
    //    SliceOptions Options { get; }
    //    ushort Size { get; }
    //    bool HasValue { get; }

    //    void CopyTo(int from, byte* dest, int offset, int count);
    //    void CopyTo(byte* dest);
    //    void CopyTo(byte[] dest);
    //    void CopyTo(int from, byte[] dest, int offset, int count);

    //    T Clone<T>() where T : class, ISlice;
    //    T Skip<T>(ushort bytesToSkip) where T : class, ISlice;
    //}

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct Slice
    {
        public ByteString Content;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Slice(SliceOptions options, ByteString content)
        {
            content.SetUserDefinedFlags((ByteStringType)options);

            this.Content = content;            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Slice(ByteString content)
        {
            this.Content = content;
        }

        public bool HasValue
        {
            get { return Content.HasValue; }
        }

        public ushort Size
        {
            get
            {
                Debug.Assert(Content.Length >= 0 && Content.Length <= ushort.MaxValue);
                return (ushort) Content.Length;
            }
        }

        public SliceOptions Options
        {
            get
            {
                return (SliceOptions) (Content.Flags & ByteStringType.UserDefinedMask);
            }
        }

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Content.Ptr != null, "Uninitialized slice!");

                if (!Content.HasValue)
                    throw new InvalidOperationException("Uninitialized slice!");

                return *(Content.Ptr + (sizeof(byte) * index));
            }
        }

        public Slice Clone()
        {
            throw new NotImplementedException();
        }

        public Slice Skip()
        {
            throw new NotImplementedException();
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(byte* dest)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Slice From(ByteStringContext context, string value, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.From(value, type));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Slice From(ByteStringContext context, byte[] value, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.From(value, type));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Slice From(ByteStringContext context, byte* value, int size, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.From(value, size, type));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Slice External(ByteStringContext context, byte* value, int size, ByteStringType type = ByteStringType.Mutable | ByteStringType.External)
        {
            return new Slice(context.FromPtr(value, size, type | ByteStringType.External));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release ( ByteStringContext context )
        {
            context.Release(ref Content);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueReader CreateReader()
        {
            return new ValueReader(Content.Ptr, Size);
        }

    }

    //// TODO: Implement a debug view for the slice.
    //[StructLayout(LayoutKind.Sequential)]
    //public unsafe class SlicePointer : ISlice
    //{
    //    public static SlicePointer AfterAllKeys;
    //    public static SlicePointer BeforeAllKeys;
    //    public static SlicePointer Empty = new SlicePointer();        

    //    private static readonly uint[] _lookup32 = CreateLookup32();

    //    private static uint[] CreateLookup32()
    //    {
    //        var result = new uint[256];
    //        for (int i = 0; i < 256; i++)
    //        {
    //            string s = i.ToString("X2");
    //            result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
    //        }
    //        return result;
    //    }

    //    internal ushort _size;
    //    internal SliceOptions _options;
    //    internal byte* Value;

    //    static SlicePointer()
    //    {
    //        Empty = new SlicePointer();
    //        BeforeAllKeys = new SlicePointer(SliceOptions.BeforeAllKeys);
    //        AfterAllKeys = new SlicePointer(SliceOptions.AfterAllKeys);
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SlicePointer()
    //    {
    //        _options = SliceOptions.Uninitialized;
    //        _size = 0;          
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SlicePointer(SliceOptions options)
    //    {
    //        if (options == SliceOptions.Key)
    //            throw new ArgumentException($"{nameof(SlicePointer)} cannot be a key if no value is set.");

    //        Value = Empty.Value;
    //        _size = 0;
    //        _options = options;
    //    }

    //    // TODO: Probably move this to TreeNodeHeader
    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SlicePointer(TreeNodeHeader* node)
    //    {
    //        Debug.Assert(node != null);

    //        Value = (byte*)node + Constants.NodeHeaderSize;
    //        _size = node->KeySize;
    //        _options = SliceOptions.Key;
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SlicePointer(void* pointer, ushort size)
    //    {
    //        Debug.Assert(pointer != null);

    //        Value = (byte*)pointer;
    //        _size = size;
    //        _options = SliceOptions.Key;
    //    }

    //    public byte this[int index]
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get
    //        {
    //            if (Value == null)
    //                throw new InvalidOperationException("Uninitialized slice!");

    //            return *(Value + (sizeof(byte) * index));
    //        }
    //    }

    //    public ushort Size
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return _size; }
    //    }

    //    public SliceOptions Options
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return _options; }
    //    }

    //    public bool HasValue
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return _options != SliceOptions.Uninitialized; }
    //    }

    //    public override int GetHashCode()
    //    {
    //        // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
    //        // (less than 20 bytes) and to win big on the bigger ones. 
    //        //
    //        // After 24 bytes the gain is 10%
    //        // After 64 bytes the gain is 2x
    //        // After 128 bytes the gain is 4x.
    //        //
    //        // We should control the distribution of this over time.

    //        return (int)Hashing.XXHash32.CalculateInline(Value, Size);
    //    }

    //    public override string ToString()
    //    {
    //        if (Size > 0 && Value[0] == 0)
    //        {
    //            return BytePointerToHexViaLookup32(Value, Size);
    //        }

    //        var temp = new byte[Size];
    //        CopyTo(temp);
    //        return Encoding.UTF8.GetString(temp, 0, Size);
    //    }

    //    private static string BytePointerToHexViaLookup32(byte* bytes, int count)
    //    {
    //        var lookup32 = _lookup32;
    //        var result = new char[count * 2];
    //        for (int i = 0; i < count; i++)
    //        {
    //            var val = lookup32[bytes[i]];
    //            result[2 * i] = (char)val;
    //            result[2 * i + 1] = (char)(val >> 16);
    //        }
    //        return new string(result);
    //    }

    //    public void CopyTo(int from, byte* dest, int offset, int count)
    //    {
    //        if (from + count > Size)
    //            throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

    //        Memory.CopyInline(dest + offset, Value + from, count);
    //    }

    //    public void CopyTo(byte* dest)
    //    {
    //        Memory.CopyInline(dest, Value, Size);
    //    }

    //    public void CopyTo(byte[] dest)
    //    {
    //        fixed (byte* p = dest)
    //        {
    //            Memory.CopyInline(p, Value, Size);
    //        }
    //    }

    //    public void CopyTo(int from, byte[] dest, int offset, int count)
    //    {
    //        if (from + count > Size)
    //            throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
    //        if (offset + count > dest.Length)
    //            throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer");

    //        fixed (byte* p = dest)
    //        {
    //            Memory.CopyInline(p + offset, Value + from, count);
    //        }
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public T Skip<T>(ushort bytesToSkip) where T : class, ISlice
    //    {
    //        // This pattern while it require us to write more code is extremely efficient because the
    //        // JIT will treat the condition as constants when it generates the code. Therefore, the
    //        // only code that will survive is the intended code for the proper type. 
    //        if (typeof(T) == typeof(SlicePointer))
    //        {
    //            // The JIT will evict the boxing because the casting in this case is idempotent.
    //            if (bytesToSkip == 0)
    //                return (T)(object)new SlicePointer(Value, Size);

    //            return (T)(object)new SlicePointer(Value + bytesToSkip, (ushort)(Size - bytesToSkip));
    //        }

    //        if (typeof(T) == typeof(SliceArray))
    //        {
    //            throw new NotSupportedException($"Changing the type T for Skip<T> is not supported. Use T as {nameof(SlicePointer)}.");
    //        }

    //        if (typeof(T) == typeof(ISlice))
    //        {
    //            throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
    //        }

    //        throw new NotSupportedException("The type is not supported.");
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public T Clone<T>() where T : class, ISlice
    //    {
    //        // This pattern while it require us to write more code is extremely efficient because the
    //        // JIT will treat the condition as constants when it generates the code. Therefore, the
    //        // only code that will survive is the intended code for the proper type. 
    //        if (typeof(T) == typeof(SlicePointer))
    //        {
    //            throw new NotImplementedException();
    //        }

    //        if (typeof(T) == typeof(SliceArray))
    //        {
    //            var tmp = new byte[this._size];
    //            fixed( byte* tmpPtr = tmp)
    //            {
    //                Memory.CopyInline(tmpPtr, this.Value, _size);
    //            }
    //            return (T)(object) new SliceArray(tmp);
    //        }

    //        if (typeof(T) == typeof(ISlice))
    //        {
    //            throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
    //        }

    //        throw new NotSupportedException("The type is not supported.");
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public ValueReader CreateReader()
    //    {
    //        return new ValueReader(Value, Size);
    //    }
    //}

    //// TODO: Implement a debug view for the slice.
    //[StructLayout(LayoutKind.Sequential)]
    //public unsafe class SliceArray : ISlice
    //{
    //    public static readonly SliceArray AfterAllKeys;
    //    public static readonly SliceArray BeforeAllKeys;
    //    public static readonly SliceArray Empty;

    //    private static readonly uint[] _lookup32 = CreateLookup32();

    //    private static uint[] CreateLookup32()
    //    {
    //        var result = new uint[256];
    //        for (int i = 0; i < 256; i++)
    //        {
    //            string s = i.ToString("X2");
    //            result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
    //        }
    //        return result;
    //    }

    //    internal readonly byte[] Value;
    //    internal readonly ushort _size;
    //    internal readonly SliceOptions _options;

    //    static SliceArray()
    //    {            
    //        Empty = new SliceArray(new byte[0]);
    //        BeforeAllKeys = new SliceArray(SliceOptions.BeforeAllKeys);
    //        AfterAllKeys = new SliceArray(SliceOptions.AfterAllKeys);
    //    }

    //    public SliceArray()
    //    {
    //        _size = 0;
    //        _options = SliceOptions.Uninitialized;        
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SliceArray(SliceOptions options)
    //    {
    //        Debug.Assert(options != SliceOptions.Key);

    //        Value = Empty.Value;
    //        _size = 0;
    //        _options = options;
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SliceArray(byte[] value)
    //    {
    //        Debug.Assert(value != null);

    //        this.Value = value;
    //        this._size = (ushort)value.Length;
    //        this._options = SliceOptions.Key;
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SliceArray(byte[] value, ushort size)
    //    {
    //        Debug.Assert(value != null);

    //        this.Value = value;
    //        this._size = size;
    //        this._options = SliceOptions.Key;
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public SliceArray(string key) : this(Encoding.UTF8.GetBytes(key))
    //    { }

    //    public byte this[int index]
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return Value[index]; }
    //    }

    //    public ushort Size
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get
    //        {
    //            if (Value == null)
    //                return 0;
    //            return (ushort)Value.Length;
    //        }
    //    }

    //    public SliceOptions Options
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return _options; }
    //    }

    //    public bool HasValue
    //    {
    //        [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //        get { return _options != SliceOptions.Uninitialized; }
    //    }

    //    public override int GetHashCode()
    //    {
    //        // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
    //        // (less than 20 bytes) and to win big on the bigger ones. 
    //        //
    //        // After 24 bytes the gain is 10%
    //        // After 64 bytes the gain is 2x
    //        // After 128 bytes the gain is 4x.
    //        //
    //        // We should control the distribution of this over time.

    //        fixed (byte* arrayPtr = Value)
    //        {
    //            return (int)Hashing.XXHash32.CalculateInline(arrayPtr, _size);
    //        }
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public static implicit operator SliceArray(string value)
    //    {
    //        return new SliceArray(Encoding.UTF8.GetBytes(value));
    //    }

    //    public override string ToString()
    //    {
    //        if (_size > 0 && Value[0] == 0)
    //        {
    //            return ByteArrayToHexViaLookup32(Value);
    //        }
    //        return Encoding.UTF8.GetString(Value, 0, _size);
    //    }

    //    private static string ByteArrayToHexViaLookup32(byte[] bytes)
    //    {
    //        var lookup32 = _lookup32;
    //        var result = new char[bytes.Length * 2];
    //        for (int i = 0; i < bytes.Length; i++)
    //        {
    //            var val = lookup32[bytes[i]];
    //            result[2 * i] = (char)val;
    //            result[2 * i + 1] = (char)(val >> 16);
    //        }
    //        return new string(result);
    //    }

    //    public void CopyTo(byte* dest)
    //    {
    //        fixed (byte* a = Value)
    //        {
    //            Memory.CopyInline(dest, a, _size);
    //        }
    //    }

    //    public void CopyTo(byte[] dest)
    //    {
    //        Buffer.BlockCopy(Value, 0, dest, 0, _size);
    //    }

    //    public void CopyTo(int from, byte[] dest, int offset, int count)
    //    {
    //        if (from + count > _size)
    //            throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
    //        if (offset + count > _size)
    //            throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer");

    //        Buffer.BlockCopy(Value, from, dest, offset, count);
    //    }

    //    public void CopyTo(int from, byte* dest, int offset, int count)
    //    {
    //        if (from + count > _size)
    //            throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

    //        fixed (byte* p = Value)
    //        {
    //            Memory.CopyInline(dest + offset, p + from, count);
    //        }
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public T Skip<T>(ushort bytesToSkip) where T : class, ISlice
    //    {
    //        // This pattern while it require us to write more code is extremely efficient because the
    //        // JIT will treat the condition as constants when it generates the code. Therefore, the
    //        // only code that will survive is the intended code for the proper type. 
    //        if (typeof(T) == typeof(SlicePointer))
    //        {
    //            throw new NotSupportedException($"Changing the type T for Skip<T> is not supported. Use T as {nameof(SliceArray)}.");
    //        }

    //        if (typeof(T) == typeof(SliceArray))
    //        {
    //            int tempSize = _size - bytesToSkip;
    //            var temp = new byte[tempSize];
    //            fixed (byte* src = this.Value)
    //            fixed (byte* dest = temp)
    //            {
    //                Memory.CopyInline(dest, src + bytesToSkip, tempSize);
    //            }

    //            // The JIT will evict the boxing because the casting in this case is idempotent.
    //            return (T)(object)new SliceArray(temp);
    //        }

    //        if (typeof(T) == typeof(ISlice))
    //        {
    //            throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
    //        }

    //        throw new NotSupportedException("The type is not supported.");
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public T Clone<T>() where T : class, ISlice
    //    {
    //        // This pattern while it require us to write more code is extremely efficient because the
    //        // JIT will treat the condition as constants when it generates the code. Therefore, the
    //        // only code that will survive is the intended code for the proper type. 
    //        if (typeof(T) == typeof(SlicePointer))
    //        {
    //            throw new NotImplementedException();
    //        }

    //        if (typeof(T) == typeof(SliceArray))
    //        {
    //            if (_size == 0)
    //                return (T)(object)Empty;

    //            var tmp = new byte[_size];
    //            fixed ( byte* tmpPtr = tmp )
    //            fixed ( byte* valuePtr = Value)
    //            {
    //                Memory.CopyInline(tmpPtr, valuePtr, _size);
    //            }
    //            return (T)(object)new SliceArray(tmp);
    //        }

    //        if (typeof(T) == typeof(ISlice))
    //        {
    //            throw new InvalidOperationException($"The type for Skip<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
    //        }

    //        throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
    //    }
    //}

    public static class Slices
    {
        private static readonly ByteStringContext _sharedSliceContent = new ByteStringContext();

        public static readonly Slice AfterAllKeys;
        public static readonly Slice BeforeAllKeys;
        public static readonly Slice Empty;

        static Slices()
        {
            Empty = new Slice(SliceOptions.Key, _sharedSliceContent.From(string.Empty));
            BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys, _sharedSliceContent.From(string.Empty));
            AfterAllKeys = new Slice(SliceOptions.AfterAllKeys, _sharedSliceContent.From(string.Empty));
        }


        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static T GetBeforeAllKeys<T>() where T : class, ISlice
        //{
        //    // This pattern while it require us to write more code is extremely efficient because the
        //    // JIT will treat the condition as constants when it generates the code. Therefore, the
        //    // only code that will survive is the intended code for the proper type. 
        //    if (typeof(T) == typeof(SlicePointer))
        //    {
        //        return (T)(object)SlicePointer.BeforeAllKeys;
        //    }

        //    if (typeof(T) == typeof(SliceArray))
        //    {
        //        return (T)(object)SliceArray.BeforeAllKeys;
        //    }

        //    if (typeof(T) == typeof(ISlice))
        //    {
        //        throw new InvalidOperationException($"The type for GetBeforeAllKeys<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
        //    }

        //    throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        //}

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static T GetAfterAllKeys<T>() where T : class, ISlice
        //{
        //    // This pattern while it require us to write more code is extremely efficient because the
        //    // JIT will treat the condition as constants when it generates the code. Therefore, the
        //    // only code that will survive is the intended code for the proper type. 
        //    if (typeof(T) == typeof(SlicePointer))
        //    {
        //        return (T)(object)SlicePointer.AfterAllKeys;
        //    }

        //    if (typeof(T) == typeof(SliceArray))
        //    {
        //        return (T)(object)SliceArray.AfterAllKeys;
        //    }

        //    if (typeof(T) == typeof(ISlice))
        //    {
        //        throw new InvalidOperationException($"The type for GetAfterAllKeys<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
        //    }

        //    throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        //}

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static T GetEmpty<T>() where T : class, ISlice
        //{
        //    // This pattern while it require us to write more code is extremely efficient because the
        //    // JIT will treat the condition as constants when it generates the code. Therefore, the
        //    // only code that will survive is the intended code for the proper type. 
        //    if (typeof(T) == typeof(SlicePointer))
        //    {
        //        return (T)(object)SlicePointer.Empty;
        //    }

        //    if (typeof(T) == typeof(SliceArray))
        //    {
        //        return (T)(object)SliceArray.Empty;
        //    }

        //    if (typeof(T) == typeof(ISlice))
        //    {
        //        throw new InvalidOperationException($"The type for GetEmpty<T> must be a concrete type. Change the {nameof(ISlice)} interface with the proper concrete type you need.");
        //    }

        //    throw new NotSupportedException($"The type '{nameof(T)}' is not supported.");
        //}
    }
}