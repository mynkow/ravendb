using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Util;

namespace Voron
{
    public sealed class SliceComparer : IEqualityComparer<SliceArray>, IComparer<SliceArray>,
                                        IEqualityComparer<SlicePointer>, IComparer<SlicePointer>
    {
        public static readonly SliceComparer Instance = new SliceComparer();

        int IComparer<SliceArray>.Compare(SliceArray x, SliceArray y)
        {
            return CompareInline(x, y);
        }

        int IComparer<SlicePointer>.Compare(SlicePointer x, SlicePointer y)
        {
            return CompareInline(x, y);
        }

        public static int Compare<T, W>(T x, W y)
            where T : ISlice
            where W : ISlice
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline<T,W>(T x, W y)
            where T : ISlice
            where W : ISlice
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            int r, keyDiff;
            unsafe
            {
                // This pattern while it require us to write more code is extremely efficient because the
                // JIT will treat the conditions as constants when it generates the code. Therefore, the
                // only code that will survive is the intended code for the proper type. 

                if (typeof(T) == typeof(SlicePointer) && typeof(W) == typeof(SlicePointer))
                {
                    var x1 = (SlicePointer)(object)x;
                    var y1 = (SlicePointer)(object)y;

                    var size = Math.Min(x1.Size, y1.Size);
                    keyDiff = x1.Size - y1.Size;

                    // The JIT will evict the boxing because the casting in this case is idempotent.
                    r = Memory.CompareInline(x1.Value, y1.Value, size);
                }
                else if (typeof(T) == typeof(SliceArray) && typeof(W) == typeof(SliceArray))
                {
                    var x1 = (SliceArray)(object)x;
                    var y1 = (SliceArray)(object)y;

                    var size = Math.Min(x1.Size, y1.Size);
                    keyDiff = x1.Size - y1.Size;

                    // The JIT will evict the boxing because the casting in this case is idempotent.
                    fixed (byte* xt = x1.Value)
                    fixed (byte* yt = y1.Value)
                    { 
                        r = Memory.CompareInline(xt, yt, size);
                    }
                }
                else if (typeof(T) == typeof(SliceArray) && typeof(W) == typeof(SlicePointer))
                {
                    var x1 = (SliceArray)(object)x;
                    var y1 = (SlicePointer)(object)y;

                    // The JIT will evict the boxing because the casting in this case is idempotent.
                    fixed (byte* xt = x1.Value)
                    {
                        var size = Math.Min(x1.Size, y1.Size);
                        keyDiff = x1.Size - y1.Size;

                        r = Memory.CompareInline(xt, y1.Value, size);
                    }
                }
                else if (typeof(T) == typeof(SlicePointer) && typeof(W) == typeof(SliceArray))
                {
                    var x1 = (SlicePointer)(object)x;
                    var y1 = (SliceArray)(object)y;

                    // The JIT will evict the boxing because the casting in this case is idempotent.
                    fixed (byte* yt = y1.Value)
                    {
                        var size = Math.Min(x1.Size, y1.Size);
                        keyDiff = x1.Size - y1.Size;

                        r = Memory.CompareInline(x1.Value, yt, size);
                    }
                }
                else
                {
                    if (typeof(T) == typeof(ISlice))
                    {
                        if (x.GetType().GetTypeInfo().IsAssignableFrom(typeof(SliceArray).GetTypeInfo()))
                            return CompareInline((SliceArray)(object)x, y);

                        if (x.GetType().GetTypeInfo().IsAssignableFrom(typeof(SlicePointer).GetTypeInfo()))
                            return CompareInline((SlicePointer)(object)x, y);

                        throw new NotSupportedException($"The type '{typeof(T)}' is not supported.");
                    }
                    if (typeof(W) == typeof(ISlice))
                    {
                        if (y.GetType().GetTypeInfo().IsAssignableFrom(typeof(SliceArray).GetTypeInfo()))
                            return CompareInline(x, (SliceArray)(object)y);

                        if (y.GetType().GetTypeInfo().IsAssignableFrom(typeof(SlicePointer).GetTypeInfo()))
                            return CompareInline(x, (SlicePointer)(object)y);

                        throw new NotSupportedException($"The type '{typeof(W)}' is not supported.");
                    }

                    throw new NotSupportedException("The type of T or W is not supported.");
                }
            }

            if (r != 0)
                return r;

            return keyDiff;         
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IEqualityComparer<SlicePointer>.Equals(SlicePointer x, SlicePointer y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Size;
            var otherKey = y.Size;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IEqualityComparer<SliceArray>.Equals(SliceArray x, SliceArray y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Size;
            var otherKey = y.Size;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        public static bool Equals<T, W>(T x, W y)
           where T : ISlice
           where W : ISlice
        {
            return EqualsInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool EqualsInline<T, W>(T x, W y)
            where T : ISlice
            where W : ISlice
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Size;
            var otherKey = y.Size;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IEqualityComparer<SliceArray>.GetHashCode(SliceArray obj)
        {
            return obj.GetHashCode();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IEqualityComparer<SlicePointer>.GetHashCode(SlicePointer obj)
        {
            return obj.GetHashCode();
        }

        public unsafe static bool StartWith<T, W>(T value, W prefix)
            where T : class, ISlice
            where W : class, ISlice
        {
            if (prefix == null)
                return true;

            int prefixSize = prefix.Size;
            if (value == null || prefixSize > value.Size)
                return false;

            if ( typeof(T) == typeof(W) )
            {
                return StartWith<T>(value, (T)(object)prefix);
            }  
                
            if ( typeof(T) == typeof(SliceArray))
            {
                if ( typeof(W) == typeof(SlicePointer))
                {
                    byte* prefixPtr = ((SlicePointer)(object)prefix).Value;
                    fixed (byte* valuePtr = ((SliceArray)(object)value).Value)
                    {
                        return Memory.CompareInline(prefixPtr, valuePtr, prefixSize) == 0;
                    }
                }

                throw new NotSupportedException($"The type '{typeof(W)}' is not supported. ");
            }  
                
            if ( typeof(T) == typeof(SlicePointer))
            {
                if ( typeof(W) == typeof(SliceArray))
                {
                    byte* valuePtr = ((SlicePointer)(object)value).Value;
                    fixed (byte* prefixPtr = ((SliceArray)(object)prefix).Value)
                    {
                        return Memory.CompareInline(valuePtr, prefixPtr, prefixSize) == 0;
                    }
                }

                throw new NotSupportedException($"The type '{typeof(W)}' is not supported. ");
            }

            throw new NotSupportedException($"The type '{typeof(T)}' is not supported. ");
        }

        private unsafe static bool StartWith<T>(T value, T prefix)
            where T : ISlice
        {
            if (prefix.Size > value.Size)
                return false;

            if (typeof(T) == typeof(SliceArray))
            {
                fixed (byte* prefixPtr = ((SliceArray)(object)prefix).Value)
                fixed (byte* valuePtr = ((SliceArray)(object)value).Value)
                {
                    return Memory.CompareInline(prefixPtr, valuePtr, prefix.Size) == 0;
                }
            }

            if (typeof(T) == typeof(SlicePointer))
            {
                byte* prefixPtr = ((SlicePointer)(object)prefix).Value;
                byte* valuePtr = ((SlicePointer)(object)value).Value;

                return Memory.CompareInline(prefixPtr, valuePtr, prefix.Size) == 0;
            }
            else throw new NotSupportedException($"The type '{typeof(T)}' is not supported. ");
        }
    }
}
