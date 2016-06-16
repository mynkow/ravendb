using Sparrow;
using Sparrow.Binary;
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
    public sealed class SliceComparer : IEqualityComparer<Slice>, IComparer<Slice>
    {
        public static readonly SliceComparer Instance = new SliceComparer();

        int IComparer<Slice>.Compare(Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare( Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var x1 = x.Content;
            var y1 = y.Content;
            if (x1 == y1) // Reference equality (specially useful on searching on collections)
                return 0;

            int r, keyDiff;
            unsafe
            {
                var size = Math.Min(x1.Length, y1.Length);
                keyDiff = x1.Length - y1.Length;

                r = Memory.CompareInline(x1.Ptr, y1.Ptr, size);
            }

            if (r != 0)
                return r;

            return keyDiff;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IEqualityComparer<Slice>.Equals(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Content.Length;
            var otherKey = y.Content.Length;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        public static bool Equals(Slice x, Slice y)
        {
            return EqualsInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool EqualsInline(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Content.Length;
            var otherKey = y.Content.Length;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IEqualityComparer<Slice>.GetHashCode(Slice obj)
        {
            return obj.GetHashCode();
        }

        public unsafe static bool StartWith(Slice value, Slice prefix)
        {
            int prefixSize = prefix.Content.Length;
            if (!value.Content.HasValue || prefixSize > value.Content.Length)
                return false;

            byte* prefixPtr = prefix.Content.Ptr;
            byte* valuePtr = value.Content.Ptr;

            return Memory.CompareInline(prefixPtr, valuePtr, prefix.Size) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static bool InBetween( Slice value, Slice bottom, Slice upper )
        {
            if (bottom.Options == SliceOptions.BeforeAllKeys && upper.Options == SliceOptions.AfterAllKeys)
                return true;

            // WARNING: DO NOT REORDER THE INSTRUCTIONS UNLESS YOU CAN PROVE BETTER PERFORMANCE. 

            // We do not copy to the stack the ByteStrings because that would introduce a write-read data hazard in the CPU pipeline
            // and slow down the retirement of the instructions (a stall in the pipeline). 
            int bottomSize = bottom.Content.Length;
            int upperSize = upper.Content.Length;
            int valueSize = value.Content.Length;

            byte* vBottomPtr = value.Content.Ptr;
            byte* vUpperPtr = value.Content.Ptr;            
            byte* bottomPtr = bottom.Content.Ptr;
            byte* upperPtr = upper.Content.Ptr;

            // We wait to calculate this to give it time for the load to finish. 
            int bInc = bottomSize != 0 ? 8 : 0;
            int uInc = upperSize != 0 ? 8 : 0;
            
            // We will move to the last element of bottom and upper (whichever is bigger)
            // But we will never be able to compare after the size of the value we are working with. 
            int l = Math.Min( Math.Max(upperSize, bottomSize), valueSize );

            // The JIT will convert l / 8 into a sar operation (shift and rotate) instead of idiv (which is costly as hell at this level of optimization). 
            for (int i = 0; i < l / 8; i++, bottomPtr += bInc, vBottomPtr += bInc, upperPtr += uInc, vUpperPtr += uInc)
            {
                if (bInc == 0 && uInc == 0)
                    return true;

                if (bInc != 0)
                {
                    if (*(ulong*)vBottomPtr != *(ulong*)bottomPtr)
                    {
                        if (*((int*)vBottomPtr) == *((int*)bottomPtr))
                        {
                            vBottomPtr += 4;
                            bottomPtr += 4;
                        }

                        if (InBetweenLastByte(ref vBottomPtr, ref bottomPtr, 4) == false)
                            return false;

                        // We are no longer incrementing the pointer.
                        bInc = 0;
                    }
                }

                if (uInc != 0)
                {
                    if (*(ulong*)vUpperPtr != *(ulong*)upperPtr)
                    {
                        if (*((int*)vUpperPtr) == *((int*)upperPtr))
                        {
                            vUpperPtr += 4;
                            upperPtr += 4;
                        }

                        if (InBetweenLastByte(ref upperPtr, ref vUpperPtr, 4) == false)
                            return false;

                        // We are no longer incrementing the pointer. 
                        uInc = 0;
                    }
                }

                if (i - 1 == bottomSize ) bInc = 0;
                if (i - 1 == upperSize) uInc = 0;                
            }

            // While probably not evident at first sight, the statement l mod 8 is correct. 
            // The rationale behind is that only 3 conditions can hold:
            // - None can be executed because the lenghts are equal and multiple of 8
            // - Only a single if is executed therefore the one executed correspond to the longest slice.
            // - Both will execute but for that to happen the remaining cannot be bigger than 8 (or it would have been consumed by the ulong loop).
            if (bottomSize != 0 )
            {
                if (bInc != 0 && InBetweenLastByte(ref vBottomPtr, ref bottomPtr, l % 8) == false)
                    return false;

                if (bottomSize > valueSize)
                    return false;
            }

            if (upperSize != 0)
            {
                if (uInc != 0 && InBetweenLastByte(ref upperPtr, ref vUpperPtr, l % 8) == false)
                    return false;

                if (upperSize < valueSize)
                    return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static bool InBetweenLastByte(ref byte* bpx, ref byte* bpy)
        {
            int last = 8;
            while (last > 0)
            {
                if (*bpx < *bpy)
                    return false;

                bpx++;
                bpy++;
                last--;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static bool InBetweenLastByte(ref byte* bpx, ref byte* bpy, int size)
        {
            int last = size;
            if ((last & 4) != 0)
            {
                if (*((int*)bpx) != *((int*)bpy))
                {
                    last = 4;
                    goto TAIL;
                }
                bpx += 4;
                bpy += 4;
            }

            if ((last & 2) != 0)
            {
                if (*((short*)bpx) != *((short*)bpy))
                {
                    last = 2;
                    goto TAIL;
                }

                bpx += 2;
                bpy += 2;
            }

            if ((last & 1) != 0)
            {
                // This is NOT(*bpx < *bpy)
                if (*bpx < *bpy)
                    return false;
            }

            return true;

            TAIL:
            while (last > 0)
            {
                if (*bpx < *bpy)
                    return false;

                bpx++;
                bpy++;
                last--;
            }

            return true;
        }
    }
}
