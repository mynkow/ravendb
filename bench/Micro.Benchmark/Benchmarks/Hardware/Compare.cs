﻿using System;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Threading;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    [Config(typeof(Compare.Config))]
    public unsafe class Compare
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job(RunMode.Default)
                {
                    Env =
                    {
                        Runtime = Runtime.Core,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    }
                });

                // Exporters for data
                Add(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                Add(RPlotExporter.Default);

                Add(StatisticColumn.AllStatistics);

                Add(BaselineValidator.FailOnError);
                Add(JitOptimizationsValidator.FailOnError);

                Add(EnvironmentAnalyser.Default);
            }
        }

        //[Params(7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1024, 2048, 4096)]
        [Params(15, 31, 63, 127, 255, 256)]
        public int KeySize { get; set; }

        public const int Operations = 1000000;

        private ByteStringContext _context;
        private int size = 1024 * 1024 * 100;
        private ByteString source;
        private ByteString destination;
        private int[] randomLocation;

        public const int VectorBytes = 32;

        [GlobalSetup]
        public void Setup()
        {
            if (Vector<byte>.Count != VectorBytes)
                throw new NotSupportedException("");

            _context = new ByteStringContext(SharedMultipleUseFlag.None);

            _context.Allocate(size, out source);
            _context.Allocate(size, out destination);

            var r = new Random();
            for (int i = 0; i < size; i++)
            {
                int b = r.Next();
                source.Ptr[i] = (byte)b;
                destination.Ptr[i] = (byte)b;
            }

            randomLocation = new int[Operations];
            int range = size - KeySize - 1;
            for (int i = 0; i < randomLocation.Length; i++)
                randomLocation[i] = r.Next(range);            
        }

        [Benchmark(Baseline = true, OperationsPerInvoke = Operations)]
        public int Original()
        {
            int r = 0;
            foreach (int index in randomLocation)
                r += CompareOriginal(source.Ptr + index, destination.Ptr + index, KeySize);

            return r;
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public int ScalarAlt()
        {
            int r = 0;
            foreach (int index in randomLocation)
                r += CompareScalarAlt(source.Ptr + index, destination.Ptr + index, KeySize);

            return r;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareScalarAlt(void* p1, void* p2, int size)
        {
            // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
            // If you know you will be comparing big memory chunks do not use the inline version.             

            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;
            long offset = bpy - bpx - sizeof(long);

            bpx += sizeof(long);

            int last;
            for (byte* end = bpx + size; bpx <= end; bpx += sizeof(long))
            {
                if (*((long*)(bpx - sizeof(long))) != *((long*)(bpx + offset)))
                {
                    last = 8;
                    goto Tail;
                }
            }

            if ((size & 4) != 0)
            {
                if (*((int*)(bpx - sizeof(long))) != *((int*)(bpx + offset)))
                {
                    last = 4;
                    goto Tail;
                }
                bpx += 4;
            }

            if ((size & 2) != 0)
            {
                if (*((short*)(bpx - sizeof(long))) != *((short*)(bpx + offset)))
                {
                    last = 2;
                    goto Tail;
                }

                bpx += 2;
            }

            if ((size & 1) != 0)
            {
                return *(bpx - sizeof(long)) - *(bpx + offset);
            }

            return 0;

            Tail:
            while (last > 0)
            {
                if (*(bpx - sizeof(long)) != *(bpx + offset))
                    return *(bpx - sizeof(long)) - *(bpx + offset);

                bpx++;
                last--;
            }

            return 0;
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public int ScalarAlt2()
        {
            int r = 0;
            foreach (int index in randomLocation)
                r += CompareScalarAlt2(source.Ptr + index, destination.Ptr + index, KeySize);

            return r;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareScalarAlt2(void* p1, void* p2, int size)
        {
            // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
            // If you know you will be comparing big memory chunks do not use the inline version.             

            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            long offset = bpy - bpx;
            if (size < 8)
                goto ProcessWord;

            int l = size >> 3; // (Equivalent to size / 8)
            for (int i = 0; i < l; i++, bpx += 8)
            {
                if (*((long*)bpx) != *((long*)(bpx + offset)))
                {
                    goto ProcessWord;
                }
            }
            goto ProcessSmall;

            ProcessWord:
            // We know that we have 8 valid bytes to process and they are different somewhere.
            // We then check if half of it is different.
            if (*((int*)bpx) == *((int*)(bpx + offset)))
                bpx += 4;

            // We reset the size to account for knowing that we are performing this test on 4 bytes only
            size = 4;

            ProcessSmall:
            if ((size & 4) != 0)
            {
                if (*((int*)bpx) == *((int*)(bpx + offset)))
                {
                    bpx += 4;
                }                
            }

            if ((size & 2) != 0)
            {
                if (*((short*)bpx) == *((short*)(bpx + offset)))
                {
                    bpx += 2;
                }                
            }

            if ((size & 1) != 0)
            {
                return *bpx - *(bpx + offset);
            }

            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareOriginal(void* p1, void* p2, int size)
        {
            // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
            // If you know you will be comparing big memory chunks do not use the inline version. 
            int l = size;

            byte* bpx = (byte*)p1, bpy = (byte*)p2;
            int last;
            for (int i = 0; i < l / 8; i++, bpx += 8, bpy += 8)
            {
                if (*((long*)bpx) != *((long*)bpy))
                {
                    last = 8;
                    goto Tail;
                }
            }

            if ((l & 4) != 0)
            {
                if (*((int*)bpx) != *((int*)bpy))
                {
                    last = 4;
                    goto Tail;
                }
                bpx += 4;
                bpy += 4;
            }

            if ((l & 2) != 0)
            {
                if (*((short*)bpx) != *((short*)bpy))
                {
                    last = 2;
                    goto Tail;
                }

                bpx += 2;
                bpy += 2;
            }

            if ((l & 1) != 0)
            {
                return (*((byte*)bpx) - *((byte*)bpy));
            }

            return 0;

            Tail:
            while (last > 0)
            {
                if (*((byte*)bpx) != *((byte*)bpy))
                    return *bpx - *bpy;

                bpx++;
                bpy++;
                last--;
            }

            return 0;
        }

        //[Benchmark(OperationsPerInvoke = Operations)]
        //public int Numerics32()
        //{
        //    int r = 0;
        //    foreach (int index in randomLocation)
        //        r += CompareNumerics(source.Ptr + index, destination.Ptr + index, KeySize);

        //    return r;
        //}

        //[Benchmark(OperationsPerInvoke = Operations)]
        //public int NumericsAlt32()
        //{
        //    int r = 0;
        //    foreach (int index in randomLocation)
        //        r += CompareNumericsAlt(source.Ptr + index, destination.Ptr + index, KeySize);

        //    return r;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareNumericsAlt(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1, bpy = (byte*)p2;

            byte* end = bpx + size;
            byte* currentEnd = end - (size & (32 - 1));
            while (bpx < currentEnd)
            {
                var vx = Unsafe.Read<Vector<byte>>(bpx);
                var vy = Unsafe.Read<Vector<byte>>(bpy);

                var xor = Vector.Xor(vx, vy);
                if (xor == Vector<byte>.Zero)
                    break;

                bpx += 32;
                bpy += 32;
            }

            currentEnd = end - (size & (sizeof(long) - 1));
            while (bpx < currentEnd)
            {
                ulong vx = ((ulong*)bpx)[0];
                ulong vy = ((ulong*)bpy)[0];
                if (vx != vy)
                    break;

                bpx += 8;
                bpy += 8;
            }

            while (bpx < end)
            {
                int r = *bpx - *bpy;
                if (r != 0)
                    return r;

                bpx += 1;
                bpy += 1;
            }

            return 0;           
        }

        //private static int CompareNumerics(void* p1, void* p2, int size)
        //{
        //    byte* bpx = (byte*)p1, bpy = (byte*)p2;

        //    // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
        //    // If you know you will be comparing big memory chunks do not use the inline version. 
        //    int l = size / VectorBytes; // This should translate into a shift operation.
        //    size -= l * VectorBytes; // This should translate into a shift operation.

        //    while (l > 0)
        //    {
        //        var vx = Unsafe.Read<Vector<byte>>(bpx);
        //        var vy = Unsafe.Read<Vector<byte>>(bpy);

        //        var xor = Vector.Xor(vx, vy);
        //        if (xor == Vector<byte>.Zero)
        //            break;

        //        l--;
        //        bpx += VectorBytes;
        //        bpy += VectorBytes;
        //    }

            
        //    if (size <= 8)
        //        goto Last;

        //    if (size > 8 && ((long*)bpx)[0] != ((long*)bpy)[0])
        //        goto Last;

        //    if (size > 16 && ((long*)bpx)[1] != ((long*)bpy)[1])
        //        goto Last;

        //    if (size > 24 && ((long*)bpx)[2] != ((long*)bpy)[2])
        //        goto Last;

        //    if (size == 32 && ((long*)bpx)[3] != ((long*)bpy)[3])
        //        goto Last;

        //    return 0;

        //    Last:

        //    size %= 8; // This should translate to a AND operation.
        //    int last = 0;

        //    while (size > 0)
        //    {
        //        int r = bpx[last] - bpy[last];
        //        if (r != 0)
        //            return r;

        //        size--;
        //        last++;
        //    }

        //    return 0;
        //}

    }
}
