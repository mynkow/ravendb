using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    [Config(typeof(DiffNonZeroes.Config))]
    public unsafe class DiffNonZeroes
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

        private ByteStringContext _context;
        private int size = 1024 * 1024 * 24;
        private ByteString source;
        private ByteString modified;
        private ByteString destination;

        private ScalarDiff original;
        private AvxDiff _avx;
        private SseDiff _sse;
        private NumericsDiff _numerics;

        [GlobalSetup]
        public void Setup()
        {
            _context = new ByteStringContext( SharedMultipleUseFlag.None );

            _context.Allocate(size, out source);
            _context.Allocate(size, out modified);
            _context.Allocate(size, out destination);

            var r = new Random();
            for (int i = 0; i < size; i++)
            {
                int b = r.Next();
                source.Ptr[i] = (byte)b;
                modified.Ptr[i] = (byte)b;
            }

            original = new ScalarDiff
            {
                OutputSize = size,
                Output = destination.Ptr
            };

            _avx = new AvxDiff
            {
                OutputSize = size,
                Output = destination.Ptr
            };

            _sse = new SseDiff
            {
                OutputSize = size,
                Output = destination.Ptr
            };

            _numerics = new NumericsDiff
            {
                OutputSize = size,
                Output = destination.Ptr
            };
        }

        [Benchmark]
        public void Original_Sequential()
        {
            original.ComputeDiff(source.Ptr, modified.Ptr, size);
        }

        [Benchmark]
        public void Numerics32_Sequential()
        {
            _numerics.ComputeDiff(source.Ptr, modified.Ptr, size);
        }


        [Benchmark]
        public void Numerics64_Sequential()
        {
            _numerics.ComputeDiff2(source.Ptr, modified.Ptr, size);
        }

        [Benchmark]
        public void Avx_Sequential()
        {
            _avx.ComputeDiff(source.Ptr, modified.Ptr, size);
        }

        [Benchmark]
        public void Sse_Sequential()
        {
            _sse.ComputeDiff(source.Ptr, modified.Ptr, size);
        }

        public class NumericsDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size;
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                for (long i = 0; i < len; i += 32, originalPtr += 32, modifiedPtr += 32)
                {
                    var o0 = Unsafe.Read<Vector<long>>(originalPtr);
                    var m0 = Unsafe.Read<Vector<long>>(modifiedPtr);

                    if (allZeros)
                        allZeros &= m0.Equals(Vector<long>.Zero);

                    if (!o0.Equals(m0))
                        continue;

                    if (start == i)
                    {
                        start = i + 32;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    long countCheck = allZeros ? 0 : count;
                    if (OutputSize + countCheck + sizeof(long) * 2 > size)
                        goto CopyFull;

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 32;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }

                return;

CopyFull:
                CopyFullBuffer((byte*)modifiedBuffer, size);
            }

            public void ComputeDiff2(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size;
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                for (long i = 0; i < len; i += 64, originalPtr += 64, modifiedPtr += 64)
                {                    
                    var m0 = Unsafe.Read<Vector<long>>(modifiedPtr);
                    var m1 = Unsafe.Read<Vector<long>>(modifiedPtr + 32);

                    var o0 = Unsafe.Read<Vector<long>>(originalPtr);
                    var o1 = Unsafe.Read<Vector<long>>(originalPtr + 32);

                    if (allZeros)
                        allZeros &= m0.Equals(Vector<long>.Zero) && m1.Equals(Vector<long>.Zero);

                    if (!o0.Equals(m0) || !o1.Equals(m1))
                        continue;

                    if (start == i)
                    {
                        start = i + 64;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    long countCheck = allZeros ? 0 : count;
                    if (OutputSize + countCheck + sizeof(long) * 2 > size)
                        goto CopyFull;

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 64;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }

                return;

CopyFull:
                CopyFullBuffer((byte*)modifiedBuffer, size);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffAllZeroes(long start, long count)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
                outputPtr[0] = start;
                outputPtr[1] = -count;

                OutputSize += sizeof(long) * 2;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyFullBuffer(byte* modified, int size)
            {
                // too big, no saving, just use the full modification
                OutputSize = size;
                Memory.Copy(Output, modified, size);
                IsDiff = false;
            }
        }

        public class SseDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size;
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                var zero = Sse2.SetZeroVector128<byte>();
                for (long i = 0; i < len; i += 16, originalPtr += 16, modifiedPtr += 16)
                {
                    var o0 = Sse2.LoadVector128(originalPtr);
                    var m0 = Sse2.LoadVector128(modifiedPtr);

                    if (allZeros)
                        allZeros &= Sse41.TestZ(m0, zero);

                    if (!Sse41.TestZ(o0, m0))
                        continue;

                    if (start == i)
                    {
                        start = i + 16;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    long countCheck = allZeros ? 0 : count;
                    if (OutputSize + countCheck + sizeof(long) * 2 > size)
                        goto CopyFull;

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 16;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }

                return;

CopyFull:
                CopyFullBuffer((byte*)modifiedBuffer, size);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffAllZeroes(long start, long count)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
                outputPtr[0] = start;
                outputPtr[1] = -count;

                OutputSize += sizeof(long) * 2;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyFullBuffer(byte* modified, int size)
            {
                // too big, no saving, just use the full modification
                OutputSize = size;
                Memory.Copy(Output, modified, size);
                IsDiff = false;
            }
        }

        public class AvxDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size;
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                byte* originalPtr = (byte*)originalBuffer;
                byte* modifiedPtr = (byte*)modifiedBuffer;

                var zero = Avx.SetZeroVector256<byte>();
                for (long i = 0; i < len; i += 32, originalPtr += 32, modifiedPtr += 32)
                {
                    var o0 = Avx.LoadVector256(originalPtr);
                    var m0 = Avx.LoadVector256(modifiedPtr);

                    if (allZeros)
                        allZeros &= Avx.TestZ(m0, zero);

                    if (!Avx.TestZ(o0, m0))
                        continue;

                    if (start == i)
                    {
                        start = i + 32;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    long countCheck = allZeros ? 0 : count;
                    if (OutputSize + countCheck + sizeof(long) * 2 > size)
                        goto CopyFull;

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 32;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }

                return;

CopyFull:
                CopyFullBuffer((byte*)modifiedBuffer, size);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffAllZeroes(long start, long count)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
                outputPtr[0] = start;
                outputPtr[1] = -count;

                OutputSize += sizeof(long) * 2;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyFullBuffer(byte* modified, int size)
            {
                // too big, no saving, just use the full modification
                OutputSize = size;
                Memory.Copy(Output, modified, size);
                IsDiff = false;
            }
        }        

        public unsafe class ScalarDiff
        {
            public byte* Output;
            public long OutputSize;
            public bool IsDiff { get; private set; }

            public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);

                var len = size / sizeof(long);
                IsDiff = true;

                long start = 0;
                OutputSize = 0;
                bool allZeros = true;

                // This stops the JIT from accesing originalBuffer directly, as we know
                // it is not mutable, this lowers the number of generated instructions
                long* originalPtr = (long*)originalBuffer;
                long* modifiedPtr = (long*)modifiedBuffer;

                for (long i = 0; i < len; i += 4, originalPtr += 4, modifiedPtr += 4)
                {
                    long m0 = modifiedPtr[0];
                    long o0 = originalPtr[0];

                    long m1 = modifiedPtr[1];
                    long o1 = originalPtr[1];

                    long m2 = modifiedPtr[2];
                    long o2 = originalPtr[2];

                    long m3 = modifiedPtr[3];
                    long o3 = originalPtr[3];

                    if (allZeros)
                        allZeros &= m0 == 0 && m1 == 0 && m2 == 0 && m3 == 0;

                    if (o0 != m0 || o1 != m1 || o2 != m2 || o3 != m3)
                        continue;

                    if (start == i)
                    {
                        start = i + 4;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    long countCheck = allZeros ? 0 : count;
                    if (OutputSize + countCheck + sizeof(long) * 2 > size)
                        goto CopyFull;

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 4;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }

                return;

CopyFull:
                CopyFullBuffer((byte*)modifiedBuffer, size);
            }

            public void ComputeNew(void* modifiedBuffer, int size)
            {
                Debug.Assert(size % 4096 == 0);
                Debug.Assert(size % sizeof(long) == 0);
                var len = size / sizeof(long);
                IsDiff = true;

                long start = 0;
                OutputSize = 0;

                bool allZeros = true;
                long* modifiedPtr = (long*)modifiedBuffer;

                for (long i = 0; i < len; i += 4, modifiedPtr += 4)
                {
                    long m0 = modifiedPtr[0];
                    long m1 = modifiedPtr[1];
                    long m2 = modifiedPtr[2];
                    long m3 = modifiedPtr[3];

                    if (allZeros)
                        allZeros &= m0 == 0 && m1 == 0 && m2 == 0 && m3 == 0;

                    if (0 != m0 || 0 != m1 || 0 != m2 || 0 != m3)
                        continue;

                    if (start == i)
                    {
                        start = i + 4;
                        continue;
                    }

                    long count = (i - start) * sizeof(long);

                    long countCheck = allZeros ? 0 : count;
                    if (OutputSize + countCheck + sizeof(long) * 2 > size)
                        goto CopyFull;

                    if (allZeros)
                    {
                        WriteDiffAllZeroes(start * sizeof(long), count);
                    }
                    else
                    {
                        WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                        allZeros = true;
                    }

                    start = i + 4;
                }

                if (start == len)
                    return;

                long length = (len - start) * sizeof(long);
                if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), length);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
                }

                return;

CopyFull:
                CopyFullBuffer((byte*)modifiedBuffer, size);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffNonZeroes(long start, long count, byte* modified)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long outputSize = OutputSize;
                long* outputPtr = (long*)Output + outputSize / sizeof(long);
                outputPtr[0] = start;
                outputPtr[1] = count;
                outputSize += sizeof(long) * 2;

                Memory.Copy(Output + outputSize, modified + start, count);
                OutputSize = outputSize + count;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteDiffAllZeroes(long start, long count)
            {
                Debug.Assert(count > 0);
                Debug.Assert((OutputSize % sizeof(long)) == 0);

                long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
                outputPtr[0] = start;
                outputPtr[1] = -count;

                OutputSize += sizeof(long) * 2;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyFullBuffer(byte* modified, int size)
            {
                // too big, no saving, just use the full modification
                OutputSize = size;
                Memory.Copy(Output, modified, size);
                IsDiff = false;
            }
        }
    }
}
