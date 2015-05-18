using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util.Buffers
{

     /*
     * Copyright 2014 Google Inc. All rights reserved.
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    /// <summary>
    /// Attribution: Derivative work from FlatBuffers
    /// </summary>

    public class ByteBuffer
    {
        private readonly byte[] _buffer;

        private int _pos;  // Must track start of the buffer.

        public int Length { get { return _buffer.Length; } }

        public byte[] Data { get { return _buffer; } }

        public int Position
        {
            get { return _pos; }
        }

        public ByteBuffer(byte[] buffer)
        {
            _buffer = buffer;
            _pos = 0;
        }


        // Pre-allocated helper arrays for conversion.
        private float[] floathelper = new[] { 0.0f };
        private int[] inthelper = new[] { 0 };
        private double[] doublehelper = new[] { 0.0 };
        private ulong[] ulonghelper = new[] { 0UL };

        // Helper functions for the unsafe version.
        static public ushort ReverseBytes(ushort input)
        {
            return (ushort)(((input & 0x00FFU) << 8) |
                            ((input & 0xFF00U) >> 8));
        }

        static public uint ReverseBytes(uint input)
        {
            return ((input & 0x000000FFU) << 24) |
                   ((input & 0x0000FF00U) << 8) |
                   ((input & 0x00FF0000U) >> 8) |
                   ((input & 0xFF000000U) >> 24);
        }

        static public ulong ReverseBytes(ulong input)
        {
            return (((input & 0x00000000000000FFUL) << 56) |
                    ((input & 0x000000000000FF00UL) << 40) |
                    ((input & 0x0000000000FF0000UL) << 24) |
                    ((input & 0x00000000FF000000UL) << 8) |
                    ((input & 0x000000FF00000000UL) >> 8) |
                    ((input & 0x0000FF0000000000UL) >> 24) |
                    ((input & 0x00FF000000000000UL) >> 40) |
                    ((input & 0xFF00000000000000UL) >> 56));
        }

        private void AssertOffsetAndLength(int offset, int length)
        {
            if (offset < 0 ||
                offset >= _buffer.Length ||
                offset + length > _buffer.Length)
                throw new ArgumentOutOfRangeException();
        }

        public void PutSbyte(int offset, sbyte value)
        {
            AssertOffsetAndLength(offset, sizeof(sbyte));
            _buffer[offset] = (byte)value;
            _pos = offset;
        }

        public void PutByte(int offset, byte value)
        {
            AssertOffsetAndLength(offset, sizeof(byte));
            _buffer[offset] = value;
            _pos = offset;
        }

        // Unsafe but more efficient versions of Put*.
        public void PutShort(int offset, short value)
        {
            PutUshort(offset, (ushort)value);
        }

        public unsafe void PutUshort(int offset, ushort value)
        {
            AssertOffsetAndLength(offset, sizeof(ushort));
            fixed (byte* ptr = _buffer)
            {
                *(ushort*)(ptr + offset) = BitConverter.IsLittleEndian
                    ? value
                    : ReverseBytes(value);
            }
            _pos = offset;
        }

        public void PutInt(int offset, int value)
        {
            PutUint(offset, (uint)value);
        }

        public unsafe void PutUint(int offset, uint value)
        {
            AssertOffsetAndLength(offset, sizeof(uint));
            fixed (byte* ptr = _buffer)
            {
                *(uint*)(ptr + offset) = BitConverter.IsLittleEndian
                    ? value
                    : ReverseBytes(value);
            }
            _pos = offset;
        }

        public unsafe void PutChar(int offset, char value)
        {
            AssertOffsetAndLength(offset, sizeof(char));
            fixed (byte* ptr = _buffer)
            {
                *(ushort*)(ptr + offset) = BitConverter.IsLittleEndian
                    ? (ushort)value
                    : ReverseBytes((ushort)value);
            }
            _pos = offset;
        }

        public unsafe void PutLong(int offset, long value)
        {
            PutUlong(offset, (ulong)value);
        }

        public unsafe void PutUlong(int offset, ulong value)
        {
            AssertOffsetAndLength(offset, sizeof(ulong));

            fixed (byte* ptr = _buffer)
            {
                *(ulong*)(ptr + offset) = BitConverter.IsLittleEndian
                    ? value
                    : ReverseBytes(value);
            }
            _pos = offset;
        }

        public unsafe void PutFloat(int offset, float value)
        {
            AssertOffsetAndLength(offset, sizeof(float));
            fixed (byte* ptr = _buffer)
            {
                if (BitConverter.IsLittleEndian)
                {
                    *(float*)(ptr + offset) = value;
                }
                else
                {
                    *(uint*)(ptr + offset) = ReverseBytes(*(uint*)(&value));
                }
            }
            _pos = offset;
        }

        public unsafe void PutDouble(int offset, double value)
        {
            AssertOffsetAndLength(offset, sizeof(double));
            fixed (byte* ptr = _buffer)
            {
                if (BitConverter.IsLittleEndian)
                {
                    *(double*)(ptr + offset) = value;

                }
                else
                {
                    *(ulong*)(ptr + offset) = ReverseBytes(*(ulong*)(ptr + offset));
                }
            }
            _pos = offset;
        }

        public unsafe void PutString(int offset, string value)
        {
            AssertOffsetAndLength(offset, sizeof(char) * value.Length);

            fixed (byte* ptr = _buffer)
            fixed (char* charPtr = value)
            {
                char* ptrWithOffset = (char*) (ptr + offset);
                for (int i = value.Length - 1; i >= 0; i--)
                {
                    *(ptrWithOffset + i) = BitConverter.IsLittleEndian
                                        ? charPtr[i]
                                        : (char)ReverseBytes((ushort)charPtr[i]);

                }
            }

            _pos = offset;
        }

        public sbyte GetSbyte(int index)
        {
            AssertOffsetAndLength(index, sizeof(sbyte));
            return (sbyte)_buffer[index];
        }

        public byte Get(int index)
        {
            AssertOffsetAndLength(index, sizeof(byte));
            return _buffer[index];
        }

        // Unsafe but more efficient versions of Get*.
        public short GetShort(int offset)
        {
            return (short)GetUshort(offset);
        }

        public unsafe ushort GetUshort(int offset)
        {
            AssertOffsetAndLength(offset, sizeof(ushort));
            fixed (byte* ptr = _buffer)
            {
                return BitConverter.IsLittleEndian
                    ? *(ushort*)(ptr + offset)
                    : ReverseBytes(*(ushort*)(ptr + offset));
            }
        }

        public int GetInt(int offset)
        {
            return (int)GetUint(offset);
        }

        public unsafe uint GetUint(int offset)
        {
            AssertOffsetAndLength(offset, sizeof(uint));
            fixed (byte* ptr = _buffer)
            {
                return BitConverter.IsLittleEndian
                    ? *(uint*)(ptr + offset)
                    : ReverseBytes(*(uint*)(ptr + offset));
            }
        }

        public long GetLong(int offset)
        {
            return (long)GetUlong(offset);
        }

        public unsafe ulong GetUlong(int offset)
        {
            AssertOffsetAndLength(offset, sizeof(ulong));
            fixed (byte* ptr = _buffer)
            {
                return BitConverter.IsLittleEndian
                    ? *(ulong*)(ptr + offset)
                    : ReverseBytes(*(ulong*)(ptr + offset));
            }
        }

        public unsafe float GetFloat(int offset)
        {
            AssertOffsetAndLength(offset, sizeof(float));
            fixed (byte* ptr = _buffer)
            {
                if (BitConverter.IsLittleEndian)
                {
                    return *(float*)(ptr + offset);
                }
                else
                {
                    uint uvalue = ReverseBytes(*(uint*)(ptr + offset));
                    return *(float*)(&uvalue);
                }
            }
        }

        public unsafe double GetDouble(int offset)
        {
            AssertOffsetAndLength(offset, sizeof(double));
            fixed (byte* ptr = _buffer)
            {
                if (BitConverter.IsLittleEndian)
                {
                    return *(double*)(ptr + offset);
                }
                else
                {
                    ulong uvalue = ReverseBytes(*(ulong*)(ptr + offset));
                    return *(double*)(&uvalue);
                }
            }
        }

        public void Clear()
        {
            _pos = 0;
        }


    }
}
