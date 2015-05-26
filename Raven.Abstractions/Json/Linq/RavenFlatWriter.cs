using Raven.Abstractions.Util.Buffers;
using Raven.Imports.Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Json.Linq;
using System.Runtime.InteropServices;

namespace Raven.Json.Linq
{
    public static class RavenFlatProtocol
    {
        public const short PrimitiveMarker = 0x3700;
        public const short ObjectMarker = 0x3A2A;
        public const short ArrayMarker = 0x3AF2;
    }


    [StructLayout(LayoutKind.Explicit)]
    public struct VTableItem
    {
        [FieldOffset(0)]
        public byte NameIndex;
        [FieldOffset(1)]
        public byte Type;
        [FieldOffset(2)]
        public ushort Size;

        public VTableItem(byte nameIndex, byte type, ushort size = 0)
        {
            this.NameIndex = nameIndex;
            this.Type = type;
            this.Size = size;
        }
    }

    public class VTable : IEquatable<VTable>
    {
        public readonly List<VTableItem> Items = new List<VTableItem>();

        public bool Equals(VTable other)
        {
            if (Items.Count != other.Items.Count)
                return false;

            var seq1 = Items;
            var seq2 = other.Items;

            for (int i = 0; i < seq1.Count; i++)
            {
                var item1 = seq1[i];
                var item2 = seq2[i];

                if (item1.NameIndex != item2.NameIndex || item1.Type != item2.Type || item1.Size != item2.Size)
                    return false;
            }

            return true;
        }        
    }

    public class RavenFlatWriter : IDisposable
    {
        private const int InitialSize = 64;

        private readonly BinaryWriter writer;
        private readonly List<VTable> _vtables = new List<VTable>();        
        
        private ByteBuffer _bb = new ByteBuffer(new byte[InitialSize]);
        private int _space;
        private int _minAlign = 1;
        private readonly Dictionary<string, byte> _mapPropertiesToIndex = new Dictionary<string, byte>();
        private readonly List<string> _mapProperties = new List<string>();


        public RavenFlatWriter(Stream stream) : this ( new BinaryWriter(stream, Encoding.UTF8, true) ) { }

        public RavenFlatWriter(BinaryWriter writer)
        {
            this.writer = writer;
        }

        public void Flush()
        {
            writer.Flush();
        }

        public void Dispose()
        {
            if (writer != null)
                writer.Dispose();
        }

        public void Write(RavenJToken token)
        {
            this._bb.Clear();
            this._space = this._bb.Length - this._bb.Position;

            this._vtables.Clear();
            this._mapProperties.Clear();
            this._mapPropertiesToIndex.Clear();            

            // We write everything in reverse order. 
            switch (token.Type)
            {
                case JTokenType.Object:
                    {
                        // Object: IsObject && !IsPrimitive (Bit7 ON - Bit8 OFF)                        
                        var offset = WriteObjectInternal((RavenJObject)token);
                        WriteVTables();
                        WritePropertyTable();                        
                        WriteSize();
                        WriteMagicNumberForObject();
                        break;
                    }
                case JTokenType.Array:
                    {
                        // Array: IsArray (Bit7 ON/OFF - Bit8 ON)   
                        var offset = WriteArrayInternal((RavenJArray)token);
                        WriteVTables();
                        WritePropertyTable();   
                        WriteSize();
                        WriteMagicNumberForArray();
                        break;
                    }
                default:
                    {
                        // Primitive: IsPrimitive && IsObject  (Bit7 OFF - Bit8 OFF)                        
                        VTableItem vtableEntry;
                        var offset = WritePrimitiveInternal((RavenJValue)token, null, out vtableEntry);
                        WriteMagicNumberForPrimitive();
                        break;
                    }
            }

            int startIndex = this._bb.Position;
            int dataLength = this._bb.Length - startIndex;

            this.writer.Write(this._bb.Data, startIndex, dataLength);
        }
        
        private int WriteObjectInternal(RavenJObject @object)
        {
            VTable vtable = new VTable();
            
            List<KeyValuePair<string, RavenJToken>> primitives = new List<KeyValuePair<string, RavenJToken>>();
            List<int> ptrOffsets = new List<int>();            

            // Recursively write all the complex properties in reverse order.
            foreach (var pair in @object)
            {
                var token = pair.Value;

                switch (token.Type)
                {
                    case JTokenType.Object:
                        {
                            int offset = WriteObjectInternal((RavenJObject)token);

                            VTableItem vtableEntry = CreateVTableEntry(token, pair.Key);
                            vtable.Items.Add(vtableEntry);
                            ptrOffsets.Add(offset);
                            break;
                        }
                    case JTokenType.Array:
                        {
                            int offset = WriteArrayInternal((RavenJArray)token);

                            VTableItem vtableEntry = CreateVTableEntry(token, pair.Key);
                            vtable.Items.Add(vtableEntry);
                            ptrOffsets.Add(offset);
                            break;
                        }
                    case JTokenType.String: 
                        {
                            // This is a variable size type
                            VTableItem vtableEntry = CreateVTableEntry(token, pair.Key);
                            vtable.Items.Add(vtableEntry);

                            var data = (RavenJValue)token;
                            Add((string)data.Value);
                            ptrOffsets.Add(Offset);
                            break;
                        }
                    case JTokenType.Uri:
                        {
                            throw new NotImplementedException();
                        }
                    case JTokenType.Bytes:
                        {
                            throw new NotImplementedException();
                        }
                    case JTokenType.Undefined:
                        {
                            throw new NotImplementedException();
                        }
                    default:
                        {
                            // Mark all fixed size primitives to be processed later.
                            primitives.Add(pair);
                            break;
                        }
                }
            }

            // Write all the variable size data and complex pointers. 
            PrepNoAlign(sizeof(int) * ptrOffsets.Count);
            foreach (var offset in ptrOffsets)
            {
                // Write the offset
                Put(offset);
            }

            foreach ( var pair in primitives )
            {
                var token = (RavenJValue)pair.Value;

                // Write the data and add to the VTable.
                VTableItem vtableEntry;
                WritePrimitiveInternal(token, pair.Key, out vtableEntry);
                vtable.Items.Add(vtableEntry);
            }            

            // If the VTable for this object is new, store it.
            int vtableIndex = _vtables.IndexOf(  vtable );
            if (vtableIndex == -1)
            {
                vtableIndex = _vtables.Count;
                _vtables.Add(vtable);
            }

            // Write VTable index for this object.
            PrepNoAlign(sizeof(byte));
            Put((byte)vtableIndex);

            return Offset;
        }

        private VTableItem CreateVTableEntry(RavenJToken token, string name = null)
        {
            // Lookup the index for that property name.
            byte index;

            if ( string.IsNullOrEmpty(name) )
            {
                // If we are the root or we don't care about the mapping.
                index = 0xFE;
            }
            else if (!this._mapPropertiesToIndex.TryGetValue(name, out index))
            {
                int current = this._mapProperties.Count;

                // TODO: Relax this using 7bit encoding of the size for even the experimental release.
                if (current > 0x00FE)
                    throw new NotSupportedException("The current implementation does not support a document which has more than 255 different property names. Let us know if you stumble upon this.");

                index = checked((byte)current);
                this._mapProperties.Add(name);
                this._mapPropertiesToIndex[name] = index;
            }

            switch( token.Type )
            {
                case JTokenType.Array:
                    // TODO: Find out if it is an array of primitives or an array of objects.                    
                    return new VTableItem(index, (byte)RavenFlatTokenMask.IsArray, sizeof(int));
                case JTokenType.Object:
                    return new VTableItem(index, (byte)RavenFlatTokenMask.IsObject, sizeof(int));
                case JTokenType.Boolean:
                    return new VTableItem(index, (byte)RavenFlatToken.Boolean, sizeof(byte));
                case JTokenType.Integer:
                    // TODO: Find out if this is an integer or a long.
                    return new VTableItem(index, (byte)RavenFlatToken.Integer, sizeof(long));
                case JTokenType.Float:
                    // TODO: Find out if this is a float or a double.
                    return new VTableItem(index, (byte)RavenFlatToken.Float, sizeof(float));                    
                case JTokenType.String:
                    return new VTableItem(index, (byte)RavenFlatToken.String | (byte)RavenFlatTokenMask.IsPtr, sizeof(int)); // Strings are variable size __indirect[PTR].
                case JTokenType.Date:
                    return new VTableItem(index, (byte)RavenFlatToken.Date, sizeof(long));
                case JTokenType.TimeSpan:
                    return new VTableItem(index, (byte)RavenFlatToken.TimeSpan, sizeof(long));
                case JTokenType.Guid:
                    return new VTableItem(index, (byte)RavenFlatToken.Guid, 16);
                case JTokenType.Uri:
                    return new VTableItem(index, (byte)RavenFlatToken.Uri | (byte)RavenFlatTokenMask.IsPtr, sizeof(int)); // Uris are variable size __indirect[PTR].
                case JTokenType.Bytes:
                    return new VTableItem(index, (byte)RavenFlatToken.Bytes | (byte)RavenFlatTokenMask.IsPtr, sizeof(int)); // Bytes are variable size __indirect[PTR].
                case JTokenType.Null:
                    return new VTableItem(index, (byte)RavenFlatToken.Null, sizeof(byte));
                case JTokenType.Undefined:
                    return new VTableItem(index, (byte)RavenFlatToken.Undefined | (byte)RavenFlatTokenMask.IsPtr, sizeof(int)); // Undefined are variable size __indirect[PTR].
                default:
                    throw new NotSupportedException();
            }
        }

        private int WriteArrayInternal(RavenJToken token)
        {
            var arrayToken = (RavenJArray)token;

            // TODO: We still do not support arrays of primitives without prefixed types. 

            var ptrOffsets = new List<int>();  
            foreach ( var item in arrayToken )
            {
                switch (item.Type)
                {
                    case JTokenType.Object:
                        {
                            WriteObjectInternal((RavenJObject)item);

                            PrepNoAlign(sizeof(byte));
                            Put((byte)(RavenFlatTokenMask.IsObject));
                            break;
                        }
                    case JTokenType.Array:
                        {
                            WriteArrayInternal((RavenJArray)item);
                            
                            PrepNoAlign(sizeof(byte));
                            Put((byte)(RavenFlatTokenMask.IsArray));
                            break;
                        }
                    case JTokenType.String:
                        {
                            // This is a variable size type
                            Add(item.Value<string>());

                            PrepNoAlign(sizeof(byte));
                            Put((byte)(RavenFlatToken.String));
                            break;
                        }
                    default:
                        {
                            VTableItem itemVTable;
                            WritePrimitiveInternal((RavenJValue)item, string.Empty, out itemVTable);
                            
                            PrepNoAlign(sizeof(byte));
                            Put(itemVTable.Type);                            
                            break;
                        }
                }

                ptrOffsets.Add(Offset);
            }

            // Write all the offsets governing those items.
            PrepNoAlign(sizeof(int) * (ptrOffsets.Count + 2));
            for (int i = ptrOffsets.Count - 1; i >= 0; i--)
            {
                // Write the offset in reverse order to read them in one go.
                Put(ptrOffsets[i]);
            }

            // The amount of items.
            Put(arrayToken.Length);
            Put((byte)(RavenFlatTokenMask.IsArray | RavenFlatTokenMask.IsObject));

            return Offset;
        }

        private int WritePrimitiveInternal(RavenJValue token, string name, out VTableItem vtableEntry)
        {
            vtableEntry = CreateVTableEntry(token, name);

            PrepNoAlign(sizeof(long));

            switch (token.Type)
            {
                case JTokenType.Boolean:
                    Put(token.Value<bool>());
                    break;
                case JTokenType.Integer:
                    Put((long)token.Value<long>());
                    break;
                case JTokenType.Float:
                    Put(token.Value<float>());
                    break;
                case JTokenType.Date:
                    Put(token.Value<DateTime>());
                    break;
                case JTokenType.TimeSpan:
                    throw new NotImplementedException();
                case JTokenType.Guid:
                    throw new NotImplementedException();
                case JTokenType.Uri:
                    throw new NotImplementedException();
                case JTokenType.Bytes:
                    throw new NotImplementedException();
                case JTokenType.Null:
                    break;
                case JTokenType.Undefined:
                    throw new NotImplementedException();
                default:
                    throw new NotSupportedException();
            }

            return Offset;
        }

        
        
        private void WriteMagicNumberForPrimitive()
        {
            PrepNoAlign(sizeof(ushort));
            Put((ushort)RavenFlatProtocol.PrimitiveMarker);
        }

        private void WriteMagicNumberForArray()
        {
            PrepNoAlign(sizeof(ushort));
            Put((ushort)RavenFlatProtocol.ArrayMarker);
        }

        private void WriteMagicNumberForObject()
        {
            PrepNoAlign(sizeof(ushort));
            Put((ushort)RavenFlatProtocol.ObjectMarker);
        }

        private void WriteSize()
        {
            int dataSegmentSize = Offset;

            PrepNoAlign(sizeof(int));
            Put(dataSegmentSize);
        }

        private void WritePropertyTable()
        {
            int count = this._mapProperties.Count;

            for (int i = count - 1; i >= 0; i-- )
                Add(this._mapProperties[i]);

            // TODO: Relax this using 7bit encoding of the size for even the experimental release.
            PrepNoAlign(1);
            Put((byte)count);
        }

        private void WriteVTables()
        {
            int sizeOfVTableItem = Marshal.SizeOf(typeof(VTableItem));

            checked
            {
                // We write them in reverse order to ensure that we then read the properties correctly. 
                for (int i = this._vtables.Count - 1; i >= 0; i--)
                {
                    var vtable = this._vtables[i];

                    int count = vtable.Items.Count;

                    // TODO: Relax this using 7bit encoding of the size for even the experimental release.
                    PrepNoAlign(count * sizeOfVTableItem + 1);

                    // We write them in the correct order to ensure that we then read the properties in reverse as they were written.
                    for (int vidx = 0; vidx < count; vidx++)
                    {
                        var item = vtable.Items[vidx];

                        Put(item.Size);
                        Put(item.Type);
                        Put(item.NameIndex);
                    }

                    Put((byte)count);
                }

                // TODO: Relax this using 7bit encoding of the size for even the experimental release.
                PrepNoAlign(1);
                Put((byte)this._vtables.Count);
            }
        }

        #region Utilities

        private int Offset
        {
            get { return _bb.Length - _space; }
        }

        private void Pad(int size)
        {
            for (var i = 0; i < size; i++)
                _bb.PutByte(--_space, 0);
        }

        // Doubles the size of the ByteBuffer, and copies the old data towards
        // the end of the new buffer (since we build the buffer backwards).
        private void GrowBuffer()
        {
            var oldBuf = _bb.Data;
            var oldBufSize = oldBuf.Length;
            if ((oldBufSize & 0xC0000000) != 0)
                throw new Exception( "FlatWriter: cannot grow buffer beyond 2 gigabytes.");

            var newBufSize = oldBufSize << 1;
            var newBuf = new byte[newBufSize];

            // Change this to use our routine. 
            Buffer.BlockCopy(oldBuf, 0, newBuf, newBufSize - oldBufSize, oldBufSize);

            _bb = new ByteBuffer(newBuf);
        }

        // Prepare to write an element of `size` after `additional_bytes`
        // have been written, e.g. if you write a string, you need to align
        // such the int length field is aligned to SIZEOF_INT, and the string
        // data follows it directly.
        // If all you need to do is align, `additional_bytes` will be 0.
        private void Prep(int size, int additionalBytes = 0)
        {
            // Track the biggest thing we've ever aligned to.
            if (size > _minAlign)
                _minAlign = size;

            // Find the amount of alignment needed such that `size` is properly
            // aligned after `additional_bytes`
            var alignSize =
                ((~((int)_bb.Length - _space + additionalBytes)) + 1) &
                (size - 1);

            // Reallocate the buffer if needed.
            while (_space < alignSize + size + additionalBytes)
            {
                var oldBufSize = (int)_bb.Length;
                GrowBuffer();
                _space += (int)_bb.Length - oldBufSize;

            }

            Pad(alignSize);
        }

        // Prepare to write an element of `size` after `additional_bytes`
        // have been written, e.g. if you write a string, you need to align
        // such the int length field is aligned to SIZEOF_INT, and the string
        // data follows it directly.
        // If all you need to do is align, `additional_bytes` will be 0.
        private void PrepNoAlign(int size)
        {
            // Reallocate the buffer if needed.
            while (_space < size )
            {
                var oldBufSize = (int)_bb.Length;
                GrowBuffer();
                _space += (int)_bb.Length - oldBufSize;
            }
        }

        private void Put(bool x)
        {
            _bb.PutByte(_space -= sizeof(byte), (byte)(x ? 1 : 0));
        }

        private void Put(sbyte x)
        {
            _bb.PutSbyte(_space -= sizeof(sbyte), x);
        }

        private void Put(byte x)
        {
            _bb.PutByte(_space -= sizeof(byte), x);
        }

        private void Put(short x)
        {
            _bb.PutShort(_space -= sizeof(short), x);
        }

        private void Put(ushort x)
        {
            _bb.PutUshort(_space -= sizeof(ushort), x);
        }

        private void Put(char x)
        {
            _bb.PutChar(_space -= sizeof(char), x);
        }

        private void Put(int x)
        {
            _bb.PutInt(_space -= sizeof(int), x);
        }

        private void Put(uint x)
        {
            _bb.PutUint(_space -= sizeof(uint), x);
        }

        private void Put(long x)
        {
            _bb.PutLong(_space -= sizeof(long), x);
        }

        private void Put(ulong x)
        {
            _bb.PutUlong(_space -= sizeof(ulong), x);
        }

        private void Put(float x)
        {
            _bb.PutFloat(_space -= sizeof(float), x);
        }

        private void Put(double x)
        {
            _bb.PutDouble(_space -= sizeof(double), x);
        }

        private void Put(string x)
        {
            _bb.PutString(_space -= sizeof(char) * x.Length, x);
        }

        private void Put(DateTime x)
        {
            _bb.PutLong(_space -= sizeof(long), x.Ticks);
        }

        private void Add(string x)
        {
            PrepNoAlign(sizeof(char) * x.Length + sizeof(int));

            Put(x);
            Put((int)x.Length);
        }


        #endregion


    }
}
