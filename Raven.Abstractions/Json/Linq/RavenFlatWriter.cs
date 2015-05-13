using Raven.Abstractions.Util.Buffers;
using Raven.Imports.Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Json.Linq;

namespace Raven.Json.Linq
{
    public class RavenFlatWriter : IDisposable
    {        
        protected struct VTableItem
        {
            public byte NameIndex;
            public byte Type;            
            public int Size;

            public VTableItem( byte nameIndex, byte type, byte size = 0 )
            {
                this.NameIndex = nameIndex;
                this.Type = type;
                this.Size = size;            
            }
        }

        protected class VTable : IEquatable<VTable>
        {
            public readonly List<VTableItem> Items = new List<VTableItem>();

            public bool Equals(VTable other)
            {
                if (Items.Count != other.Items.Count)
                    return false;

                var seq1 = Items;
                var seq2 = other.Items;

                for ( int i = 0; i < seq1.Count; i++ )
                {
                    var item1 = seq1[i];
                    var item2 = seq2[i];

                    if (item1.NameIndex != item2.NameIndex || item1.Type != item2.Type || item1.Size != item2.Size)
                        return false;
                }

                return true;
            }
        }

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
                        WriteObjectVTables();
                        WritePropertyTable();                        
                        WriteSize();
                        WriteMagicNumberForObject();
                        break;
                    }
                case JTokenType.Array:
                    {
                        // Array: IsArray (Bit7 ON/OFF - Bit8 ON)   
                        VTableItem vtableEntry;
                        var offset = WriteArrayInternal((RavenJArray)token, null, out vtableEntry);
                        WriteArrayVTables(vtableEntry);
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
        }

        private int WriteObjectInternal(RavenJObject @object)
        {
            VTable vtable = new VTable();
            List<KeyValuePair<string, RavenJToken>> primitives = new List<KeyValuePair<string, RavenJToken>>();
            List<int> complexOffsets = new List<int>();            

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
                            complexOffsets.Add(offset);
                            break;
                        }
                    case JTokenType.Array:
                        {
                            VTableItem vtableEntry;
                            int offset = WriteArrayInternal((RavenJArray)token, pair.Key, out vtableEntry);
                            
                            vtable.Items.Add(vtableEntry);
                            complexOffsets.Add(offset);
                            break;
                        }
                    default:
                        {
                            // Mark all primitives to be processed later.
                            primitives.Add(pair);
                            break;
                        }
                }
            }

            Prep(sizeof(int) * complexOffsets.Count, 0);
            foreach ( var offset in complexOffsets )
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
            Add((byte)vtableIndex);

            return Offset;
        }

        private VTableItem CreateVTableEntry(RavenJToken token, string name = null)
        {
            // Lookup the index for that property name.
            byte index;

            if ( name == null )
            {
                // If we are the root, we dont care about the mapping.
                index = 0xFE;
            }
            else if (!this._mapPropertiesToIndex.TryGetValue(name, out index))
            {
                int current = this._mapProperties.Count;
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
                    return new VTableItem(index, (byte)RavenFlatToken.Integer, sizeof(int));
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

        private int WriteArrayInternal(RavenJToken token, string name, out VTableItem vtableEntry)
        {
            throw new NotImplementedException();
        }

        private int WritePrimitiveInternal(RavenJToken token, string name, out VTableItem vtableEntry)
        {
            throw new NotImplementedException();
        }

        
        private void WriteMagicNumberForPrimitive()
        {
            Prep(sizeof(byte));
            Put((byte)0x37);
        }

        private void WriteMagicNumberForArray()
        {
            Prep(sizeof(byte) * 2);
            Put((byte)0x2A);
            Put((byte)0x3A);
        }

        private void WriteMagicNumberForObject()
        {
            Prep(sizeof(byte) * 2);
            Put((byte)0x2A);
            Put((byte)0xA3);
        }

        private void WriteSize()
        {
            throw new NotImplementedException();
        }

        private void WritePropertyTable()
        {
            throw new NotImplementedException();
        }

        private void WriteArrayVTables(VTableItem rootVTable)
        {
            throw new NotImplementedException();
        }

        private void WriteObjectVTables()
        {
            throw new NotImplementedException();
        }



        private static bool IsObject(byte descriptor)
        {
            throw new NotImplementedException();
        }

        private static bool IsArray(byte descriptor)
        {
            throw new NotImplementedException();
        }

        private static bool IsPrimitive(byte descriptor)
        {
            throw new NotImplementedException();
        }

        private static bool IsComplex(byte descriptor)
        {
            throw new NotImplementedException();
        }

        private static bool IsVariableSize(byte descriptor)
        {
            throw new NotImplementedException();
        }

        private static bool IsFixedSize(byte descriptor)
        {
            throw new NotImplementedException();
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

        // Adds a scalar to the buffer, properly aligned, and the buffer grown
        // if needed.
        private void Add(bool x) { Prep(sizeof(byte), 0); Put(x); }
        private void Add(sbyte x) { Prep(sizeof(sbyte), 0); Put(x); }
        private void Add(byte x) { Prep(sizeof(byte), 0); Put(x); }
        private void Add(short x) { Prep(sizeof(short), 0); Put(x); }
        private void Add(ushort x) { Prep(sizeof(ushort), 0); Put(x); }
        private void Add(int x) { Prep(sizeof(int), 0); Put(x); }
        private void Add(uint x) { Prep(sizeof(uint), 0); Put(x); }
        private void Add(long x) { Prep(sizeof(long), 0); Put(x); }
        private void Add(ulong x) { Prep(sizeof(ulong), 0); Put(x); }
        private void Add(float x) { Prep(sizeof(float), 0); Put(x); }
        private void Add(double x)
        {
            Prep(sizeof(double), 0);
            Put(x);
        }


        #endregion


    }
}
