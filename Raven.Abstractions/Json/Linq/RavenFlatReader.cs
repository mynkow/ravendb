using Raven.Abstractions.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Json.Linq
{
    public class RavenFlatHeader
    {
        public readonly int Size;
        public readonly JTokenType Type;
        public readonly VTable[] VTables;
        public readonly string[] Properties;
        public readonly IReadOnlyDictionary<string, int> PropertiesMap;

        public RavenFlatHeader(int size, JTokenType type, VTable[] vtables, Dictionary<string, int> properties)
        {
            this.Size = size;
            this.Type = type;
            this.VTables = vtables;

            this.Properties = new string[properties.Count];
            foreach (var item in properties)
                this.Properties[item.Value] = item.Key;

            this.PropertiesMap = properties;
        }

        public int GetOffset(int offset)
        {
            return Size - offset;
        }
    }



    public class RavenFlatReader : IDisposable
    {
        private readonly BinaryReader streamReader;
        private byte[] data;
        private RavenFlatHeader header;

        public RavenFlatReader(Stream stream) : this ( new BinaryReader(stream) ) { }

        public RavenFlatReader(BinaryReader reader)
        {
            this.streamReader = reader;       
        }

        public RavenJToken Read ()
        {
            var bodyOffset = ReadHeader();

            using (var memoryStream = new MemoryStream(data))
            {
                var binaryReader = new BinaryReader(memoryStream);
                memoryStream.Position = bodyOffset;

                switch ( header.Type )
                {
                    case JTokenType.Object:
                        {
                            return ReadObjectInternal(binaryReader);
                        }
                    case JTokenType.Array:
                        {
                            return ReadArrayInternal(binaryReader);
                        }
                    default: throw new NotImplementedException();
                }
            }
        }

        private RavenJArray ReadArrayInternal(BinaryReader binaryReader)
        {           
            byte arrayType = binaryReader.ReadByte();            
            if (IsArray(arrayType) && IsObject(arrayType))
            {
                // This is the case where the content are full blown objects, primitives or unknown types.
                // We are storing the data-type before any object.
                int arrayContentSize = binaryReader.ReadInt32();
                    
                int[] arrayPointers = new int[arrayContentSize];
                for (int i = 0; i < arrayContentSize; i++)
                    arrayPointers[i] = header.GetOffset( binaryReader.ReadInt32() );

                long position = binaryReader.BaseStream.Position;
                try
                {
                    var resultToken = new RavenJArray();
                    for (int i = 0; i < arrayContentSize; i++)
                    {
                        binaryReader.BaseStream.Position = arrayPointers[i];

                        byte type = binaryReader.ReadByte();

                        RavenJToken token;
                        if (IsObject(type))
                        {
                            // We handle this as an object.
                            token = ReadObjectInternal(binaryReader);
                        }
                        else if (IsArray(type))
                        {
                            // We handle this as an array of objects.
                            token = ReadArrayInternal(binaryReader);
                        }
                        else
                        {
                            byte primitiveType = (byte)(type & (byte)RavenFlatTokenMask.AsPrimitive);

                            if (IsPointer(type))
                            {
                                // This is a variable size primitive like string.
                                token = ReadVariableSizePrimitiveFromPtr((RavenFlatToken)primitiveType, binaryReader);
                            }
                            else
                            {
                                // This is a fixed size primitive.
                                token = ReadFixedSizePrimitive((RavenFlatToken)primitiveType, binaryReader);
                            }
                        }

                        resultToken.Add(token);
                    }

                    return resultToken;
                }
                finally
                {
                    binaryReader.BaseStream.Position = position;
                }             
            }
            else
            {
                // These are for special types like integer arrays that may use a different encoding. 
                if ( IsArray(arrayType) )
                    throw new NotSupportedException("Special encodings are not supported yet.");
            }
                        
            throw new NotSupportedException("The content is not a flat buffer array or it is an unsupported type");
        }

        private RavenJObject ReadObjectInternal(BinaryReader binaryReader)
        {
            var token = new RavenJObject();

            byte vtableIndex = binaryReader.ReadByte();

            VTable vtable = header.VTables[vtableIndex];
            foreach (var element in vtable.Items)
            {
                string propertyName = header.Properties[element.NameIndex];
                byte type = element.Type;

                if (IsObject(type))
                {
                    // We handle this as an object.
                    int ptr = binaryReader.ReadInt32();
                    long position = binaryReader.BaseStream.Position;
                    try
                    {
                        binaryReader.BaseStream.Position = header.GetOffset(ptr);
                        token[propertyName] = ReadObjectInternal(binaryReader);
                    }
                    finally
                    {
                        binaryReader.BaseStream.Position = position;
                    }
                }
                else if (IsArray(type))
                {
                    // We handle this as an array of objects.
                    int ptr = binaryReader.ReadInt32();
                    long position = binaryReader.BaseStream.Position;
                    try
                    {
                        binaryReader.BaseStream.Position = header.GetOffset(ptr);
                        token[propertyName] = ReadArrayInternal(binaryReader);
                    }
                    finally
                    {
                        binaryReader.BaseStream.Position = position;
                    }
                    
                }
                else
                {
                    // In either case we care about the type
                    byte primitiveType = (byte)(type & (byte)RavenFlatTokenMask.AsPrimitive);

                    if (IsPointer(type))
                    {
                        // This is a variable size primitive like string.
                        token[propertyName] = ReadVariableSizePrimitiveFromPtr((RavenFlatToken)primitiveType, binaryReader);
                    }
                    else
                    {
                        // This is a fixed size primitive.
                        token[propertyName] = ReadFixedSizePrimitive((RavenFlatToken)primitiveType, binaryReader);
                    }
                }
            }

            return token;
        }

        private RavenJValue ReadFixedSizePrimitive(RavenFlatToken type, BinaryReader binaryReader)
        {
            switch (type)
            {
                case RavenFlatToken.Boolean:
                    byte boolean = binaryReader.ReadByte();
                    return new RavenJValue(boolean == 1);
                case RavenFlatToken.Integer:
                    return new RavenJValue(binaryReader.ReadInt32());
                case RavenFlatToken.Float:
                    return new RavenJValue(BitConverter.ToSingle(binaryReader.ReadBytes(4), 0));
                case RavenFlatToken.Date:
                    return new RavenJValue(new DateTime(binaryReader.ReadInt64()));
                case RavenFlatToken.Null:
                    return null;
                default: throw new NotSupportedException("Unsupported types yet.");
            }
        }

        private RavenJValue ReadVariableSizePrimitiveFromPtr(RavenFlatToken type, BinaryReader binaryReader)
        {
            int ptr = binaryReader.ReadInt32();

            long currentPosition = binaryReader.BaseStream.Position;
            try
            {
                binaryReader.BaseStream.Position = header.GetOffset(ptr);
                return ReadVariableSizePrimitiveDirect(type, binaryReader);
               
            }
            finally
            {
                binaryReader.BaseStream.Position = currentPosition;                
            }
        }

        private RavenJValue ReadVariableSizePrimitiveDirect(RavenFlatToken type, BinaryReader binaryReader)
        {
            unsafe
            {
                switch (type)
                {
                    case RavenFlatToken.String:
                        int size = binaryReader.ReadInt32();

                        byte[] stringBytes = binaryReader.ReadBytes(size * 2);
                        fixed (byte* stringAsBytes = stringBytes)
                        {
                            string @string = new string((char*)stringAsBytes, 0, size);
                            return new RavenJValue(@string);
                        }
                    default: throw new NotSupportedException("Unsupported types yet.");
                }
            } 
        }

        private int ReadHeader()
        {
            JTokenType type;
            ushort magicNumber =  streamReader.ReadUInt16();
            if ( magicNumber == RavenFlatProtocol.PrimitiveMarker )
            {
                throw new NotImplementedException();
            }
            else if (magicNumber == RavenFlatProtocol.ObjectMarker)
            {
                type = JTokenType.Object;
            }
            else
            {
                if (magicNumber != RavenFlatProtocol.ArrayMarker)
                    throw new NotSupportedException("The data type is not supported or it is not a flat buffer");

                type = JTokenType.Array;
            }

            int size = streamReader.ReadInt32();
            
            data = new byte[size];
            int read = streamReader.Read(data, 0, size);
            if (read != size)
                throw new NotSupportedException("The data format is not supported or it is not a flat buffer");

            using (var memoryStream = new MemoryStream(data))
            {
                var binaryReader = new BinaryReader(memoryStream);

                Dictionary<string, int> propertyTable = ReadPropertyTable(binaryReader);
                VTable[] vtables = ReadVTables(binaryReader);

                header = new RavenFlatHeader(size, type, vtables, propertyTable);

                return (int) memoryStream.Position;
            }
        }

        private VTable[] ReadVTables(BinaryReader reader)
        {
            // TODO: Optimize this.
            int vtablesCount = (int)reader.ReadByte();

            VTable[] vtables = new VTable[vtablesCount];
            for (int i = 0; i < vtablesCount; i++)
            {
                var vtable = new VTable();

                int itemCount = (int)reader.ReadByte();
                VTableItem[] items = new VTableItem[itemCount];
                for ( int j = 0; j < itemCount; j++ )
                {
                    byte nameIndex = reader.ReadByte();
                    byte type = reader.ReadByte();
                    ushort size = reader.ReadUInt16();

                    vtable.Items.Add(new VTableItem( nameIndex, type, size ));
                }

                vtables[i] = vtable;
            }

            return vtables;
        }

        private Dictionary<string, int> ReadPropertyTable(BinaryReader reader)
        {
            // TODO: Optimize this.
            var result = new Dictionary<string, int>();

            unsafe
            {
                int count = (int)reader.ReadByte();
                for (int i = 0; i < count; i++)
                {
                    int stringSize = reader.ReadInt32();

                    byte[] stringBytes = reader.ReadBytes(stringSize * 2);
                    fixed (byte* stringAsBytes = stringBytes)
                    {
                        string value = new string((char*)stringAsBytes, 0, stringSize);
                        result[value] = i;
                    }
                }
            }

            return result;
        }

        private static bool IsObject(byte descriptor)
        {
            return (descriptor & (byte)RavenFlatTokenMask.IsObject) != 0;
        }

        private static bool IsArray(byte descriptor)
        {
            return (descriptor & (byte)RavenFlatTokenMask.IsArray) != 0;
        }
        private static bool IsPointer(byte descriptor)
        {
            return (descriptor & (byte)RavenFlatTokenMask.IsPtr) != 0;
        }

        public void Dispose()
        {
            
        }
    }
}
