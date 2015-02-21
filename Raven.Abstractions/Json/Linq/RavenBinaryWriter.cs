using Raven.Imports.Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Raven.Json.Linq
{
    public class RavenBinaryWriter
    {
        private readonly BinaryWriter writer;

        private List<string> properties;
        private Dictionary<string, byte> propertiesLookup;

        private MemoryStream bodyStream;
        private BinaryWriter bodyWriter;

        public RavenBinaryWriter(Stream stream) : this ( new BinaryWriter(stream) ) { }

        public RavenBinaryWriter(BinaryWriter writer)
        {
            this.writer = writer;
        }

        public void WriteStartBody()
        {
            this.properties = new List<string>();
            this.propertiesLookup = new Dictionary<string, byte>();
            this.bodyStream = new MemoryStream();
            this.bodyWriter = new BinaryWriter(bodyStream);
        }

        public void Write(RavenJToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Object:
                    {
                        WriteObject((RavenJObject)token);
                        break;
                    }
                case JTokenType.Array:
                    {
                        WriteArray((RavenJArray)token);
                        break;
                    }
                default:
                    {
                        WritePrimitive((RavenJValue)token);
                        break;
                    }
            }
        }

        public void WriteEndBody()
        {   
            // We ensure that the writer finished writing on the memory stream.
            bodyWriter.Flush();
            
            writer.Write((byte)RavenBinaryToken.HeaderStart);
            {
                short count = (short)properties.Count;
                writer.Write(count);

                for (short i = 0; i < count; i++)
                {
                    char[] propertyName = properties[i].ToCharArray();

                    writer.Write((short)propertyName.Length);
                    writer.Write(propertyName);
                }
            }
            writer.Write((byte)RavenBinaryToken.HeaderEnd);

            writer.Write((byte)RavenBinaryToken.BodyStart);
            {
                bodyStream.Position = 0;
                bodyStream.CopyTo(writer.BaseStream);
            }            
            writer.Write((byte)RavenBinaryToken.BodyEnd);

            this.properties = null;
            this.propertiesLookup = null;
            this.bodyWriter = null;
            this.bodyStream.Dispose();
            this.bodyStream = null;            
        }

        private void WriteArray(RavenJArray array)
        {
            bodyWriter.Write((byte)RavenBinaryToken.ArrayStart);

            foreach (var token in array)
                Write(token);

            bodyWriter.Write((byte)RavenBinaryToken.ArrayEnd);   
        }

        private void WriteObject(RavenJObject @object)
        {
            bodyWriter.Write((byte)RavenBinaryToken.ObjectStart);

            var snapshot = @object.ToArray();

            // Write how many properties in this object.
            bodyWriter.Write((short)snapshot.Count());

            foreach (var property in snapshot)
            {
                byte index;
                if (!this.propertiesLookup.TryGetValue(property.Key, out index))
                {
                    index = (byte)this.properties.Count;

                    this.properties.Add( property.Key );
                    this.propertiesLookup[property.Key] = index;
                }

                bodyWriter.Write(index);

                var token = property.Value;
                switch (token.Type)
                {
                    case JTokenType.Object:
                        {
                            WriteObject((RavenJObject)token);
                            break;
                        }
                    case JTokenType.Array:
                        {
                            WriteArray((RavenJArray)token);
                            break;
                        }
                    default:
                        {
                            WritePrimitive((RavenJValue)token);
                            break;
                        }
                }
            }

            bodyWriter.Write((byte)RavenBinaryToken.ObjectEnd);   
        }

        public void WriteValue( RavenJValue record )
        {
            bodyWriter.Write((byte)RavenBinaryToken.ValueStart);
            WritePrimitive(record);
            bodyWriter.Write((byte)RavenBinaryToken.ValueEnd); 
        }

        private void WritePrimitive(RavenJValue record)
        {
            object _value = record.Value;
            switch (record.Type)
            {
                case JTokenType.Integer:
                    {
                        bodyWriter.Write((byte)RavenBinaryToken.Integer);
                        bodyWriter.Write(record.Value<int>());
                        return;
                    }
                case JTokenType.Float:
                    {
                        if (_value is decimal)
                        {
                            throw new NotSupportedException();
                        }
                        else if (_value is float)
                        {
                            bodyWriter.Write((byte)RavenBinaryToken.Float);
                            bodyWriter.Write(record.Value<float>());
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                        return;
                    }
                case JTokenType.String:
                    {
                        if ( _value == null )
                            _value = string.Empty;

                        char[] valueAsArray = ((string) _value).ToCharArray();
                        short size = (short) valueAsArray.Length;

                        bodyWriter.Write((byte)RavenBinaryToken.String);
                        bodyWriter.Write(size);
                        bodyWriter.Write(valueAsArray);

                        return;
                    }
                case JTokenType.Boolean:
                    {
                        bodyWriter.Write((byte)RavenBinaryToken.Boolean);
                        bodyWriter.Write((bool)_value);

                        return;
                    }                                        
                case JTokenType.Date:
                    {
                        long ticks = 0;
                        long offset = 0;
                        if (_value is DateTimeOffset)
                        {
                            var dateTime = (DateTimeOffset) _value;
                            ticks = dateTime.Ticks;
                            offset = dateTime.Offset.Ticks;
                        }
                        else if (_value is DateTime)
                        {
                            var dateTime = Convert.ToDateTime(_value, CultureInfo.InvariantCulture);
                            ticks = dateTime.Ticks;
                        }
                        else throw new NotSupportedException();

                        bodyWriter.Write((byte)RavenBinaryToken.Date);
                        bodyWriter.Write(ticks);
                        bodyWriter.Write(offset);
                        return;
                    }
                case JTokenType.Bytes:
                    {
                        byte[] array = (byte[])_value;
                        bodyWriter.Write((byte)RavenBinaryToken.Bytes);
                        bodyWriter.Write(array.Length);
                        bodyWriter.Write(array);
                        return;
                    }
                case JTokenType.Guid:
                    {
                        var guid = (Guid)_value;
                        byte[] array = guid.ToByteArray();

                        bodyWriter.Write((byte)RavenBinaryToken.Guid);
                        bodyWriter.Write(array);

                        return;
                    }
                case JTokenType.Uri: throw new NotSupportedException();
                case JTokenType.TimeSpan:
                    {
                        long ticks = ((TimeSpan)_value).Ticks;

                        bodyWriter.Write((byte)RavenBinaryToken.TimeSpan);
                        bodyWriter.Write(ticks);
                        return;
                    }        
                case JTokenType.Null:
                    {
                        bodyWriter.Write((byte)RavenBinaryToken.Null);
                        return;
                    }
                case JTokenType.Undefined:
                    {
                        bodyWriter.Write((byte)RavenBinaryToken.Undefined);
                        return;
                    }
                default: throw new NotSupportedException();
            }         
        }

        public void Flush()
        {
            writer.Flush();
        }


    }
}
