using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Json.Utilities;
using System.Globalization;
using System.IO;

namespace Raven.Json.Linq
{
    public class RavenBinaryReader : IDisposable
    {
        public RavenBinaryToken Current { get; set; }

        private readonly BinaryReader streamReader;
        private BinaryReader reader;

        public RavenBinaryReader( Stream stream ) : this ( new BinaryReader(stream) ) { }

        public RavenBinaryReader(BinaryReader reader)
        {
            this.streamReader = reader;
            this.reader = reader;
            
            this.Current = RavenBinaryToken.None;
        }


        public RavenBinaryToken PeekToken()
        {
            return (RavenBinaryToken)reader.PeekChar();
        }
        
        public bool ReadToken()
        {
            try
            {
                var token = reader.ReadByte();
                
                this.Current = (RavenBinaryToken)token;
                if (this.Current == RavenBinaryToken.BodyEnd)
                    this.reader = streamReader;
            }
            catch
            {
                return false;
            }
            
            return true;
        }

        public RavenBinaryHeader ReadHeader()
        {
            if (this.Current == RavenBinaryToken.None)
            {
                if (!this.ReadToken())
                    throw new Exception("Error reading header from RavenBinaryReader.");
            }

            if (this.Current != RavenBinaryToken.HeaderStart)
                throw new Exception("Error reading header from RavenBinaryReader. Current object does not have a header: {0}".FormatWith(CultureInfo.InvariantCulture, this.Current));

            int propertiesCount = reader.ReadInt16();

            // We read all the properties
            string[] propertyNameIndex = new string[propertiesCount];
            for (int i = 0; i < propertiesCount; i++)
                propertyNameIndex[i] = ReadString();

            if (!this.ReadToken() && this.Current != RavenBinaryToken.HeaderEnd)
                throw new Exception("Error reading header from RavenBinaryReader.");

            var bodySize = this.ReadInteger();
            byte[] body = this.reader.ReadBytes(bodySize);            
            var memoryStream = new MemoryStream(body);
            this.reader = new BinaryReader(memoryStream);

            // Prime the token for the next one reading.
            if (!this.ReadToken() && this.Current != RavenBinaryToken.BodyStart)
                throw new Exception("Error reading RavenJToken from RavenBinaryReader.");

            return new RavenBinaryHeader(propertyNameIndex);
        }

        public string ReadString()
        {
            var stringSize = reader.ReadUInt16();

            char[] stringAsBytes = reader.ReadChars(stringSize);
            return new string( stringAsBytes );
        }

        public int ReadInteger()
        {
            return reader.ReadInt32();
        }

        public float ReadSingle()
        {
            return reader.ReadSingle();
        }

        public DateTimeOffset ReadDateTimeOffset()
        {
            long dateTime = reader.ReadInt64();
            long offset = reader.ReadInt64();
            return new DateTimeOffset(dateTime, new TimeSpan(offset));
        }

        public DateTimeOffset ReadDateTimeUtc()
        {
            long dateTime = reader.ReadInt64();
            return new DateTime(dateTime, DateTimeKind.Utc);
        }

        public bool ReadBoolean()
        {
            return reader.ReadBoolean();
        }

        public byte[] ReadBytes()
        {
            var size = reader.ReadInt32();
            return reader.ReadBytes(size);
        }

        public byte ReadByte()
        {
            return reader.ReadByte();
        }

        public ushort ReadUInt16()
        {
            return reader.ReadUInt16();
        }

        public void Dispose()
        {
            // TODO: Properly implement IDisposable
        }
    }
}
