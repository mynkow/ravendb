using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Json.Utilities;
using System.Globalization;
using System.IO;

namespace Raven.Json.Linq
{
    public class RavenBinaryReader
    {
        public RavenBinaryToken Current { get; set; }

        private readonly BinaryReader reader;

        public RavenBinaryReader( Stream stream ) : this ( new BinaryReader(stream) ) { }

        public RavenBinaryReader( BinaryReader reader )
        {
            this.reader = reader;

            this.Current = RavenBinaryToken.None;
        }

        public bool ReadToken()
        {
            try
            {
                var token = reader.ReadByte();
                this.Current = (RavenBinaryToken)token;
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

            // Prime the token for the next one reading.
            if (!this.ReadToken())
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
    }
}
