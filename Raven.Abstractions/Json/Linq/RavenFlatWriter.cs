using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Json.Linq
{
    public class RavenFlatWriter : IDisposable
    {
        private readonly BinaryWriter writer;

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
    }
}
