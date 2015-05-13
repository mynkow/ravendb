using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Json.Linq
{
    public class RavenFlatReader : IDisposable
    {
        private readonly BinaryReader streamReader;

        public RavenFlatReader( Stream stream ) : this ( new BinaryReader(stream) ) { }

        public RavenFlatReader(BinaryReader reader)
        {
            this.streamReader = reader;       
        }

        public void Dispose()
        {
            // TODO: Properly implement IDisposable
        }
    }
}
