using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Sparrow.Json.Parsing
{
    public unsafe class ExperimentalJsonParser
    {
        private JsonOperationContext _ctx;
        private ReadOnlyMemory<byte> _buffer;        
        private StructuralIndex _index;
        private string _debugTag;

        private Memory<byte> _backlashs;
        private Memory<byte> _quotes;
        private Memory<byte> _colons;
        private Memory<byte> _leftBraces;
        private Memory<byte> _rightBraces;


        public ExperimentalJsonParser(JsonOperationContext ctx, ReadOnlyMemory<byte> buffer, string debugTag)
        {
            this._ctx = ctx;            
            this._buffer = buffer;
            this._debugTag = debugTag;
        }

        public BlittableJsonReaderObject Parse()
        {
            this._index = new StructuralIndex();

            var bitmaps = _ctx.GetMemory(_buffer.Length * 5);

            //_backlashs = new OwnedMemory<byte>(bitmaps.Address, _buffer.Length);

            throw new NotImplementedException();
        }

        public string GenerateErrorState()
        {
            throw new NotImplementedException();
        }


        private class StructuralIndex
        {
        }
    }
}
