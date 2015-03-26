using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Json.Linq
{

    public enum RavenBinaryToken : byte
    {
        None = 0,
        HeaderStart = 1,
        HeaderEnd = 6,
        BodyStart = 2,
        BodyEnd = 7,
        ObjectStart = 3,
        ObjectEnd = 8,
        ArrayStart = 4,
        ArrayEnd = 9,

        // Unused it is only a marker. 
        Primitives = 30,

        Null = 32,
        Undefined = 33,
        String = 34,
        Integer = 35,
        Float = 36,
        Date = 37,
        Boolean = 38,
        Bytes = 39,
        TimeSpan = 40,
        Guid = 41,
        Double = 42,
        Decimal = 43,
    }
}
