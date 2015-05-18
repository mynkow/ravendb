using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Abstractions.Json.Linq
{
    [Flags]
    public enum RavenFlatTokenMask : byte
    {
        IsArray = 1 << 7, // Bit 8 ON
        IsObject = 1 << 6, // Bit 7 ON
        IsPtr = 1 << 5, // Bit 6 ON
        AsPrimitive = 0x1F, // Bits 1 to 5
    }

    /// <summary>
    /// The definition of tokens. We have up to 32 available token values.
    /// </summary>
    public enum RavenFlatToken : byte
    {
        Invalid = 0,

        Boolean = 1,
        Integer,
        Long,
        Float,
        Double,
        Decimal,
        String,
        Date,
        TimeSpan,
        Guid,
        Uri,
        Bytes, 
        Null,
        Undefined = 20,                 
    }
}
