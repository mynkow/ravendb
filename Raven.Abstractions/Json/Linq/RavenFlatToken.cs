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
        IsReserved = 1 << 4, // Bit 5 ON
        IsPrimitive = 0x0F, // Bits 1 to 4
    }

    [Flags]
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
