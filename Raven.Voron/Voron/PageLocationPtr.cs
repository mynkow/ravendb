using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron
{

    /// <summary>
    /// The <see cref="PageLocationPtr"/> is an structure that can be used to point a certain location inside a single page.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PageLocationPtr
    {
        [FieldOffset(0)]
        public long Page;

        [FieldOffset(8)]
        public int Offset;
    }

    /// <summary>
    /// The <see cref="PageLocationExPtr"/> is an structure that can be used to point a certain location inside an overflow page composed of
    /// multiple continuous pages.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PageLocationExPtr
    {
        [FieldOffset(0)]
        public long Page;

        [FieldOffset(8)]
        public int Count;

        [FieldOffset(12)]
        public int Offset;
    }
}
