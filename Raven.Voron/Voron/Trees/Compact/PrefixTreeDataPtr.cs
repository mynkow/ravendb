using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    // Until we have a final implementation, we are using sequential to make it easier to change.
    // TODO: Change when the implementation is more mature.

    // [StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PrefixTreeDataPtr
    {
        public long Page;
        public long Start;
        public long Size;
    }

    public enum PrefixTreeDataFields
    {
        Key,
        Data,
    }
}
