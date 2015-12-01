using System.Runtime.InteropServices;

namespace Voron.Data.Compact
{
    //TODO: Change this when we are ready to go.
    //[StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential)]
    public struct PrefixTreeRootHeader
    {
        public long Root;
        public long Table;
        public int Depth;
    }
}
