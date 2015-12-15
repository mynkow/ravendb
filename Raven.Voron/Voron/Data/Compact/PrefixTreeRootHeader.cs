using System.Runtime.InteropServices;

namespace Voron.Data.Compact
{
    //TODO: Change this when we are ready to go.
    //[StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential)]
    public struct PrefixTreeRootHeader
    {
        /// <summary>
        /// The root header page for the tree. 
        /// </summary>
        public long Root;

        /// <summary>
        /// The table header page for the tree.
        /// </summary>
        public long Table;       
    }
}
