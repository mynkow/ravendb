using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    //TODO: Change this when we are ready to go.
    //[StructLayout(LayoutKind.Explicit, Pack = 1)]
    [StructLayout(LayoutKind.Sequential)]
    public struct PrefixTreeTablePageHeader
    {
        /// <summary>
        /// The current capacity of the dictionary
        /// </summary>
        public int Capacity;

        /// <summary>
        /// This is the initial capacity of the dictionary, we will never shrink beyond this point.
        /// </summary>
        public int InitialCapacity;

        /// <summary>
        /// This is the real counter of how many items are in the hash-table (regardless of buckets)
        /// </summary>
        public int Size;

        /// <summary>
        /// How many used buckets. 
        /// </summary>
        public int NumberOfUsed;

        /// <summary>
        /// How many occupied buckets are marked deleted
        /// </summary>
        public int NumberOfDeleted;

        /// <summary>
        /// The next growth threshold. 
        /// </summary>
        public int NextGrowthThreshold;
    }
}
