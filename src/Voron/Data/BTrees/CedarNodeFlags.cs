using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    public enum CedarNodeFlags : byte
    {
        /// <summary>
        /// Is this a reference to a branch Node?
        /// </summary>
        Branch = 0,

        /// <summary>
        /// Is this embedded data?.
        /// </summary>
        Data = 1,
    }
}
