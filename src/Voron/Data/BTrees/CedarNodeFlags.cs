using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    public enum CedarNodeFlags : byte
    {
        /// <summary>
        /// Is this a reference to a branch node?
        /// </summary>
        Branch = 1,

        /// <summary>
        /// Is this a reference to data?
        /// </summary>
        DataRef = 2,

        /// <summary>
        /// Is this embedded data?.
        /// </summary>
        Data = 3,
    }
}
