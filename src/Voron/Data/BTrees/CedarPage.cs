using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// Each CedarPage is composed of the following components:
    /// - Header
    /// - BlocksPages with as many as <see cref="CedarRootHeader.NumberOfBlocksPages"/>
    ///     - The first page is going to be shared with the <see cref="CedarPageHeader"/> therefore it will have a few lesser blocks than possible in a page.
    /// - TailPages with as many as <see cref="CedarRootHeader.NumberOfTailPages"/>
    /// - NodesPages with as many as <see cref="CedarRootHeader.NumberOfNodePages"/>    
    /// </summary>
    public class CedarPage
    {
    }
}
