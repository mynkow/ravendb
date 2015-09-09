using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    public sealed unsafe class PrefixTreeMutableState
    {
        public PrefixTreeNodePtr Root;
        public PrefixTreeNodePtr Head;
        public PrefixTreeNodePtr Tail;

        public long TablePageNumber;
        public long TablePageCount;
        public long TableSize;

        public long LeafCount;
        public long InternalCount;
        public long NodesPageCount;

        public bool IsModified;

        public PrefixTreeMutableState( PrefixTreeRootHeader* header )
        {
            this.Root = header->Root;
            this.Head = header->Head;
            this.Tail = header->Tail;

            this.TablePageNumber = header->TablePageNumber;
            this.TablePageCount = header->TablePageCount;
            this.TableSize = header->TableSize;

            this.LeafCount = header->LeafCount;
            this.InternalCount = header->InternalCount;
            this.NodesPageCount = header->NodesPageCount;
        }
    }
}
