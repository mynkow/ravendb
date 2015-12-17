using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    partial class PrefixTree
    {
        /// <summary>
        /// The cutpoint for a string x with respect to the trie is the length of the longest common prefix between x and exit(x).
        /// </summary>
        private unsafe class CutPoint
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;

            /// <summary>
            /// The parent of the exit node.
            /// </summary>
            public readonly Internal* Parent;

            /// <summary>
            /// The binary representation of the search key.
            /// </summary>
            public readonly BitVector SearchKey;

            /// <summary>
            /// The exit node. If parex(x) == root then exit(x) is the root; otherwise, exit(x) is the left or right child of parex(x) 
            /// depending whether x[|e-parex(x)|] is zero or one, respectively. Page 166 of [1]
            /// </summary>
            public readonly Node* Exit;

            public CutPoint(int lcp, Internal* parent, Node* exit, BitVector searchKey)
            {
                this.LongestPrefix = lcp;
                this.Parent = parent;
                this.Exit = exit;
                this.SearchKey = searchKey;
            }

            /// <summary>
            /// There are two cases. We say that x cuts high if the cutpoint is strictly smaller than |handle(exit(x))|, cuts low otherwise. Page 165 of [1]
            /// </summary>
            /// <remarks>Only when the cut is low, the handle(exit(x)) is a prefix of x.</remarks>
            public bool IsCutLow(PrefixTree owner)
            {
                // Theorem 3: Page 165 of [1]
                var handleLength = owner.GetHandleLength(this.Exit);
                return this.LongestPrefix >= handleLength;
            }

            public bool IsRightChild
            {
                get { return this.Parent != null && this.Parent == this.Exit; }
            }
        }

        private unsafe class ExitNode
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;

            /// <summary>
            /// The exit node, it will be a leaf when the search key matches the query. 
            /// </summary>
            public readonly Node* Exit;

            public readonly BitVector SearchKey;

            public ExitNode(int lcp, Node* exit, BitVector v)
            {
                this.LongestPrefix = lcp;
                this.Exit = exit;
                this.SearchKey = v;
            }

            public bool IsLeaf
            {
                get { return this.Exit->IsLeaf; }
            }
        }
    }
}
