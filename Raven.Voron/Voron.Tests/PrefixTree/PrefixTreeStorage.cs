using Bond;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Data.Compact;
using Voron.Tests;
using Xunit;

namespace Voron.Data.Compact.Tests
{
    public unsafe class PrefixTreeStorageTests : StorageTest
    {
        [Schema]
        public sealed class SampleData : IEquatable<SampleData>
        {
            [Id(0)]
            public string Data;

            bool IEquatable<SampleData>.Equals(SampleData other)
            {
                return other.Data == this.Data;
            }
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
        }


        public static void DumpKeys(PrefixTree tree)
        {
            Console.WriteLine("Tree stored order");

            throw new NotImplementedException();

            //var current = tree.Head.Next;
            //while (current != null && current != tree.Tail)
            //{
            //    Console.WriteLine(current.Key.ToString());
            //    current = current.Next;
            //}
        }

        public static void DumpTree(PrefixTree tree)
        {
            if (tree.Count == 0)
            {
                Console.WriteLine("Tree is empty.");
            }
            else
            {
                DumpNodes(tree, tree.Root, null, 0, 0);
            }
        }

        private static int DumpNodes(PrefixTree tree, PrefixTree.Node* node, PrefixTree.Node* parent, int nameLength, int depth)
        {
            if (node == null)
                return 0;

            for (int i = depth; i-- != 0;)
                Console.Write('\t');

            if (node->IsInternal)
            {
                var internalNode = (PrefixTree.Internal*)node;

                var jumpLeft = (PrefixTree.Node*)tree.ReadDirect(internalNode->JumpLeftPtr);
                var jumpRight = (PrefixTree.Node*)tree.ReadDirect(internalNode->JumpRightPtr);

                Console.WriteLine(string.Format("Node {0} (name length: {1}) Jump left: {2} Jump right: {3}", tree.ToDebugString(node), nameLength, tree.ToDebugString(jumpLeft), tree.ToDebugString(jumpRight)));

                var left = (PrefixTree.Node*)tree.ReadDirect(internalNode->LeftPtr);
                var right = (PrefixTree.Node*)tree.ReadDirect(internalNode->RightPtr);

                return 1 + DumpNodes(tree, left, node, internalNode->ExtentLength + 1, depth + 1)
                         + DumpNodes(tree, right, node, internalNode->ExtentLength + 1, depth + 1);
            }
            else
            {
                Console.WriteLine(string.Format("Node {0} (name length: {1})", tree.ToDebugString(node), nameLength));

                return 1;
            }
        }


        public static void StructuralVerify(PrefixTree tree) 
        {
            throw new NotImplementedException();

            //Assert.NotNull(tree.Head);
            //Assert.NotNull(tree.Tail);
            //Assert.Null(tree.Tail.Next);
            //Assert.Null(tree.Head.Previous);

            //Assert.True(tree.Root == null || tree.Root->NameLength == 0); // Either the root does not exist or the root is internal and have name length == 0
            //Assert.True(tree.Count == 0 && tree.NodesTable.Count == 0 || tree.Count == tree.NodesTable.Values.Count() + 1);

            //if (tree.Count == 0)
            //{
            //    Assert.Equal(tree.Head, tree.Tail.Previous);
            //    Assert.Equal(tree.Tail, tree.Head.Next);

            //    Assert.NotNull(tree.NodesTable);
            //    Assert.Equal(0, tree.NodesTable.Count);

            //    return; // No more to check for an empty trie.
            //}

            //var root = tree.Root;
            //var nodes = new HashSet<long>();

            //foreach (var node in tree.NodesTable.Values)
            //{
            //    int handleLength = tree.GetHandleLength(node);

            //    Assert.True(root == node || tree.GetHandleLength(root) < handleLength); // All handled of lower nodes must be bigger than the root.

            //    var referenceNode = (PrefixTree.Node*)tree.ReadDirect(node.ReferencePtr);
            //    var backReferenceNode = (PrefixTree.Node*)tree.ReadDirect(referenceNode.ReferencePtr);

            //    Assert.Equal(node, backReferenceNode); // The reference of the reference should be itself.

            //    nodes.Add(node);
            //}

            //Assert.Equal(tree.NodesTable.Values.Count(), nodes.Count); // We are ensuring there are no repeated nodes in the hash table. 

            //if (tree.Count == 1)
            //{
            //    Assert.Equal(tree.Root, tree.Head.Next);
            //    Assert.Equal(tree.Root, tree.Tail.Previous);
            //}
            //else
            //{
            //    var toRight = tree.Head.Next;
            //    var toLeft = tree.Tail.Previous;

            //    for (int i = 1; i < tree.Count; i++)
            //    {
            //        Ensure there is name order in the linked list of leaves.
            //       Assert.True(toRight.Name(tree).CompareTo(toRight.Next.Name(tree)) <= 0);
            //        Assert.True(toLeft.Name(tree).CompareTo(toLeft.Previous.Name(tree)) >= 0);

            //        toRight = toRight.Next;
            //        toLeft = toLeft.Previous;
            //    }

            //    var leaves = new HashSet<long>();
            //    var references = new HashSet<long>();

            //    int numberOfNodes = VisitNodes(tree, tree.Root, null, 0, nodes, leaves, references);
            //    Assert.Equal(2 * tree.Count - 1, numberOfNodes); // The amount of nodes is directly correlated with the tree size.
            //    Assert.Equal(tree.Count, leaves.Count); // The size of the tree is equal to the amount of leaves in the tree.

            //    int counter = 0;
            //    foreach (var leaf in leaves)
            //    {
            //        if (references.Contains(leaf.Key))
            //            counter++;
            //    }

            //    Assert.Equal(tree.Count - 1, counter);
            //}

            //Assert.Equal(0, nodes.Count);

            //Assert.DoesNotThrow(() => tree.NodesTable.VerifyStructure());
        }

        private static int VisitNodes(PrefixTree tree, PrefixTree.Node* node,
                                     PrefixTree.Node* parent, int nameLength,
                                     HashSet<long> nodes,
                                     HashSet<long> leaves,
                                     HashSet<long> references) 
        {
            if (node == null)
                return 0;

            Assert.True(nameLength <= tree.GetExtentLength(node));

            if ( parent->IsInternal )
            {
                Assert.True(tree.Extent(parent).Equals(tree.Extent(node).SubVector(0, ((PrefixTree.Internal*)parent)->ExtentLength)));
            }

            if (node->IsInternal)
            {
                var leafNode = (PrefixTree.Leaf*)tree.ReadDirect(node->ReferencePtr);

                Assert.NotNull(leafNode->IsLeaf); // We ensure that internal node references are leaves. 

                Assert.True(references.Add(leafNode->DataPtr));
                Assert.True(nodes.Remove((long)node));

                var handle = tree.Handle(node);

                var allNodes = tree.NodesTable.Values.Select(x => tree.Handle((PrefixTree.Node*)tree.ReadDirect(x)) );

                Assert.True(allNodes.Contains(handle));

                var internalNode = (PrefixTree.Internal*)node;
                int jumpLength = tree.GetJumpLength(internalNode);

                var jumpLeft = (PrefixTree.Node*)tree.ReadDirect(internalNode->LeftPtr);
                while (jumpLeft->IsInternal && jumpLength > ((PrefixTree.Internal*)jumpLeft)->ExtentLength)
                    jumpLeft = (PrefixTree.Node*)tree.ReadDirect(((PrefixTree.Internal*)jumpLeft)->LeftPtr);

                Assert.Equal(internalNode->JumpLeftPtr, (long)jumpLeft);

                var jumpRight = (PrefixTree.Node*)tree.ReadDirect(internalNode->RightPtr);
                while (jumpRight->IsInternal && jumpLength > ((PrefixTree.Internal*)jumpRight)->ExtentLength)
                    jumpRight = (PrefixTree.Node*)tree.ReadDirect(((PrefixTree.Internal*)jumpRight)->RightPtr);

                Assert.Equal(internalNode->JumpRightPtr, (long)jumpRight);

                var left = (PrefixTree.Node*)tree.ReadDirect(internalNode->LeftPtr);
                var right = (PrefixTree.Node*)tree.ReadDirect(internalNode->RightPtr);

                return 1 + VisitNodes(tree, left, node, internalNode->ExtentLength + 1, nodes, leaves, references)
                         + VisitNodes(tree, right, node, internalNode->ExtentLength + 1, nodes, leaves, references);
            }
            else
            {
                Assert.True(node->IsLeaf);
                var leafNode = (PrefixTree.Leaf*)node;

                Assert.True(leaves.Add((long)leafNode)); // We haven't found this leaf somewhere else.
                Assert.Equal(tree.Name(node).Count, tree.GetExtentLength(node)); // This is a leaf, the extent is the key

                var reference = (PrefixTree.Node*)tree.ReadDirect(parent->ReferencePtr);
                Assert.True(reference->IsLeaf); // We ensure that internal node references are leaves. 

                return 1;
            }
        }
    }
}
