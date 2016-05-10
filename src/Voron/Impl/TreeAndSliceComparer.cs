using System;
using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Voron.Impl
{
    internal class TreeAndSliceComparer : IEqualityComparer<Tuple<Tree, ISlice>>
    {
        public bool Equals(Tuple<Tree, ISlice> x, Tuple<Tree, ISlice> y)
        {
            if (x == null && y == null)
                return true;
            if (x == null || y == null)
                return false;

            if (x.Item1 != y.Item1)
                return false;

            ISlice x1 = x.Item2;
            ISlice x2 = y.Item2;

            if (x1 is SliceArray )
            {
                if (x2 is SliceArray)
                    return SliceComparer.Equals((SliceArray)x1, (SliceArray)x2);
                if (x2 is SlicePointer)
                    return SliceComparer.Equals((SliceArray)x1, (SlicePointer)x2);
                
                throw new NotSupportedException($"The type '{x2.GetType().FullName}' is not supported.");
            }

            if ( x2 is SlicePointer)
            {
                if (x2 is SliceArray)
                    return SliceComparer.Equals((SlicePointer)x1, (SliceArray)x2);
                if (x2 is SlicePointer)
                    return SliceComparer.Equals((SlicePointer)x1, (SlicePointer)x2);
                
                throw new NotSupportedException($"The type '{x2.GetType().FullName}' is not supported.");
            }

            throw new NotSupportedException($"The type '{x1.GetType().FullName}' is not supported.");
        }

        public int GetHashCode(Tuple<Tree, ISlice> obj)
        {
            return obj.Item1.GetHashCode() ^ 397 * obj.Item2.GetHashCode();
        }
    }
}
