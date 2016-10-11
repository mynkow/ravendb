using System.Runtime.CompilerServices;
using Voron;

namespace Regression.PageLocator
{
    public class LowLevelTransactionStub
    {
        // TODO: implement register shuffling here.
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static Page ModifyPage(long pageNumber)
        {
            unsafe
            {
                return new Page(null, null);
            }
        }

        // TODO: implement register shuffling here.
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static Page GetPage(long pageNumber)
        {
            unsafe
            {
                return new Page(null, null);
            }
        }
    }
}