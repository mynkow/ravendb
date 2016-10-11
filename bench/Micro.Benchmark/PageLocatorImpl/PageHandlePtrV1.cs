using System.Runtime.CompilerServices;
using Voron;

namespace Regression.PageLocator
{
    public struct PageHandlePtrV1
    {
        public readonly Page Value;
        public readonly bool IsWritable;

        private const int Invalid = -1;

        public PageHandlePtrV1(Page value, bool isWritable)
        {
            this.Value = value;
            this.IsWritable = isWritable;
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsValid ? Value.PageNumber : Invalid; }
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Value != null; }
        }
    }
}