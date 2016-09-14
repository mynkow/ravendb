using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Data.BTrees
{

    public interface ICedarResult
    {
        void SetResult(short value, int length, long pos);
    };

    public interface ICedarResultKey : ICedarResult
    {
        void SetKey(Slice key);
    }

    public struct CedarRef : ICedarResult
    {
        public short Value;

        void ICedarResult.SetResult(short value, int length, long pos)
        {
            Value = value;
        }
    }

    public struct CedarKeyPair : ICedarResultKey
    {
        public Slice Key;
        public short Value;
        public int Length; // prefix length

        void ICedarResult.SetResult(short value, int length, long pos)
        {
            Value = value;
            Length = length;
        }

        void ICedarResultKey.SetKey(Slice key)
        {
            Key = key;
        }
    }

    public struct CedarPair : ICedarResult
    {
        public short Value;
        public int Length; // prefix length

        void ICedarResult.SetResult(short value, int length, long pos)
        {
            Value = value;
            Length = length;
        }
    }

    public struct CedarTuple : ICedarResult // for predict ()
    {
        public short Value;
        public int Length; // suffix length
        public long Node; // node id of value

        void ICedarResult.SetResult(short value, int length, long pos)
        {
            Value = value;
            Length = length;
            Node = pos;
        }
    }

    public enum CedarResultCode : short
    {
        Success = 0,
        NoValue = -1,
        NoPath = -2
    }

}
