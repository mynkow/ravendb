using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Voron.Data.BTrees.Cedar;

namespace Voron.Data.BTrees
{
    unsafe partial class CedarPage
    {
        private const int _maxTrial = 1;

        private const long TAIL_OFFSET_MASK = 0xffffffff;
        private const long NODE_INDEX_MASK = 0xffffffff << 32;

        public int NonZeroSize
        {
            get
            {
                Node* array = Blocks.Nodes;

                int i = 0;
                for (int to = 0; to < Header.Ptr->Size; to++)
                {
                    if (array[to].Check >= 0)
                        i++;
                }
                return i;
            }
        }

        public int NonZeroLength
        {
            get
            {
                Node* array = Blocks.Nodes;

                int i = 0;
                int j = 0;
                for (int to = 0; to < Header.Ptr->Size; to++)
                {
                    Node* n = &array[to];
                    if (n->Check >= 0 && array[n->Check].Base != to && n->Base < 0)
                    {
                        j++;

                        for (int index = -n->Base; Tail[index] != 0; index++)
                            i++;
                    }
                }

                return i + j*(1 + sizeof(short));
            }
        }

        public int NumberOfKeys
        {
            get
            {
                Node* array = Blocks.Nodes;

                int i = 0;
                for (int to = 0; to < Header.Ptr->Size; to++)
                {
                    Node* n = &array[to];
                    if (n->Check >= 0 && (array[n->Check].Base == to || n->Base < 0))
                        i++;
                }

                return i;
            }
        }

        // test the validity of double array for debug
        internal void DebugTest(long from = 0)
        {
            Node* array = Blocks.Nodes;
            NodeInfo* nInfo = Blocks.NodesInfo;

            int @base = array[from].Base;
            if (@base < 0)
            {
                // validate tail offset
                if (Tail.Length < -@base + 1 + sizeof(short))
                    throw new Exception($"Fail in tail offset {from}");

                return;
            }

            byte c = nInfo[from].Child;
            do
            {
                if (from != 0)
                {
                    if (array[@base ^ c].Check != from)
                        throw new Exception($"");
                }
                if (c != 0)
                {
                    DebugTest(@base ^ c);
                }

                c = nInfo[@base ^ c].Sibling;
            } while (c != 0);
        }

        private int Follow(long from, byte label)
        {
            throw new NotImplementedException();
        }

        private CedarActionStatus Update(byte* key, int len, short value, ref long from, ref long pos)
        {
            if (len == 0 && from == 0)
                throw new ArgumentException("failed to insert zero-length key");

            // We are being conservative is there is not enough space in the tail to write it entirely, we wont continue.
            if (Tail.Length + len < Header.Ptr->TailBytesPerPage)
                return CedarActionStatus.NotEnoughSpace;

            //Console.WriteLine($"Start {nameof(Update)} with key-size: {len}");


            // Chances are that we will need to Write on the array, so getting the write version is the way to go here. 
            var _array = (Node*)Blocks.DirectWrite<Node>();

            long offset = from >> 32;
            if (offset == 0)
            {
                //Console.WriteLine("Begin 1");

                for (byte* keyPtr = key; _array[from].Base >= 0; pos++)
                {
                    if (pos == len)
                    {
                        int current = Follow(from, 0);
                        _array[current].Value = value;

                        return CedarActionStatus.Success;
                    }

                    from = Follow(from, key[pos]);
                }

                //Console.WriteLine("End 1");

                offset = -_array[from].Base;
            }

            long pos_orig;
            byte* tailPtr;
            if (offset >= sizeof(int)) // go to _tail
            {
                long moved;
                pos_orig = pos;

                tailPtr = Tail.DirectRead(offset - pos);
                while (pos < len && key[pos] == tailPtr[pos])
                    pos++;

                if (pos == len && tailPtr[pos] == '\0')
                {

                    tailPtr = Tail.DirectWrite(offset - pos);

                    // we found an exact match
                    moved = pos - pos_orig;
                    if (moved != 0)
                    {
                        // search end on tail
                        from &= TAIL_OFFSET_MASK;
                        from |= (offset + moved) << 32;
                    }

                    byte* ptr = tailPtr + (len + 1);
                    Debug.Assert(ptr + sizeof(short) - 1 < Tail.DirectRead() + Tail.Length);

                    // TODO: Write in the proper endianness.
                    Unsafe.Write(ptr, value);

                    //Console.WriteLine($"_tail[{ptrOffset}] = {Unsafe.Read<T>(ptr)}");

                    return CedarActionStatus.Success;
                }

                // otherwise, insert the common prefix in tail if any
                if (from >> 32 != 0)
                {
                    from &= TAIL_OFFSET_MASK; // reset to update tail offset
                    for (int offset_ = -_array[from].Base; offset_ < offset;)
                    {
                        from = Follow(from, Tail[offset_]);
                        offset_++;
                    }

                    //Console.WriteLine();
                }

                //Console.WriteLine("Begin 2");

                for (long pos_ = pos_orig; pos_ < pos; pos_++)
                    from = Follow(from, key[pos_]);

                //Console.WriteLine("End 2");                    

                moved = pos - pos_orig;
                if (tailPtr[pos] != 0)
                {
                    // remember to move offset to existing tail
                    long to_ = Follow(from, tailPtr[pos]);
                    moved++;

                    //Console.WriteLine($"_array[{to_}].Base = {-(int)(offset + moved)}");
                    _array[to_].Base = (short) -(offset + moved);

                    moved -= 1 + sizeof(short); // keep record
                }

                moved += offset;
                for (int i = (int) offset; i <= moved; i += 1 + sizeof(short))
                {
                    Tail0.Length += 1;

                    // NO REALLOCATION WILL HAPPEN HERE... KEEPING THE CODE JUST IN CASE FOR UNTIL PORTING IS FINISHED.
                    //if (_quota0 == u2.Length0)
                    //{
                    //    _quota0 += u2.Length0 >= MAX_ALLOC_SIZE ? MAX_ALLOC_SIZE : u2.Length0;
                    //    // _quota0 += _quota0;

                    //    // Reallocating Tail0                        
                    //    u2 = new SizeUnion(Reallocate<int>(u2.Tail0, _quota0, u2.Length0));
                    //}

                    Tail0[Tail0.Length] = i;
                }

                if (pos == len || tailPtr[pos] == '\0')
                {
                    long to = Follow(from, 0);
                    if (pos == len)
                    {
                        var n = (Node*) Blocks.DirectWrite<Node>(to);
                        
                        // TODO: Write in the proper endianness.
                        n->Value = value;

                        return CedarActionStatus.Success;
                    }
                    else
                    {
                        short toValue = *(short*) &tailPtr[pos + 1];

                        var n = (Node*) Blocks.DirectWrite<Node>(to);

                        // TODO: Write in the proper endianness.
                        n->Value = toValue;

                        return CedarActionStatus.Success;
                    }
                }

                from = Follow(from, key[pos]);
                pos++;
            }

            //
            int needed = (int) (len - pos + 1 + sizeof(short));
            if (pos == len && Tail0.Length != 0)
            {
                int offset0 = Tail0[Tail0.Length];
                Tail[offset0] = 0;

                //Console.WriteLine($"_array[{from}].Base = {-offset0}");
                _array[from].Base = (short) -offset0;
                Tail0.Length = Tail0.Length - 1;

                //Console.WriteLine($"_tail[{offset0 + 1}] = {value}");
                Unsafe.Write(Tail.DirectWrite(offset0 + 1), value);

                return CedarActionStatus.Success;
            }

            // NO REALLOCATION WILL HAPPEN HERE, WE MUST FAIL BEFORE ENTERING HERE... 
            // KEEPING THE CODE JUST IN CASE FOR UNTIL PORTING IS FINISHED.

            //if (_quota < Tail.Length + needed)
            //{
            //    if (needed > u1.Length || needed > MAX_ALLOC_SIZE)
            //        _quota += needed;
            //    else
            //        _quota += (u1.Length >= MAX_ALLOC_SIZE) ? MAX_ALLOC_SIZE : u1.Length;

            //    // Reallocating Tail.
            //    u1 = new ElementUnion(Reallocate<byte>(u1.Tail, _quota, u1.Length));
            //}

            //Console.WriteLine($"_array[{from}].Base = {-u1.Length}");
            _array[from].Base = (short) -Tail.Length;
            pos_orig = pos;

            tailPtr = Tail.DirectWrite(Tail.Length - pos);
            if (pos < len)
            {
                do
                {
                    //Console.WriteLine($"_tail[{tailOffset + pos}] = {key[pos]}");
                    tailPtr[pos] = key[pos];
                }
                while (++pos < len);

                from |= ((long) (Tail.Length) + (len - pos_orig)) << 32;
            }

            Tail.Length += needed;

            Unsafe.Write(&tailPtr[len + 1], value);

            //Console.WriteLine($"_tail[{tailOffset + (len + 1)}] = {Unsafe.Read<T>(ptr)}");


            //Console.WriteLine($"End {nameof(Update)} with key-size: {len}");

            return CedarActionStatus.Success;
        }
    }
}
