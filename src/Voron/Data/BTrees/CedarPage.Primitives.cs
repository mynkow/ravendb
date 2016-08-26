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

        /// <summary>
        /// This value is intended to be used only for diagnostics purposes.
        /// </summary>
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

        /// <summary>
        /// This value is intended to be used only for diagnostics purposes.
        /// </summary>
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

                return i + j*(1 + sizeof(long));
            }
        }

        /// <summary>
        /// This value is intended to be used only for diagnostics purposes.
        /// </summary>
        public int NumberOfKeys
        {
            get
            {
                // TODO: This will become a header element eventually if we need to call this outside of the testing work. 
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

        /// <summary>
        /// This value is intended to be used only for diagnostics purposes.
        /// </summary>
        internal void Validate(long from = 0)
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
                    Validate(@base ^ c);
                }

                c = nInfo[@base ^ c].Sibling;
            } while (c != 0);
        }

        public CedarActionStatus Update(Slice key, int size, out byte* pos, ushort? version = null)
        {
            pos = null;

            int index;
            CedarDataPtr* ptr;
            if (!Data.TryAllocateNode(out index, out ptr))
                return CedarActionStatus.NotEnoughSpace;

            long from = 0;
            long tailPos = 0;
            CedarActionStatus result = Update(key.Content.Ptr, key.Content.Length, (short)index, ref from, ref tailPos);
            if (result != CedarActionStatus.Success)
            {
                // Even though we could avoid freeing the node we just reserved,
                // because we are going to split the page and reconstruct all pages.
                // I prefer to err on the safe side here, unless we really need to
                // make this extremely efficient (which I doubt will be needed anyways).
                Data.FreeNode(index);

                return result; 
            }                

            ptr->Flags = CedarNodeFlags.Data;
            ptr->Version = version ?? 0;
            ptr->DataSize = (byte)size;
            pos = (byte*)&ptr->Data;            

            return CedarActionStatus.Success;
        }

        private CedarActionStatus Update(byte* key, int len, short value, ref long from, ref long pos)
        {
            if (len == 0 && from == 0)
                throw new ArgumentException("failed to insert zero-length key");

            // We are being conservative if there is not enough space in the tail to write it entirely, we wont continue.
            if (Tail.Length + len + sizeof(short) >= Header.Ptr->TailBytesPerPage)
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


        private int Follow(long from, byte label)
        {
            int to = 0;
            int @base = Blocks.Nodes[from].Base;
            if (@base < 0 || Blocks.Nodes[to = @base ^ label].Check < 0) // TODO: Check if the rules are the same here as in C++
            {
                to = _pop_enode(@base, label, (short)from);
                _push_sibling(from, to ^ label, label, @base >= 0);
            }
            else if (Blocks.Nodes[to].Check != (int)from)
            {
                to = _resolve(from, @base, label);
            }

            //Console.WriteLine($"F->{to}");

            return to;
        }

        private int _pop_enode(int @base, byte label, short from)
        {
            //Console.WriteLine($"enters [{@base},{label},{from}] (_pop_enode)");

            Header.SetWritable();
            var _array = (Node*)Blocks.DirectWrite<Node>();
            var _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();


            int e = @base < 0 ? _find_place() : @base ^ label;
            int bi = e >> 8;

            _block[bi].Num--;
            if (_block[bi].Num == 0)
            {
                if (bi != 0)
                    _transfer_block(bi, ref Header.Ptr->_bheadC, ref Header.Ptr->_bheadF); // Closed to Full
            }
            else // release empty node from empty ring
            {
                _array[-(_array[e].Base)].Check = _array[e].Check;

                //Console.WriteLine($"_array[{-(_array[e].Check)}].Base = {_array[e].Base}");
                _array[-(_array[e].Check)].Base = _array[e].Base;

                if (e == _block[bi].Ehead)
                    _block[bi].Ehead = -(_array[e].Check); // set ehead

                if (bi != 0 && _block[bi].Num == 1 && _block[bi].Trial != _maxTrial) // Open to Closed
                    _transfer_block(bi, ref Header.Ptr->_bheadO, ref Header.Ptr->_bheadC);
            }

            // initialize the released node
            if (label != 0)
            {
                //Console.WriteLine($"_array[{e}].Base = -1");
                _array[e].Base = -1;
            }
            else
            {
                ////Console.WriteLine($"_array[{e}].Base = 0");
                _array[e].Value = 0;

                Debug.Assert(_array[e].Base == 0);
            }

            _array[e].Check = from;

            if (@base < 0)
            {
                _array[from].Base = (short) (e ^ label);
                //Console.WriteLine($"_array[{from}].Base = {e ^ label}");
            }

            //Console.WriteLine($"returns: {e} (_pop_enode)");
            return e;
        }

        private void _transfer_block(int bi, ref int head_in, ref int head_out)
        {
            //Console.WriteLine($"enters [{bi},{head_in},{head_out}] (_transfer_block)");

            _pop_block(bi, ref head_in, bi == Blocks.Metadata[bi].Next);
            _push_block(bi, ref head_out, head_out == 0 && Blocks.Metadata[bi].Num != 0);
        }

        private void _pop_block(int bi, ref int head_in, bool last)
        {
            //Console.WriteLine($"enters [{bi},{head_in},{Convert.ToByte(last)}] (_pop_block)");

            if (last)
            {
                // last one poped; Closed or Open
                head_in = 0;
            }
            else
            {
                var _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();

                _block[_block[bi].Prev].Next = _block[bi].Next;
                //Console.WriteLine($"_block[{_block[bi].Prev}].Next = {_block[bi].Next} (_pop_block)");

                _block[_block[bi].Next].Prev = _block[bi].Prev;
                //Console.WriteLine($"_block[{_block[bi].Next}].Prev = {_block[bi].Prev} (_pop_block)");

                if (bi == head_in)
                    head_in = _block[bi].Next;
            }

            //Console.WriteLine($"returns [{head_in}] (_pop_block)");
        }

        private int _find_place()
        {
            var _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();

            //Console.WriteLine($"enters [] (_find_place)");

            if (Header.Ptr->_bheadC != 0)
            {
                //Console.WriteLine($"returns: {_block[_bheadC].Ehead} (_find_place)");
                return _block[Header.Ptr->_bheadC].Ehead;
            }

            if (Header.Ptr->_bheadO != 0)
            {
                //Console.WriteLine($"returns: {_block[_bheadO].Ehead} (_find_place)");
                return _block[Header.Ptr->_bheadO].Ehead;
            }

            int result = _add_block() << 8;
            //Console.WriteLine($"returns: {result} (_find_place)");

            return result;
        }

        private int _add_block()
        {
            int size = Header.Ptr->Size;
            int capacity = Header.Ptr->Capacity;
            if (size == capacity)
            {
                // TODO: CHECK WHAT HAPPENS WHEN THIS KICKS OFF.
                return (size >> 8) - 1;
            }

            Header.SetWritable();
            var _array = (Node*)Blocks.DirectWrite<Node>();
            var _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();

            _block[size >> 8].Ehead = size;
            _array[size] = new Node(-(size + 255), -(size + 1));
            for (int i = size + 1; i < size + 255; i++)
                _array[i] = new Node(-(i - 1), -(i + 1));

            _array[size + 255] = new Node(-(size + 254), -size);
            _push_block(size >> 8, ref Header.Ptr->_bheadO, Header.Ptr->_bheadO == 0);


            Header.Ptr->Size += BlockSize;

            return (Header.Ptr->Size >> 8) - 1;
        }

        private void _push_block(int bi, ref int head_out, bool empty)
        {
            //Console.WriteLine($"enters [{bi},{head_out},{Convert.ToByte(empty)}] (_push_block)");

            BlockMetadata* _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();
            BlockMetadata* bbi = _block + bi;

            if (empty)
            {
                // the destination is empty
                head_out = bbi->Prev = bbi->Next = bi;

                //Console.WriteLine($"_block[{bi}].Next = {_block[bi].Next} (_push_block)");
                //Console.WriteLine($"_block[{bi}].Prev = {_block[bi].Prev} (_push_block)");                    
            }
            else
            {
                int tail_out = _block[head_out].Prev;
                bbi->Prev = tail_out;
                bbi->Next = head_out;

                //Console.WriteLine($"_block[{bi}].Next = {tail_out} (_push_block)");
                //Console.WriteLine($"_block[{bi}].Prev = {head_out} (_push_block)");
                //Console.WriteLine($"_block[{tail_out}].Next = {bi} (_push_block)");
                //Console.WriteLine($"_block[{head_out}].Prev = {bi} (_push_block)");

                _block[tail_out].Next = bi;
                _block[head_out].Prev = bi;
                head_out = bi;
            }

            //Console.WriteLine($"returns [{head_out}] (_push_block)");
        }

        private void _push_sibling(long from, int @base, byte label, bool flag = true)
        {

            var _ninfo = (NodeInfo*)Blocks.DirectWrite<NodeInfo>();

            //Console.WriteLine($"enters [{from},{@base},{(int)label},{(flag ? 1 : 0)}] (_push_sibling)");

            bool changeChild = true;
            long current = from;
            byte c = _ninfo[current].Child;
            if (flag && label > c )
            {
                do
                {
                    changeChild = false;
                    current = @base ^ c;
                    c = _ninfo[current].Sibling;
                }
                while (c != 0 && c < label);
            }

            //Console.WriteLine($"\t_ninfo[{ @base ^ label }].Sibling = {c.ToString()} (_push_sibling)");

            _ninfo[@base ^ label].Sibling = c;

            if (changeChild)
            {
                //Console.WriteLine($"\t_ninfo[{current}].Child = {label} (_push_sibling)");
                _ninfo[current].Child = label;
            }
            else
            {
                //Console.WriteLine($"\t_ninfo[{current}].Sibling = {label} (_push_sibling)");
                _ninfo[current].Sibling = label;
            }
        }

        /// <summary>
        /// Resolve conflict on base_n ^ label_n = base_p ^ label_p
        /// </summary>
        private int _resolve(long from_n, int base_n, byte label_n)
        {
            var _array = (Node*)Blocks.DirectWrite<Node>();
            var _ninfo = (NodeInfo*)Blocks.DirectWrite<NodeInfo>();

            //Console.WriteLine($"enters [{from_n}, {base_n}, {label_n}] (_resolve)");

            // examine siblings of conflicted nodes
            int to_pn = base_n ^ label_n;
            int from_p = _array[to_pn].Check;
            int base_p = _array[from_p].Base;

            // whether to replace siblings of newly added
            bool flag = _consult(base_n, base_p, _ninfo[from_n].Child, _ninfo[from_p].Child);

            // TODO: Check performance impact of this. 
            byte* child = stackalloc byte[BlockSize];
            byte* first = child;
            byte* last = flag ? _set_child(first, base_n, _ninfo[from_n].Child, label_n) : _set_child(first, base_p, _ninfo[from_p].Child);

            int @base = (first == last ? _find_place() : _find_place(first, last)) ^ *first;

            // replace & modify empty list
            int from = flag ? (int)from_n : from_p;
            int base_ = flag ? base_n : base_p;

            //Console.WriteLine($"base_[{base_}], from[{from}], to_pn[{to_pn}], from_p[{from_p}], base_p[{base_p}] (_resolve)");

            if (flag && *first == label_n)
            {
                //Console.WriteLine($"_ninfo[{from}].Child = {label_n} (_resolve)");
                _ninfo[from].Child = label_n; // new child
            }


            //Console.WriteLine($"_array[{from}].Base = {@base} (_resolve)");
            _array[from].Base = (short) @base; // new base

            for (byte* p = first; p <= last; p++)
            {
                // to_ => to
                int to = _pop_enode(@base, *p, (short) from);
                int to_ = base_ ^ *p;

                //Console.WriteLine($"to[{to}], to_[{to_}] (_resolve)");

                //Console.WriteLine($"_ninfo[{to}].Sibling = {(byte)(p == last ? 0 : *(p + 1))} (_resolve)");
                _ninfo[to].Sibling = (byte)(p == last ? 0 : *(p + 1));

                if (flag && to_ == to_pn) // skip newcomer (no child)
                    continue;

                //Console.WriteLine($"_array[{to}].Base = {_array[to_].Base} (_resolve)");
                _array[to].Base = _array[to_].Base;
                if (_array[to_].Base > 0 && *p != 0)
                {
                    // copy base; bug fix
                    //Console.WriteLine($"_ninfo[{to}].Child = {_ninfo[to_].Child} (_resolve)");
                    byte c = _ninfo[to].Child = _ninfo[to_].Child;
                    do
                    {
                        int toBase = _array[to].Base ^ c;
                        //Console.WriteLine($"_array[{toBase}].Check = {to} (_resolve)");
                        _array[toBase].Check = (short) to;
                        c = _ninfo[toBase].Sibling;
                    }
                    while (c != 0);
                }

                if (!flag && to_ == (int)from_n) // parent node moved
                    from_n = (long)to;

                if (!flag && to_ == to_pn)
                {
                    // the address is immediately used
                    _push_sibling(from_n, to_pn ^ label_n, label_n);

                    //Console.WriteLine($"_ninfo[{to_}].Child = 0 (_resolve)");
                    _ninfo[to_].Child = 0; // remember to reset child

                    if (label_n != 0)
                    {
                        //Console.WriteLine($"_array[{to_}].Base = -1 (_resolve)");
                        _array[to_].Base = -1;
                    }
                    else
                    {
                        // //Console.WriteLine($"_array[{to_}].Value = 0 (_resolve)");
                        _array[to_].Value = 0;
                    }

                    //Console.WriteLine($"_array[{to_}].Check = {(int)from_n} (_resolve)");
                    _array[to_].Check = (short)from_n;
                }
                else
                {
                    _push_enode(to_);
                }
            }

            //Console.WriteLine($"returns [{(flag ? @base ^ label_n : to_pn)}] (_resolve)");

            return flag ? @base ^ label_n : to_pn;
        }

        private void _push_enode(int e)
        {
            Header.SetWritable();

            var _array = (Node*)Blocks.DirectWrite<Node>();
            var _ninfo = (NodeInfo*)Blocks.DirectWrite<NodeInfo>();
            var _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();

            //Console.WriteLine($"enters [{e}] (_push_enode)");

            int bi = e >> 8;

            if (++_block[bi].Num == 1)
            {
                // Full to Closed
                _block[bi].Ehead = e;
                _array[e] = new Node(-e, -e);
                //Console.WriteLine($"_array[{e}].Base = {_array[e].Base} (_push_enode)");
                //Console.WriteLine($"_array[{e}].Check = {_array[e].Check} (_push_enode)");
                if (bi != 0)
                    _transfer_block(bi, ref Header.Ptr->_bheadF, ref Header.Ptr->_bheadC); // Full to Closed
            }
            else
            {
                int prev = _block[bi].Ehead;
                int next = -_array[prev].Check;
                _array[e] = new Node(-prev, -next);
                //Console.WriteLine($"_array[{e}].Base = {_array[e].Base} (_push_enode)");
                //Console.WriteLine($"_array[{e}].Check = {_array[e].Check} (_push_enode)");

                _array[prev].Check = _array[next].Base = (short) -e;
                //Console.WriteLine($"_array[{prev}].Check = {_array[prev].Check} (_push_enode)");
                //Console.WriteLine($"_array[{next}].Base = {_array[next].Base} (_push_enode)");

                if (_block[bi].Num == 2 || _block[bi].Trial == _maxTrial)
                {
                    // Closed to Open
                    if (bi != 0)
                        _transfer_block(bi, ref Header.Ptr->_bheadC, ref Header.Ptr->_bheadO);
                }
                _block[bi].Trial = 0;
            }

            if (_block[bi].Reject < Header.Ptr->Reject[_block[bi].Num])
                _block[bi].Reject = Header.Ptr->Reject[_block[bi].Num];

            _ninfo[e] = new NodeInfo();
            //Console.WriteLine($"_ninfo[{e}] = [{_ninfo[e].Child},{_ninfo[e].Sibling}] (_push_enode)");
        }


        private int _find_place(byte* first, byte* last)
        {
            //Console.WriteLine($"enters [{*first},{*last}] (_find_place)");

            int bi = Header.Ptr->_bheadO;
            if (bi != 0)
            {
                Header.SetWritable();

                var _block = (BlockMetadata*)Blocks.DirectWrite<BlockMetadata>();
                var _array = (Node*)Blocks.DirectWrite<Node>();

                int bz = _block[Header.Ptr->_bheadO].Prev;
                short nc = (short)(last - first + 1);
                while (true)
                {
                    if (_block[bi].Num >= nc && nc < _block[bi].Reject) // explore configuration
                    {
                        int e = _block[bi].Ehead;
                        while (true)
                        {
                            int @base = e ^ *first;
                            for (byte* p = first; _array[@base ^ *(++p)].Check < 0;)
                            {
                                if (p == last)
                                {
                                    _block[bi].Ehead = e;
                                    return e;
                                }
                            }

                            e = -_array[e].Check;
                            if (e == _block[bi].Ehead)
                                break;
                        }
                    }

                    _block[bi].Reject = nc;
                    if (_block[bi].Reject < Header.Ptr->Reject[_block[bi].Num])
                        Header.Ptr->Reject[_block[bi].Num] = _block[bi].Reject;

                    int bi_ = _block[bi].Next;
                    _block[bi].Trial++;
                    if (_block[bi].Trial == _maxTrial)
                        _transfer_block(bi, ref Header.Ptr->_bheadO, ref Header.Ptr->_bheadC);

                    Debug.Assert(_block[bi].Trial <= _maxTrial);

                    if (bi == bz)
                        break;

                    bi = bi_;
                }
            }

            int result = _add_block() << 8;
            //Console.WriteLine($"returns: {result} (_find_place)");
            return result;

        }

        private byte* _set_child(byte* p, int @base, byte c)
        {
            unchecked
            {
                return _set_child(p, @base, c, -1);
            }
        }

        private byte* _set_child(byte* p, int @base, byte c, int label)
        {
            //Console.WriteLine($"enters [{*p},{@base},{c},{label}] (_set_child)");

            p--;
            if (c == 0)
            {
                // 0 is a terminal character.
                p++;
                *p = c;
                c = Blocks.NodesInfo[@base ^ c].Sibling;
            }


            while (c != 0 && c < label)
            {
                p++;
                *p = c;
                c = Blocks.NodesInfo[@base ^ c].Sibling;
            }


            if (label != -1)
            {
                p++;
                *p = (byte)label;
            }

            while (c != 0)
            {
                p++;
                *p = c;
                c = Blocks.NodesInfo[@base ^ c].Sibling;
            }

            return p;
        }

        private bool _consult(int base_n, int base_p, byte c_n, byte c_p)
        {
            //Console.WriteLine($"enters [{base_n},{base_p},{c_n},{c_p}] (_consult)");

            do
            {
                //Console.WriteLine($"\tc_n: _ninfo[{base_n ^ c_n}].sibling = {_ninfo[base_n ^ c_n].Sibling} (_consult)");
                c_n = Blocks.NodesInfo[base_n ^ c_n].Sibling;
                //Console.WriteLine($"\tc_p: _ninfo[{base_p ^ c_p}].sibling = {_ninfo[base_p ^ c_p].Sibling} (_consult)");
                c_p = Blocks.NodesInfo[base_p ^ c_p].Sibling;
            }
            while (c_n != 0 && c_p != 0);

            //Console.WriteLine($"returns: { (c_p != 0 ? 1 : 0)}  (_consult)");
            return c_p != 0;
        }

        private CedarResultCode _find(byte* key, ref long from, ref long pos, int len, out short result)
        {
            result = 0;

            long offset = from >> 32;
            if (offset == 0)
            {

                Node* _array = Blocks.Nodes;

                // node on trie
                for (byte* key_ = key; _array[from].Base >= 0;)
                {
                    if (pos == len)
                    {
                        int offset_ = _array[from].Base ^ 0;
                        Node* n = &_array[offset_];
                        if (n->Check != from)
                            return CedarResultCode.NoValue;

                        result = n->Value;
                        return CedarResultCode.Success;
                    }

                    int to = _array[from].Base ^ key_[pos];
                    if (_array[to].Check != from)
                        return CedarResultCode.NoPath;

                    pos++;
                    from = to;
                }

                offset = -_array[from].Base;
            }

            // switch to _tail to match suffix
            long pos_orig = pos; // start position in reading _tail

            byte* tail = Tail.DirectRead(offset - pos);

            if (pos < len)
            {
                do
                {
                    if (key[pos] != tail[pos])
                        break;
                }
                while (++pos < len);

                long moved = pos - pos_orig;
                if (moved != 0)
                {
                    from &= TAIL_OFFSET_MASK;
                    from |= (offset + moved) << 32;
                }

                if (pos < len)
                    return CedarResultCode.NoPath; // input > tail, input != tail
            }

            if (tail[pos] != 0)
                return CedarResultCode.NoValue; // input < tail

            result = *(short*)(tail + len + 1);
            return CedarResultCode.Success;
        }

        public CedarResultCode ExactMatchSearch<TResult>(Slice key, out TResult value, out CedarDataPtr* ptr, long from = 0)
            where TResult : struct, ICedarResult
        {
            return ExactMatchSearch(key.Content.Ptr, key.Content.Length, out value, out ptr, from);
        }

        public CedarResultCode ExactMatchSearch<TResult>(byte* key, int size, out TResult value, out CedarDataPtr* ptr, long from = 0)
            where TResult : struct, ICedarResult
        {
            long pos = 0;

            short r;
            var errorCode = _find(key, ref from, ref pos, size, out r);
            if (errorCode == CedarResultCode.NoPath)
                errorCode = CedarResultCode.NoValue;

            value = default(TResult);
            value.SetResult(r, size, from);

            if (errorCode == CedarResultCode.Success)
            {
                ptr = Data.DirectRead(r);
                Debug.Assert(ptr->Flags == CedarNodeFlags.Data);
            }
            else
            {
                ptr = null;
            }

            return errorCode;
        }
    }
}
