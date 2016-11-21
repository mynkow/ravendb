using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Sparrow;
using Voron.Data.BTrees.Cedar;
using Voron.Impl;

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
                NodesReadPtr array = Blocks.Nodes;

                int i = 0;
                for (int to = 0; to < Header.Ptr->Size; to++)
                {
                    if (array[to]->Check >= 0)
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
                NodesReadPtr array = Blocks.Nodes;

                int i = 0;
                int j = 0;
                for (int to = 0; to < Header.Ptr->Size; to++)
                {
                    Node* n = array[to];
                    if (n->Check >= 0 && array[n->Check]->Base != to && n->Base < 0)
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
                NodesReadPtr array = Blocks.Nodes;

                int i = 0;
                for (int to = 0; to < Header.Ptr->Size; to++)
                {
                    var n = array[to];
                    if (n->Check >= 0 && (array[n->Check]->Base == to || n->Base < 0))
                        i++;
                }

                return i;
            }
        }

        internal void Validate(long from = 0)
        {
            int items = 0;
            var nodes = new HashSet<short>();
            Validate(from, nodes, ref items);

            if (items != Header.Ptr->NumberOfEntries)
                throw new Exception("Counted items and registered items do not match.");
        }

        /// <summary>
        /// This value is intended to be used only for diagnostics purposes.
        /// </summary>
        internal void Validate(long from, HashSet<short> nodes, ref int count)
        {
            NodesReadPtr array = Blocks.Nodes;
            NodesInfoReadPtr nInfo = Blocks.NodesInfo;

            int @base = array[from]->Base;
            if (@base < 0)
            {
                // validate tail offset
                if (Tail.Length < -@base + 1 + sizeof(short))
                    throw new Exception($"Fail in tail offset {from}");

                var tail = Tail - @base;
                int len = tail.StrLength();
                short v = tail.Read<short>(len + 1);

                if (nodes.Contains(v))
                    throw new Exception($"Node already seen. Multiple tails cannot share data nodes.");

                // Try to read the node.                 
                Data.DirectRead(v);

                nodes.Add(v);
                count++;

                return;
            }

            byte c = nInfo[from]->Child;
            do
            {
                if (from != 0)
                {
                    var n = array[@base ^ c];
                    if (n->Check != from)
                        throw new Exception($"");

                    if (n->Check >= 0 && array[n->Check]->Base == (@base ^ c))
                    {
                        short v = n->Value;

                        if (nodes.Contains(v))
                            throw new Exception($"Node already seen. Multiple tails cannot share data nodes.");

                        // Try to read the node.                 
                        Data.DirectRead(v);

                        nodes.Add(v);
                        count++;
                    }
                }
                if (c != 0)
                {
                    Validate(@base ^ c, nodes, ref count);
                }

                c = nInfo[@base ^ c]->Sibling;
            }
            while (c != 0);
        }
        
        private struct ShrinkHelper
        {
            public readonly byte* Tail;

            public int Length
            {
                get { return *((int*)Tail); }
                set { *((int*)Tail) = value; }
            }

            public ShrinkHelper( byte* tail )
            {
                this.Tail = tail;
            }
        }

        internal void ShrinkTail()
        {
            TailAccessor _tail = Tail;
            NodesWritePtr _array = Blocks.DirectWrite<NodesWritePtr>();

            int _size = Header.Ptr->Size;
            int tailBytes = Tail.TotalBytes;

            ByteString tempTail;
            using (_llt.Allocator.Allocate(tailBytes, out tempTail))
            {
                Memory.Set(tempTail.Ptr, 0, tailBytes); // Zero out the memory

                ShrinkHelper t = new ShrinkHelper(tempTail.Ptr) { Length = sizeof(int) };

                for (int to = 0; to < _size; ++to)
                {
                    Node* n = _array.Write(to);
                    if (n->Check >= 0 && _array.Read(n->Check)->Base != to && n->Base < 0)
                    {
                        byte* tail = &t.Tail[t.Length];

                        TailAccessor tail_ = _tail + (-n->Base);
                        n->Base = (short)-t.Length;

                        int written = tail_.CopyDataTo(tail);
                        t.Length += written; // Increment the length
                    }
                }

                _tail.Set(0); // Zero out the destination memory.
                _tail.CopyFrom(t.Tail, t.Length); // Copy the temp into the final.  

                Tail0.Reset();
            }
        }

        public CedarActionStatus Remove(Slice key, out ushort nodeVersion)
        {            
            //Console.WriteLine($"Begin Erase with len={len}");
            long pos = 0;
            long from = 0;            

            short value;
            var i = _find(key.Content.Ptr, ref from, ref pos, key.Content.Length, out value);
            if (i != CedarResultCode.Success)
            {
                nodeVersion = 0;
                return CedarActionStatus.NotFound;
            }

            if (from >> 32 != 0)
            {
                // leave tail as is
                from &= TAIL_OFFSET_MASK;
            }

            Header.SetWritable();
            NodesInfoWritePtr _ninfo = Blocks.DirectWrite<NodesInfoWritePtr>();
            NodesReadPtr _array = Blocks.Nodes;

            bool flag = _array[from]->Base < 0; // have siblings
            int e = flag ? (int)from : _array[from]->Base ^ 0;
            from = _array[e]->Check;

            //Console.WriteLine($"flag={(flag ? 1 : 0)}, e={e}, from={from} (erase)");

            
            do
            {
                Node* n = _array[from];

                //Console.WriteLine($"n.Base={n.Base}, _ninfo[{from}].Child={_ninfo[from].Child} (erase)");
                //Console.WriteLine($"_ninfo[{(n.Base ^ _ninfo[from].Child)}].Sibling={_ninfo[n.Base ^ _ninfo[from].Child].Sibling} (erase)");

                flag = _ninfo.Write(n->Base ^ _ninfo.Read(from)->Child)->Sibling != 0;

                //Console.WriteLine($"flag={(flag ? 1 : 0)}, e={e}, from={from}, n.Base={n.Base}, n.Check={n.Check} (erase)");

                if (flag)
                    _pop_sibling(ref _ninfo, from, n->Base, (byte)(n->Base ^ e));

                _push_enode(e);
                e = (int)from;
                from = n->Check;
            }
            while (!flag);

            //Console.WriteLine($"_ninfo[264] = [{_ninfo[264].Child},{_ninfo[264].Sibling}] (erase)");
            //Console.WriteLine($"End Erase with len={len}");

            // Get the node version.
            nodeVersion = Data.DirectRead(value)[0]->Version;

            // Free the storage node.
            Data.FreeNode(value);

            Header.Ptr->NumberOfEntries--;

            return CedarActionStatus.Success;
        }

        public CedarActionStatus Update(Slice key, int size, out CedarDataNode* ptr, ushort? version = null, CedarNodeFlags nodeFlag = CedarNodeFlags.Data)
        {
            ptr = null; // Default invalid value.

            if (!Data.CanAllocateNode())
                return CedarActionStatus.NotEnoughSpace;

            // TODO: Make sure that updates (not inserts) will not consume a node allocation. 
            short index;
            long from = 0;
            long tailPos = 0;
            CedarActionStatus result = Update(key.Content.Ptr, key.Content.Length, out index, ref from, ref tailPos);            
            if (result == CedarActionStatus.NotEnoughSpace)
                return result; // We failed.

            ptr = Data.DirectWrite(index);
            ptr->Flags = nodeFlag;
            ptr->Version = version ?? 0;
            ptr->DataSize = (byte)size;

            return result;
        }

        private CedarActionStatus Update(byte* key, int len, out short value, ref long from, ref long pos)
        {
            value = -1; // Default invalid value.

            if (len == 0 && from == 0)
                throw new ArgumentException("failed to insert zero-length key");

            // We are being conservative if there is not enough space in the tail to write it entirely, we wont continue.
            if (Tail.Length + len + sizeof(short) >= Tail.TotalBytes)
                return CedarActionStatus.NotEnoughSpace;

            //Console.WriteLine($"Start {nameof(Update)} with key-size: {len}");


            // Chances are that we will need to Write on the array, so getting the write version is the way to go here. 
            NodesWritePtr _array = Blocks.DirectWrite<NodesWritePtr>();
            NodesInfoWritePtr _ninfo = Blocks.DirectWrite<NodesInfoWritePtr>();

            long offset = from >> 32;
            if (offset == 0)
            {
                //Console.WriteLine("Begin 1");

                for (; _array.Read(from)->Base >= 0; pos++)
                {
                    if (pos == len)
                    {
                        int current;                        
                        if ( !TryFollow(ref _array, ref _ninfo, from, 0, out current))
                            return CedarActionStatus.NotEnoughSpace;

                        Header.SetWritable();
                        Header.Ptr->NumberOfEntries++;

                        value = (short)Data.AllocateNode();
                        _array.Write(current)->Value = value;

                        return CedarActionStatus.Success;
                    }

                    if (!TryFollow(ref _array, ref _ninfo, from, key[pos], out from))
                        return CedarActionStatus.NotEnoughSpace;
                }

                //Console.WriteLine("End 1");

                offset = -_array.Read(from)->Base;
            }

            long pos_orig;
            TailAccessor tailPtr;
            if (offset >= sizeof(short)) // go to _tail
            {
                long moved;
                pos_orig = pos;

                tailPtr = Tail + (offset - pos);
                while (pos < len && key[pos] == tailPtr[(int)pos])
                    pos++;

                if (pos == len && tailPtr[(int)pos] == '\0')
                {

                    // we found an exact match
                    moved = pos - pos_orig;
                    if (moved != 0)
                    {
                        // search end on tail
                        from &= TAIL_OFFSET_MASK;
                        from |= (offset + moved) << 32;
                    }

                    TailAccessor ptr = tailPtr + (len + 1);
                    value = ptr.Read<short>();

                    //Console.WriteLine($"_tail[{tailPtr + (len + 1) - Tail.DirectRead()}] = {Unsafe.Read<short>(ptr)}");

                    return CedarActionStatus.Found;
                }

                // otherwise, insert the common prefix in tail if any
                if (from >> 32 != 0)
                {
                    from &= TAIL_OFFSET_MASK; // reset to update tail offset
                    for (int offset_ = -_array.Read(from)->Base; offset_ < offset;)
                    {
                        if (!TryFollow(ref _array, ref _ninfo, from, Tail[offset_], out from) )
                            return CedarActionStatus.NotEnoughSpace;

                        offset_++;
                    }

                    //Console.WriteLine();
                }

                //Console.WriteLine("Begin 2");

                for (long pos_ = pos_orig; pos_ < pos; pos_++)
                {
                    if (!TryFollow(ref _array, ref _ninfo, from, key[pos_], out from))
                        return CedarActionStatus.NotEnoughSpace;
                }
  
                //Console.WriteLine("End 2");                    

                moved = pos - pos_orig;
                if (tailPtr[(int)pos] != 0)
                {
                    // remember to move offset to existing tail
                    long to_;
                    if (!TryFollow(ref _array, ref _ninfo, from, tailPtr[(int)pos], out to_))
                        return CedarActionStatus.NotEnoughSpace;

                    moved++;

                    //Console.WriteLine($"_array[{to_}].Base = {-(int)(offset + moved)}");
                    _array.Write(to_)->Base = (short) -(offset + moved);

                    moved -= 1 + sizeof(short); // keep record
                }

                moved += offset;
                for (int i = (int)offset; i <= moved; i += 1 + sizeof(short))
                {
                    Tail0.SetWritable();
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

                if (pos == len || tailPtr[(int)pos] == '\0')
                {
                    long to;
                    if (!TryFollow(ref _array, ref _ninfo, from, 0, out to))
                        return CedarActionStatus.NotEnoughSpace;

                    if (pos == len)
                    {
                        value = (short) Data.AllocateNode();
                        
                        var n = _array.Write(to);
                        n->Value = value;

                        //Console.WriteLine($"_array[{to}].Value = {value}");

                        Header.SetWritable();
                        Header.Ptr->NumberOfEntries++;

                        return CedarActionStatus.Success;
                    }
                    else
                    {
                        short toValue = tailPtr.Read<short>((int) (pos + 1));

                        var n = _array.Write(to);
                        n->Value = toValue;

                        //Console.WriteLine($"_array[{to}].Value = {value}");
                    }
                }

                if (!TryFollow(ref _array, ref _ninfo, from, key[pos], out from))
                    return CedarActionStatus.NotEnoughSpace;

                pos++;
            }

            //
            int needed = (int)(len - pos + 1 + sizeof(short));
            if (pos == len && Tail0.Length != 0)
            {
                Tail0.SetWritable();

                int offset0 = Tail0[Tail0.Length];
                Tail.Write<byte>(offset0, 0);

                //Console.WriteLine($"_array[{from}].Base = {-offset0}");
                _array.Write(from)->Base = (short) -offset0;
                Tail0.Length = Tail0.Length - 1;

                //Console.WriteLine($"_tail[{offset0 + 1}] = {value}");
                value = (short) Data.AllocateNode();

                Tail.Write<short>(offset0 + 1, value);
                
                Header.SetWritable();
                Header.Ptr->NumberOfEntries++;

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

            //Console.WriteLine($"_array[{from}].Base = {-Tail.Length}");
            _array.Write(from)->Base = (short) -Tail.Length;
            pos_orig = pos;

            tailPtr = Tail + (Tail.Length - pos);
            if (pos < len)
            {

                tailPtr.CopyFrom((int)pos, key + pos, len - (int)pos);

                from |= ((long) (Tail.Length) + (len - pos_orig)) << 32;
            }

            Tail.Length += needed;

            value = (short)Data.AllocateNode();
            tailPtr.Write<short>(len + 1, value);

            Header.SetWritable();
            Header.Ptr->NumberOfEntries++;

            //Console.WriteLine($"_tail[{tailPtr + (len + 1) - Tail.DirectRead()}] = {value}");


            //Console.WriteLine($"End {nameof(Update)} with key-size: {len}");

            return CedarActionStatus.Success;
        }

        private bool TryFollow(ref NodesWritePtr _array, ref NodesInfoWritePtr _ninfo, long from, byte label, out long to)
        {
            int to_;
            var result = TryFollow(ref _array, ref _ninfo, from, label, out to_);
            to = to_;
            return result;
        }

        private bool TryFollow(ref NodesWritePtr _array, ref NodesInfoWritePtr _ninfo, long from, byte label, out int to)
        {
            int @base = _array.Read(from)->Base;
            if (@base < 0 || _array.Read(to = @base ^ label)->Check < 0) // TODO: Check if the rules are the same here as in C++
            {
                if (!_try_pop_enode(ref _array, @base, label, (short)from, out to))
                    return false;

                _push_sibling(ref _ninfo, from, to ^ label, label, @base >= 0);
            }
            else if (_array.Read(to)->Check != (int)from)
            {
                if (!_try_resolve(ref _array, from, @base, label, out to))
                    return false;
            }

            //Console.WriteLine($"F->{to}");

            return true;
        }

        private bool _try_pop_enode(ref NodesWritePtr _array, int @base, byte label, short from, out int e)
        {
            //Console.WriteLine($"enters [{@base},{label},{from}] (_pop_enode)");

            Header.SetWritable();

            BlockMetadataWritePtr _block = Blocks.DirectWrite<BlockMetadataWritePtr>();

            if (@base < 0)
            {
                if (!_try_find_place(ref _block, out e))
                    return false;                    
            }
            else
            {
                e = @base ^ label;
            }
            
            int bi = e >> 8;

            BlockMetadata* bbi = _block.Write(bi);
            Node* ae = _array.Write(e);

            bbi->Num--;
            if (bbi->Num == 0)
            {
                if (bi != 0)
                    _transfer_block(bi, ref Header.Ptr->_bheadC, ref Header.Ptr->_bheadF); // Closed to Full
            }
            else // release empty node from empty ring
            {                
                _array.Write(-(ae->Base))->Check = ae->Check;
                //Console.WriteLine($"_array[{-(_array[e].Check)}].Base = {_array[e].Base}");
                _array.Write(-(ae->Check))->Base = ae->Base;

                if (e == bbi->Ehead)
                    bbi->Ehead = -(ae->Check); // set ehead

                if (bi != 0 && bbi->Num == 1 && bbi->Trial != _maxTrial) // Open to Closed
                    _transfer_block(bi, ref Header.Ptr->_bheadO, ref Header.Ptr->_bheadC);
            }

            // initialize the released node
            if (label != 0)
            {
                //Console.WriteLine($"_array[{e}].Base = -1");
                ae->Base = -1;
            }
            else
            {
                //Console.WriteLine($"_array[{e}].Value = 0");
                ae->Value = 0;

                Debug.Assert(ae->Base == 0);
            }

            ae->Check = from;

            if (@base < 0)
            {
                _array.Write(from)->Base = (short) (e ^ label);
                //Console.WriteLine($"_array[{from}].Base = {e ^ label}");
            }

            //Console.WriteLine($"returns: {e} (_pop_enode)");
            return true;
        }

        private void _transfer_block(int bi, ref int head_in, ref int head_out)
        {
            //Console.WriteLine($"enters [{bi},{head_in},{head_out}] (_transfer_block)");
            BlockMetadataWritePtr _block = Blocks.DirectWrite<BlockMetadataWritePtr>();

            _pop_block(ref _block, bi, ref head_in, bi == _block.Read(bi)->Next);
            _push_block(ref _block, bi, ref head_out, head_out == 0 && _block.Read(bi)->Num != 0);
        }

        private void _pop_block(ref BlockMetadataWritePtr _block, int bi, ref int head_in, bool last)
        {
            //Console.WriteLine($"enters [{bi},{head_in},{Convert.ToByte(last)}] (_pop_block)");

            if (last)
            {
                // last one poped; Closed or Open
                head_in = 0;
            }
            else
            {
                _block.Write(_block.Read(bi)->Prev)->Next = _block.Read(bi)->Next;
                //Console.WriteLine($"_block[{_block[bi].Prev}].Next = {_block[bi].Next} (_pop_block)");

                _block.Write(_block.Read(bi)->Next)->Prev = _block.Read(bi)->Prev;
                //Console.WriteLine($"_block[{_block[bi].Next}].Prev = {_block[bi].Prev} (_pop_block)");

                if (bi == head_in)
                    head_in = _block.Read(bi)->Next;
            }

            //Console.WriteLine($"returns [{head_in}] (_pop_block)");
        }

        private bool _try_find_place(ref BlockMetadataWritePtr _block, out int result)
        {
            //Console.WriteLine($"enters [] (_find_place)");

            if (Header.Ptr->_bheadC != 0)
            {
                //Console.WriteLine($"returns: {_block[Header.Ptr->_bheadC].Ehead} (_find_place)");
                result = _block.Read(Header.Ptr->_bheadC)->Ehead;
                return true;
            }

            if (Header.Ptr->_bheadO != 0)
            {
                //Console.WriteLine($"returns: {_block[Header.Ptr->_bheadO].Ehead} (_find_place)");
                result = _block.Read(Header.Ptr->_bheadO)->Ehead;
                return true;
            }

            if (Header.Ptr->Size != Header.Ptr->Capacity)
            {
                result = _add_block() << 8;
                //Console.WriteLine($"returns: {result} (_find_place)");

                return true;
            }

            result = -1;
            return false;
        }

        private int _add_block()
        {
            //Console.WriteLine($"enters [] (_add_block)");

            Header.SetWritable();

            NodesWritePtr _array = Blocks.DirectWrite<NodesWritePtr>();
            BlockMetadataWritePtr _block = Blocks.DirectWrite<BlockMetadataWritePtr>();

            int size = Header.Ptr->Size;

            _block.Write(size >> 8)->Ehead = size;
            _array.Write(size)->Set(-(size + 255), -(size + 1));
            for (int i = size + 1; i < size + 255; i++)
                _array.Write(i)->Set(-(i - 1), -(i + 1));

            _array.Write(size + 255)->Set(-(size + 254), -size);
            _push_block(ref _block, size >> 8, ref Header.Ptr->_bheadO, Header.Ptr->_bheadO == 0);

            Header.Ptr->Size += BlockSize;

            return (Header.Ptr->Size >> 8) - 1;
        }

        private void _push_block(ref BlockMetadataWritePtr _block, int bi, ref int head_out, bool empty)
        {
            //Console.WriteLine($"enters [{bi},{head_out},{Convert.ToByte(empty)}] (_push_block)");
            
            BlockMetadata* bbi = _block.Write(bi);
            if (empty)
            {
                // the destination is empty
                head_out = bbi->Prev = bbi->Next = bi;

                //Console.WriteLine($"_block[{bi}].Next = {_block[bi].Next} (_push_block)");
                //Console.WriteLine($"_block[{bi}].Prev = {_block[bi].Prev} (_push_block)");                    
            }
            else
            {
                BlockMetadata* bho = _block.Write(head_out);
                int tail_out = bho->Prev;
                bbi->Prev = tail_out;
                bbi->Next = head_out;

                //Console.WriteLine($"_block[{bi}].Next = {tail_out} (_push_block)");
                //Console.WriteLine($"_block[{bi}].Prev = {head_out} (_push_block)");
                //Console.WriteLine($"_block[{tail_out}].Next = {bi} (_push_block)");
                //Console.WriteLine($"_block[{head_out}].Prev = {bi} (_push_block)");

                _block.Write(tail_out)->Next = bi;
                bho->Prev = bi;
                head_out = bi;
            }

            //Console.WriteLine($"returns [{head_out}] (_push_block)");
        }

        private void _pop_sibling(ref NodesInfoWritePtr _ninfo, long from, int @base, byte label)
        {
            //Console.WriteLine($"enters [{from},{@base},{(int)label}] (_pop_sibling)");            

            bool changeChild = true;

            byte c = _ninfo.Read(from)->Child;
            while (c != label)
            {
                changeChild = false;

                from = @base ^ c;
                c = _ninfo.Read(from)->Sibling;
            }

            byte sibling = _ninfo.Read(@base ^ label)->Sibling;
            if (changeChild)
            {
                //Console.WriteLine($"\t_ninfo[{from}].Child = {sibling} (_pop_sibling)");
                _ninfo.Write(from)->Child = sibling;
            }
            else
            {
                //Console.WriteLine($"\t_ninfo[{@base ^ c}].Sibling = {sibling} (_pop_sibling)");
                _ninfo.Write(from)->Sibling = sibling;
            }
        }

        private void _push_sibling(ref NodesInfoWritePtr _ninfo, long from, int @base, byte label, bool flag = true)
        {
            //Console.WriteLine($"enters [{from},{@base},{(int)label},{(flag ? 1 : 0)}] (_push_sibling)");

            bool changeChild = true;
            long current = from;
            byte c = _ninfo.Read(current)->Child;
            if (flag && label > c )
            {
                do
                {
                    changeChild = false;
                    current = @base ^ c;
                    c = _ninfo.Read(current)->Sibling;
                }
                while (c != 0 && c < label);
            }

            //Console.WriteLine($"\t_ninfo[{ @base ^ label }].Sibling = {c.ToString()} (_push_sibling)");

            _ninfo.Write(@base ^ label)->Sibling = c;

            if (changeChild)
            {
                //Console.WriteLine($"\t_ninfo[{current}].Child = {label} (_push_sibling)");
                _ninfo.Write(current)->Child = label;
            }
            else
            {
                //Console.WriteLine($"\t_ninfo[{current}].Sibling = {label} (_push_sibling)");
                _ninfo.Write(current)->Sibling = label;
            }
        }

        /// <summary>
        /// Resolve conflict on base_n ^ label_n = base_p ^ label_p
        /// </summary>
        private bool _try_resolve(ref NodesWritePtr _array, long from_n, int base_n, byte label_n, out int result)
        {
            BlockMetadataWritePtr _block = Blocks.DirectWrite<BlockMetadataWritePtr>();
            NodesInfoWritePtr _ninfo = Blocks.DirectWrite<NodesInfoWritePtr>();            

            //Console.WriteLine($"enters [{from_n}, {base_n}, {label_n}] (_resolve)");

            // examine siblings of conflicted nodes
            int to_pn = base_n ^ label_n;
            int from_p = _array.Read(to_pn)->Check;
            int base_p = _array.Read(from_p)->Base;

            // whether to replace siblings of newly added
            bool flag = _consult(base_n, base_p, _ninfo.Read(from_n)->Child, _ninfo.Read(from_p)->Child);

            // TODO: Check performance impact of this. 
            byte* child = stackalloc byte[BlockSize];
            byte* first = child;
            byte* last = flag ? _set_child(first, base_n, _ninfo.Read(from_n)->Child, label_n) : _set_child(first, base_p, _ninfo.Read(from_p)->Child);

            int @base;
            if (first == last)
            {
                if (!_try_find_place(ref _block, out @base))
                {
                    result = -1;
                    return false;
                }                    
            }
            else
            {
                if (!_try_find_place(first, last, out @base))
                {
                    result = -1;
                    return false;
                }
            }

            @base ^= *first;

            // replace & modify empty list
            int from = flag ? (int)from_n : from_p;
            int base_ = flag ? base_n : base_p;

            //Console.WriteLine($"base_[{base_}], from[{from}], to_pn[{to_pn}], from_p[{from_p}], base_p[{base_p}] (_resolve)");

            if (flag && *first == label_n)
            {
                //Console.WriteLine($"_ninfo[{from}].Child = {label_n} (_resolve)");
                _ninfo.Write(from)->Child = label_n; // new child
            }


            //Console.WriteLine($"_array[{from}].Base = {@base} (_resolve)");
            _array.Write(from)->Base = (short) @base; // new base

            for (byte* p = first; p <= last; p++)
            {
                // to_ => to
                int to;
                if (!_try_pop_enode(ref _array, @base, *p, (short) from, out to))
                {
                    result = -1;
                    return false;
                }
                    

                int to_ = base_ ^ *p;

                //Console.WriteLine($"to[{to}], to_[{to_}] (_resolve)");

                //Console.WriteLine($"_ninfo[{to}].Sibling = {(byte)(p == last ? 0 : *(p + 1))} (_resolve)");
                _ninfo.Write(to)->Sibling = (byte)(p == last ? 0 : *(p + 1));

                if (flag && to_ == to_pn) // skip newcomer (no child)
                    continue;

                //Console.WriteLine($"_array[{to}].Base = {_array[to_].Base} (_resolve)");
                _array.Write(to)->Base = _array.Read(to_)->Base;
                if (_array.Read(to_)->Base > 0 && *p != 0)
                {
                    // copy base; bug fix
                    //Console.WriteLine($"_ninfo[{to}].Child = {_ninfo[to_].Child} (_resolve)");
                    byte c = _ninfo.Write(to)->Child = _ninfo.Read(to_)->Child;
                    do
                    {
                        int toBase = _array.Read(to)->Base ^ c;
                        //Console.WriteLine($"_array[{toBase}].Check = {to} (_resolve)");
                        _array.Write(toBase)->Check = (short) to;
                        c = _ninfo.Read(toBase)->Sibling;
                    }
                    while (c != 0);
                }

                if (!flag && to_ == (int)from_n) // parent node moved
                    from_n = (long)to;

                if (!flag && to_ == to_pn)
                {
                    // the address is immediately used
                    _push_sibling(ref _ninfo, from_n, to_pn ^ label_n, label_n);

                    //Console.WriteLine($"_ninfo[{to_}].Child = 0 (_resolve)");
                    _ninfo.Write(to_)->Child = 0; // remember to reset child

                    if (label_n != 0)
                    {
                        //Console.WriteLine($"_array[{to_}].Base = -1 (_resolve)");
                        _array.Write(to_)->Base = -1;
                    }
                    else
                    {
                        // //Console.WriteLine($"_array[{to_}].Value = 0 (_resolve)");
                        _array.Write(to_)->Value = 0;
                    }

                    //Console.WriteLine($"_array[{to_}].Check = {(int)from_n} (_resolve)");
                    _array.Write(to_)->Check = (short)from_n;
                }
                else
                {
                    _push_enode(to_);
                }
            }

            //Console.WriteLine($"returns [{(flag ? @base ^ label_n : to_pn)}] (_resolve)");

            result = flag ? @base ^ label_n : to_pn;
            return true;
        }

        private void _push_enode(int e)
        {
            Header.SetWritable();

            // TODO: Get rid of this, its like 50 assembler instructions (if not more).
            NodesWritePtr _array = Blocks.DirectWrite<NodesWritePtr>();
            NodesInfoWritePtr _ninfo = Blocks.DirectWrite<NodesInfoWritePtr>();
            BlockMetadataWritePtr _block = Blocks.DirectWrite<BlockMetadataWritePtr>();

            //Console.WriteLine($"enters [{e}] (_push_enode)");

            int bi = e >> 8;

            BlockMetadata* bbi = _block.Write(bi);

            bbi->Num++;
            if (bbi->Num == 1)
            {
                // Full to Closed
                bbi->Ehead = e;

                _array.Write(e)->Set(-e, -e);
                //Console.WriteLine($"_array[{e}].Base = {_array[e].Base} (_push_enode)");
                //Console.WriteLine($"_array[{e}].Check = {_array[e].Check} (_push_enode)");
                if (bi != 0)
                    _transfer_block(bi, ref Header.Ptr->_bheadF, ref Header.Ptr->_bheadC); // Full to Closed
            }
            else
            {
                int prev = bbi->Ehead;
                int next = -_array.Read(prev)->Check;

                _array.Write(e)->Set(-prev, -next);
                //Console.WriteLine($"_array[{e}].Base = {_array[e].Base} (_push_enode)");
                //Console.WriteLine($"_array[{e}].Check = {_array[e].Check} (_push_enode)");

                _array.Write(prev)->Check = _array.Write(next)->Base = (short) -e;
                //Console.WriteLine($"_array[{prev}].Check = {_array[prev].Check} (_push_enode)");
                //Console.WriteLine($"_array[{next}].Base = {_array[next].Base} (_push_enode)");

                if (bbi->Num == 2 || bbi->Trial == _maxTrial)
                {
                    // Closed to Open
                    if (bi != 0)
                        _transfer_block(bi, ref Header.Ptr->_bheadC, ref Header.Ptr->_bheadO);
                }
                bbi->Trial = 0;
            }

            if (bbi->Reject < Header.Ptr->Reject[bbi->Num])
                bbi->Reject = Header.Ptr->Reject[bbi->Num];

            _ninfo.Write(e)->Reset();
            //Console.WriteLine($"_ninfo[{e}] = [{_ninfo[e].Child},{_ninfo[e].Sibling}] (_push_enode)");
        }


        private bool _try_find_place(byte* first, byte* last, out int result)
        {
            //Console.WriteLine($"enters [{*first},{*last}] (_find_place)");

            int bi = Header.Ptr->_bheadO;
            if (bi != 0)
            {
                Header.SetWritable();

                BlockMetadataWritePtr _block = Blocks.DirectWrite<BlockMetadataWritePtr>();
                NodesWritePtr _array = Blocks.DirectWrite<NodesWritePtr>();

                int bz = _block.Read(Header.Ptr->_bheadO)->Prev;
                short nc = (short)(last - first + 1);
                while (true)
                {
                    //Console.WriteLine($"try [bi={bi},num={_block[bi].Num},nc={nc}] (_find_place)");
                    BlockMetadata* bbi = _block.Write(bi);

                    if (bbi->Num >= nc && nc < bbi->Reject) // explore configuration
                    {
                        int e = bbi->Ehead;
                        while (true)
                        {
                            int @base = e ^ *first;

                            //Console.WriteLine($"try [e={e},base={@base}] (_find_place)");
                            for (byte* p = first; _array.Read(@base ^ *(++p))->Check < 0;)
                            {
                                if (p == last)
                                {
                                    bbi->Ehead = e;

                                    //Console.WriteLine($"returns: {e} (_find_place)");
                                    result = e;
                                    return true;
                                }
                            }

                            e = -_array.Read(e)->Check;
                            if (e == bbi->Ehead)
                                break;
                        }
                    }

                    bbi->Reject = nc;
                    if (bbi->Reject < Header.Ptr->Reject[bbi->Num])
                        Header.Ptr->Reject[bbi->Num] = bbi->Reject;

                    int bi_ = bbi->Next;
                    bbi->Trial++;
                    if (bbi->Trial == _maxTrial)
                        _transfer_block(bi, ref Header.Ptr->_bheadO, ref Header.Ptr->_bheadC);

                    Debug.Assert(bbi->Trial <= _maxTrial);

                    if (bi == bz)
                        break;

                    bi = bi_;
                }
            }

            if (Header.Ptr->Size != Header.Ptr->Capacity)
            {
                result = _add_block() << 8;
                //Console.WriteLine($"returns: {result} (_find_place)");

                return true;
            }

            result = -1;
            return false;
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
                c = Blocks.NodesInfo[@base ^ c]->Sibling;
            }


            while (c != 0 && c < label)
            {
                p++;
                *p = c;
                c = Blocks.NodesInfo[@base ^ c]->Sibling;
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
                c = Blocks.NodesInfo[@base ^ c]->Sibling;
            }

            return p;
        }

        private bool _consult(int base_n, int base_p, byte c_n, byte c_p)
        {
            //Console.WriteLine($"enters [{base_n},{base_p},{c_n},{c_p}] (_consult)");

            do
            {
                //Console.WriteLine($"\tc_n: _ninfo[{base_n ^ c_n}].sibling = {Blocks.NodesInfo[base_n ^ c_n].Sibling} (_consult)");
                c_n = Blocks.NodesInfo[base_n ^ c_n]->Sibling;
                //Console.WriteLine($"\tc_p: _ninfo[{base_p ^ c_p}].sibling = {Blocks.NodesInfo[base_p ^ c_p].Sibling} (_consult)");
                c_p = Blocks.NodesInfo[base_p ^ c_p]->Sibling;
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

                NodesReadPtr _array = Blocks.Nodes;

                // node on trie
                for (byte* key_ = key; _array[from]->Base >= 0;)
                {
                    if (pos == len)
                    {
                        int offset_ = _array[from]->Base ^ 0;
                        var n = _array[offset_];
                        if (n->Check != from)
                            return CedarResultCode.NoValue;

                        result = n->Value;
                        return CedarResultCode.Success;
                    }

                    int to = _array[from]->Base ^ key_[pos];
                    if (_array[to]->Check != from)
                        return CedarResultCode.NoPath;

                    pos++;
                    from = to;
                }

                offset = -_array[from]->Base;
            }

            // switch to _tail to match suffix
            long pos_orig = pos; // start position in reading _tail

            TailAccessor tail = Tail + (offset - pos);

            if (pos < len)
            {

                int consumed;
                (tail + pos).Compare(key + pos, len - (int)pos, out consumed);
                pos += consumed;

                long moved = pos - pos_orig;
                if (moved != 0)
                {
                    from &= TAIL_OFFSET_MASK;
                    from |= (offset + moved) << 32;
                }

                if (pos < len)
                    return CedarResultCode.NoPath; // input > tail, input != tail
            }

            if (tail[(int)pos] != 0)
                return CedarResultCode.NoValue; // input < tail

            result = tail.Read<short>(len + 1);
            return CedarResultCode.Success;
        }


        public CedarResultCode ExactMatchSearch<TResult>(Slice key, out TResult value, out CedarDataNode* ptr, long from = 0)
            where TResult : struct, ICedarResult
        {
            return ExactMatchSearch(key.Content.Ptr, key.Content.Length, out value, out ptr, from);
        }

        public CedarResultCode ExactMatchSearch<TResult>(byte* key, int size, out TResult value, out CedarDataNode* ptr, long from = 0)
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

        public CedarResultCode GetFirst<TResult>(out TResult value, out CedarDataNode* ptr, long from = 0)
            where TResult : struct, ICedarResultKey
        {
            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            // TODO: Check from where can I get the maximum key size. 
            // TODO: Have a single of these ones per tree.
            Slice key;
            Slice.Create(_llt.Allocator, 4096, out key);

            byte* slicePtr = key.Content.Ptr;

            int keyLength = 0;
            short dataIndex;

            int @base = from >> 32 != 0 ? -(int)(from >> 32) : _array[from]->Base;
            if (@base >= 0)
            {
                // on trie
                byte c = _ninfo[from]->Child;
                if (from == 0)
                {
                    c = _ninfo[@base ^ c]->Sibling;

                    if (c == 0) // no entry to look for
                    {
                        value = default(TResult);
                        ptr = null;
                        return CedarResultCode.NoValue;
                    }
                }

                for (; c != 0 && @base >= 0; keyLength++)
                {
                    // Start to construct the key.
                    slicePtr[keyLength] = c;

                    from = @base ^ c;
                    @base = _array[from]->Base;
                    c = _ninfo[from]->Child;
                }

                if (@base >= 0) // it finishes in the trie
                {
                    key.Content.Ptr[keyLength] = c;
                    dataIndex = _array[@base ^ c]->Value;

                    goto PrepareResult;
                }
            }

            // we have a suffix to look for
            TailAccessor tail = Tail - @base;

            int len_ = tail.CopyKeyTo(slicePtr + keyLength); // Also returns the length that has been copied.
            dataIndex = tail.Read<short>(len_ + 1);
            keyLength += len_;

            PrepareResult:

            key.Shrink(keyLength);

            value = default(TResult);
            value.SetResult(dataIndex, keyLength, 0);
            value.SetKey(key);

            ptr = Data.DirectRead(dataIndex);
            Debug.Assert(ptr->Flags == CedarNodeFlags.Data);

            return CedarResultCode.Success;
        }

        public CedarResultCode GetLast<TResult>(out TResult value, out CedarDataNode* ptr, long from = 0)
            where TResult : struct, ICedarResultKey
        {
            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            // TODO: Check from where can I get the maximum key size. 
            // TODO: Have a single of these ones per tree.
            Slice key;
            Slice.Create(_llt.Allocator, 4096, out key);

            int keyLength = 0;
            short dataIndex;

            int @base = from >> 32 != 0 ? -(int)(from >> 32) : _array[from]->Base;
            if (@base >= 0)
            {
                // On trie         
                byte c = _ninfo[from]->Child;
                if (from == 0)
                {
                    // We are on the root. Find the first node. 
                    c = _ninfo[@base ^ c]->Sibling;

                    if (c == 0) // no entry to look for
                    {
                        value = default(TResult);
                        ptr = null;
                        return CedarResultCode.NoValue;
                    }
                }

                // In here we know we have the location for the first labeled node.
                // from: root node
                // @base: The pool location of root node (base[from])
                // c: the first node label on the trie.

                for (; @base >= 0; keyLength++)
                {
                    long currentFrom = @base ^ c;
                    while (_ninfo[currentFrom]->Sibling != 0)
                    {
                        c = _ninfo[currentFrom]->Sibling;
                        currentFrom = @base ^ c;
                    }

                    // Start to construct the key.
                    key[keyLength] = c;

                    from = currentFrom;
                    @base = _array[from]->Base;
                    c = _ninfo[from]->Child;
                }

                if (c != 0 && @base >= 0) // it finishes in the trie
                {
                    key[keyLength] = c;
                    dataIndex = _array[@base ^ c]->Value;

                    goto PrepareResult;
                }
            }

            // we have a suffix to look for
            TailAccessor tail = Tail - @base;

            int len_ = tail.CopyKeyTo(key.Content.Ptr + keyLength); // Also returns the length that has been copied.
            dataIndex = tail.Read<short>(len_ + 1);
            keyLength += len_;

            PrepareResult:

            key.SetSize(keyLength);

            value = default(TResult);
            value.SetResult(dataIndex, keyLength, 0);
            value.SetKey(key);

            ptr = Data.DirectRead(dataIndex);
            Debug.Assert(ptr->Flags == CedarNodeFlags.Data);

            return CedarResultCode.Success;
        }


        internal struct IteratorValue
        {
            public int Length;
            public short Value;
            public CedarResultCode Error;                                    
        }

        private interface IComparerDirective { }
        private struct StrictComparer : IComparerDirective { }
        private struct IncludeEqualsComparer : IComparerDirective { }

        internal IteratorValue Predecessor(Slice lookupKey, ref Slice outputKey, ref long from, ref long len)
        {
            return Predecessor<StrictComparer>(lookupKey, ref outputKey, ref from, ref len);
        }

        internal IteratorValue PredecessorOrEqual(Slice lookupKey, ref Slice outputKey, ref long from, ref long len)
        {
            return Predecessor<IncludeEqualsComparer>(lookupKey, ref outputKey, ref from, ref len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IteratorValue Predecessor<T>(Slice lookupKey, ref Slice outputKey, ref long from, ref long len)
            where T : struct, IComparerDirective
        {
            outputKey.Reset();

            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            int len_;
            TailAccessor tail = Tail;

            int @base = from >> 32 != 0 ? -(int)(from >> 32) : _array[from]->Base;
            if (@base < 0)
                throw new InvalidOperationException("This shouldnt happen.");

            // on trie
            byte c = _ninfo[from]->Child;
            if (from == 0)
            {
                c = _ninfo[@base ^ c]->Sibling;
                if (c > lookupKey[0]) // no entry strictly smaller than the key. 
                    return new IteratorValue {Error = CedarResultCode.NoPath};
            }

            // Until we consume the entire key we can have a few possible outcomes
            // 1st: The char exists to we go down that direction.
            // 2nd: The char doesnt exists so we either get the first smaller than the key OR
            // 3rd: We need to back-off because there is no such first smaller.
            // 4th: We are at the tail start. If they value is smaller than the key, we have our predecessor. If not we need to back-off. 
            // 5th: We consumed the whole lookup key and therefore we need to back-off. 
            byte currentC;
            for (; @base >= 0; len++)
            {
                if (len >= lookupKey.Size)
                    return BackoffAndGetLast(lookupKey, ref outputKey, ref from, ref len);

                // We will try to go down as much as possible. [1]
                currentC = lookupKey[(int) len];

                int to = @base ^ currentC;
                if (_array[to]->Check != from)
                    break;

                outputKey[(int) len] = currentC;

                from = @base ^ currentC;
                @base = _array[from]->Base;
            }

            // We need to know if we are still on the trie nodes section.
            if (@base < 0)
            {
                // As we are at the edge, we need to check if the tail is the predecessor.
                // len_ = _strlen(tail - @base);
                len_ = (tail - @base).StrLength();

                int comparableKeyLen = Math.Min(len_, lookupKey.Content.Length - (int)len);

                int r = (tail - @base).Compare(lookupKey.Content.Ptr + len, comparableKeyLen);
                if (r < 0)
                    goto PrepareOutputKey;
                
                // We are looking for equals, the length of both must be equal. 
                if (typeof(T) == typeof(IncludeEqualsComparer) && r == 0 && len_ == (lookupKey.Content.Length - (int)len))
                    goto PrepareOutputKey;

                // The current tail value is bigger.
                // We cannot do anything here, we must move upwards until we can find an smaller than the key position.
                return BackoffAndGetLast(lookupKey, ref outputKey, ref from, ref len);
            }

            // Here len is either the same or smaller because we failed at a tail.  
            currentC = lookupKey[(int) len];                        
            c = _ninfo[from]->Child;

            // At position len the current character doesnt exist in the siblings list. 
            // Now we need to try for [2] and if there is no options go for [3]      
            if (c > currentC)
            {
                // The current node is bigger.
                // We cannot do anything here, we must move upwards until we can find an smaller than the key position.
                return BackoffAndGetLast(lookupKey, ref outputKey, ref from, ref len);
            }

            // We consume the character
            from = @base ^ c;
            //@base = _array[from].Base;
            long currentFrom = from;
            while ( _ninfo[currentFrom]->Sibling != 0 && 
                (_ninfo[currentFrom]->Sibling < currentC || 
                (typeof(T) == typeof(IncludeEqualsComparer) && _ninfo[currentFrom]->Sibling == currentC))) 
            {
                c = _ninfo[currentFrom]->Sibling;
                currentFrom = @base ^ c;
            }

            // We consume the character we selected.
            outputKey[(int)len] = c;
            len++;

            from = currentFrom;
            @base = _array[from]->Base;
            c = _ninfo[from]->Child;     
            
            // Now we will just get the right-most 

            for (; @base >= 0; len++)
            {
                currentFrom = @base ^ c;
                while (_ninfo[currentFrom]->Sibling != 0)
                {
                    c = _ninfo[currentFrom]->Sibling;
                    currentFrom = @base ^ c;
                }

                // Start to construct the key.
                outputKey[(int) len] = c;

                from = currentFrom;
                @base = _array[from]->Base;
                c = _ninfo[from]->Child;
            }

            // we have a suffix to look for
            
            len_ = (tail - @base).StrLength();

            PrepareOutputKey:

            // Copy tail to key
            len_ = (tail - @base).CopyTo(outputKey.Content.Ptr + len, len_);            

            from &= TAIL_OFFSET_MASK;
            from |= ((long)(-@base + len_)) << 32; // this must be long
            len += len_;

            outputKey.SetSize((int)len);

            return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = tail.Read<short>(-@base + len_ + 1) };
        }

        private IteratorValue BackoffAndGetLast(Slice lookupKey, ref Slice outputKey, ref long to, ref long len)
        {
            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            to &= TAIL_OFFSET_MASK; // We dont care about the tail offset (caller already did).

            // We are moving up the tree looking for the first parent node whose children have 
            // the first smaller child node.

            short @base;
            while (len != 0)
            {
                // We get the parent 
                long from = _array[to]->Check;
                @base = _array[from]->Base;

                len--;
                byte c = lookupKey[(int)len];

                Debug.Assert((byte)(_array[from]->Base ^ (int)to) == c); // Is this the value we are looking for?

                byte child = _ninfo[from]->Child;
                if (from == 0)
                    child = _ninfo[_array[from]->Base ^ child]->Sibling;

                if (child < c && child != 0)
                {
                    // This node has at least 1 child node whose value is smaller than the key.
                    // Having at least 1 child implies that there is at least 1 predecessor in this subtree.                     

                    // First iteration!!! We need to find the first predecessor to 'c' 
                    long currentFrom = @base ^ child;
                    while (_ninfo[currentFrom]->Sibling != 0 && _ninfo[currentFrom]->Sibling < c)
                    {
                        child = _ninfo[currentFrom]->Sibling;
                        currentFrom = @base ^ child;
                    }

                    if (child == 0)
                    {
                        // We just found the value in the prefix itself.
                        goto PrepareResult;
                    }

                    outputKey[(int) len] = child;
                    len++; // Prepare for the coming node.

                    // We move down one node
                    from = currentFrom;
                    @base = _array[from]->Base;
                    child = _ninfo[from]->Child;

                    // Second iteration!!! We need to always find the last. 
                    for (; child != 0 && @base >= 0; len++)
                    {
                        currentFrom = @base ^ child;
                        while (_ninfo[currentFrom]->Sibling != 0)
                        {
                            child = _ninfo[currentFrom]->Sibling;
                            currentFrom = @base ^ child;
                        }

                        // Start to construct the key.
                        outputKey[(int) len] = child;

                        from = currentFrom;
                        @base = _array[from]->Base;
                        child = _ninfo[from]->Child;
                    }

                    if (child != 0 && @base >= 0) // it finishes in the trie
                    {
                        outputKey[(int) len] = child;
                        len++;

                        outputKey.SetSize((int)len);

                        return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = _array[from]->Value };
                    }

                    goto PrepareResult;
                }
                else
                {
                    Node* n = _array[@base ^ 0];
                    if (n->Check >= 0 && (_array[n->Check]->Base == @base))
                    {
                        // We found a prefix node finishing inside the trie nodes while backtracking
                        outputKey.SetSize((int)len);

                        return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = _array[@base ^ 0]->Value };
                    }
                }

                // If we didnt find anything, we start again with the new parent.
                to = from;
            }

            return new IteratorValue { Error = CedarResultCode.NoPath };

            PrepareResult:

            // we have a suffix to look for
            TailAccessor tail = Tail - @base;
            int len_ = tail.CopyKeyTo(outputKey.Content.Ptr + len);
            len += len_;

            outputKey.SetSize((int)len);

            return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = tail.Read<short>(len_ + 1) };
        }

        internal IteratorValue Successor(Slice lookupKey, ref Slice outputKey, ref long from, ref long len)
        {
            return Successor<StrictComparer>(lookupKey, ref outputKey, ref from, ref len);
        }

        internal IteratorValue SuccessorOrEqual(Slice lookupKey, ref Slice outputKey, ref long from, ref long len)
        {
            return Successor<IncludeEqualsComparer>(lookupKey, ref outputKey, ref from, ref len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IteratorValue Successor<T>(Slice lookupKey, ref Slice outputKey, ref long from, ref long len)
            where T : struct, IComparerDirective
        {
            outputKey.Reset();

            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            int len_;
            TailAccessor tail = Tail;

            int @base = from >> 32 != 0 ? -(int)(from >> 32) : _array[from]->Base;
            if (@base < 0)
                throw new InvalidOperationException("This shouldnt happen.");

            // on trie
            byte c = _ninfo[from]->Child;
            if (from == 0)
            {
                c = _ninfo[@base ^ c]->Sibling;
                if (c > lookupKey[0]) // no entry strictly smaller than the key. 
                    return new IteratorValue { Error = CedarResultCode.NoPath };
            }

            // Until we consume the entire key we can have a few possible outcomes
            // 1st: The char exists to we go down that direction.
            // 2nd: The char doesnt exists so we either get the first greater than the key OR
            // 3rd: We need to back-off because there is no such first greater.
            // 4th: We are at the tail start. If the value is greater than the key, we have our successor. If not we need to back-off. 
            // 5th: We consumed the whole lookup key and therefore we need to either get the first or fail and back-off. 

            byte currentC;
            for (; @base >= 0; len++)
            {
                if (len >= lookupKey.Size)
                    break; // We will continue if the is somewhere to go down. 

                // We will try to go down as much as possible. [1]
                currentC = lookupKey[(int)len];

                int to = @base ^ currentC;
                if (_array[to]->Check != from)
                    break;

                outputKey[(int)len] = currentC;

                from = @base ^ currentC;
                @base = _array[from]->Base;
            }

            IteratorValue result;

            // We need to know if we are still on the trie nodes section.
            if (@base < 0)
            {
                // As we are at the edge, we need to check if the tail is the predecessor.
                len_ = (tail - @base).StrLength();

                int comparableKeyLen = Math.Min(len_, lookupKey.Size - (int)len);
                int r = (tail - @base).Compare(lookupKey.Content.Ptr + len, comparableKeyLen);
                if (r > 0)
                    goto PrepareOutputKey;

                if (r == 0 && (lookupKey.Size - (int)len) < len_)
                    goto PrepareOutputKey;                

                // We are looking for equals, the length of both must be equal. 
                if (typeof(T) == typeof(IncludeEqualsComparer) && r == 0 && len_ == (lookupKey.Size - (int)len))
                    goto PrepareOutputKey;

                // The current tail value is smaller.
                // We need to backoff and get the next.
                result = Next(outputKey, ref from, ref len);
                goto PrepareBeginNextResult;
            }

            // Here len is either the same or smaller because we failed at a tail. 

            c = _ninfo[from]->Child;
            if (len < lookupKey.Size)
            {
                // We need to retry the lookup key.
                currentC = lookupKey[(int) len];
            }
            else
            {
                // We only care about the Begin or the Next. 
                if (c == 0 && typeof(T) == typeof(StrictComparer))
                {
                    // We dont care about the potential hit here if we are using the strict comparizon.
                    result = Next(outputKey, ref from, ref len);
                    goto PrepareBeginNextResult;
                }

                if (c == 0 && typeof(T) == typeof(IncludeEqualsComparer))
                {
                    // We dont care about the potential hit here if we are using the strict comparizon.
                    result = Begin(outputKey, ref from, ref len);
                    goto PrepareBeginNextResult;
                }

                currentC = 0;
            }

            // At position len the current character doesnt exist in the siblings list. 
            // Now we need to try for [2] and if there is no options go for [3]      
            if (c > currentC)
            {
                // The current child node holds the successor.
                result = Begin(outputKey, ref from, ref len);
                goto PrepareBeginNextResult;
            }

            // We will now try to get a sibling that is greater than the current, if not we need to backoff            

            long currentFrom = @base ^ c;
            while (_ninfo[currentFrom]->Sibling != 0 && c < currentC)
            {
                c = _ninfo[currentFrom]->Sibling;
                currentFrom = @base ^ c;
            }

            // If we couldnt find any greater value in this node, we need to backoff. 
            if (c < currentC)
            {
                result = Next(outputKey, ref from, ref len);
                goto PrepareBeginNextResult;
            }

            // We have a greater inside here. 
            // We consume the character we selected.
            outputKey[(int)len] = c;
            len++;
            from = currentFrom;

            // We have found what we need with the new from. 
            result = Begin(outputKey, ref from, ref len);
            goto PrepareBeginNextResult;

            PrepareOutputKey:

            // Copy tail to key
            (tail - @base).CopyTo(outputKey.Content.Ptr + len, len_);

            from &= TAIL_OFFSET_MASK;
            from |= ((long)(-@base + len_)) << 32; // this must be long
            len += len_;

            outputKey.SetSize((int)len);

            return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = tail.Read<short>(-@base + len_ + 1)};

            PrepareBeginNextResult:

            if ( result.Error == CedarResultCode.Success)
                outputKey.SetSize((int)len);

            return result;
        }

        internal IteratorValue Begin(Slice outputKey, ref long from, ref long len)
        {
            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            int @base = from >> 32 != 0 ? -(int)(from >> 32) : _array[from]->Base;
            if (@base >= 0)
            {
                // on trie
                byte c = _ninfo[from]->Child;
                if (from == 0)
                {
                    c = _ninfo[@base ^ c]->Sibling;
                    if (c == 0) // no entry
                        return new IteratorValue { Error = CedarResultCode.NoPath };
                }

                for (; c != 0 && @base >= 0; len++)
                {
                    outputKey[(int)len] = c;

                    from = @base ^ c;
                    @base = _array[from]->Base;
                    c = _ninfo[from]->Child;
                }

                if (@base >= 0) // it finishes in the trie
                {
                    outputKey[(int)len] = c;
                    return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = _array[@base ^ c]->Value };
                }                    
            }

            // we have a suffix to look for
            TailAccessor tail = Tail - @base;

            // Copy tail to key
            int len_ = tail.CopyKeyTo(outputKey.Content.Ptr + len);

            from &= TAIL_OFFSET_MASK;
            from |= ((long)(-@base + len_)) << 32; // this must be long
            len += len_;

            return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = tail.Read<short>(len_ + 1) };
        }

        internal IteratorValue Next(Slice outputKey, ref long from, ref long len, long root = 0)
        {
            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            // return the next child if any
            byte c = 0;

            int offset = (int)(from >> 32);
            if (offset != 0)
            {
                // on tail 
                if (root >> 32 != 0)
                    return new IteratorValue { Error = CedarResultCode.NoPath };

                from &= TAIL_OFFSET_MASK;
                len -= offset - (-_array[from]->Base);
            }
            else
            {
                c = _ninfo[_array[from]->Base ^ 0]->Sibling;
            }

            for (; c == 0 && from != root; len--)
            {
                c = _ninfo[from]->Sibling;
                from = _array[from]->Check;
            }

            if (c == 0)
                return new IteratorValue { Error = CedarResultCode.NoPath };

            outputKey[(int)len] = c;
            from = _array[from]->Base ^ c;
            len++;            

            return Begin(outputKey, ref from, ref len);
        }

        internal IteratorValue Previous(Slice outputKey, ref long from, ref long len, long root = 0)
        {
            throw new NotImplementedException("Not implemented yet. Optimized version of backwards iteration. ");
        }

        internal IteratorValue End(Slice outputKey, ref long from, ref long len, long root = 0)
        {
            NodesReadPtr _array = Blocks.Nodes;
            NodesInfoReadPtr _ninfo = Blocks.NodesInfo;

            int @base = from >> 32 != 0 ? -(int)(from >> 32) : _array[from]->Base;
            if (@base >= 0)
            {
                // On trie         
                byte c = _ninfo[from]->Child;
                if (from == 0)
                {
                    // We are on the root. Find the first node. 
                    c = _ninfo[@base ^ c]->Sibling;

                    if (c == 0) // no entry to look for
                    {
                        return new IteratorValue { Error = CedarResultCode.NoPath };
                    }
                }

                // In here we know we have the location for the first labeled node.
                // from: root node
                // @base: The pool location of root node (base[from])
                // c: the first node label on the trie.

                for (; @base >= 0; len++)
                {
                    long currentFrom = @base ^ c;
                    while (_ninfo[currentFrom]->Sibling != 0)
                    {
                        c = _ninfo[currentFrom]->Sibling;
                        currentFrom = @base ^ c;
                    }

                    if (c == 0)
                        break;

                    // Start to construct the key.
                    outputKey[(int)len] = c;

                    from = currentFrom;
                    @base = _array[from]->Base;
                    c = _ninfo[from]->Child;
                }

                if (c != 0 && @base >= 0) // it finishes in the trie
                {
                    outputKey[(int)len] = c;
                    return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = _array[@base ^ c]->Value };
                }
            }

            // we have a suffix to look for
            TailAccessor tail = Tail - @base;

            // Copy tail to key
            int len_ = tail.CopyKeyTo(outputKey.Content.Ptr + len);

            from &= TAIL_OFFSET_MASK;
            from |= ((long)(-@base + len_)) << 32; // this must be long
            len += len_;

            return new IteratorValue { Error = CedarResultCode.Success, Length = (int)len, Value = tail.Read<short>(len_ + 1)  };
        }
    }
}
