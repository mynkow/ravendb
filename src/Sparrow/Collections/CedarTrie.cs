using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Sparrow.Collections
{
    public class CedarTrie<T> : CedarTrie<T, long, Ordered> where T : struct
    {

    }

    public interface IOrderableDirective { }
    public struct Ordered : IOrderableDirective { };
    public struct Unordered : IOrderableDirective { };

    public unsafe class CedarTrie<T, SizeType, TOrdered> : IDisposable
        where T : struct
        where SizeType : struct
        where TOrdered : IOrderableDirective
    {
        protected readonly int _nodeSize;
        protected readonly int _tSize;
        protected readonly int _numTrackingNodes = 0;
        protected readonly int _maxTrial = 1;

        private const long TAIL_OFFSET_MASK = 0xffffffff;
        private const long NODE_INDEX_MASK = 0xffffffff << 32;

        public enum ErrorCode {  NoValue, NoPath }

        public struct result_pair_type
        {
            public T Value;
            public SizeType Length; // prefix length
        }

        public struct result_triple_type // for predict ()
        {
            public T Value;
            public SizeType Length; // suffix length
            public long Id; // node id of value
        }

        public CedarTrie()
        {
            _tSize = Unsafe.SizeOf<T>();
            Debug.Assert(_tSize <= sizeof(int), "Value types bigger than 4 bytes are not supported yet");

            // This is the internal size of the CedarNode<T>
            // The SizeOf<T> function wont work if we use a generic struct (even if the type size is not dependent on T). 
            _nodeSize = Unsafe.SizeOf<node>();             

            Initialize();
        }

        private node* _array;
        private ninfo* _ninfo;
        private block* _block;
        private long* _trackingNode;

        private int _bheadF;  // first block of Full;   0
        private int _bheadC;  // first block of Closed; 0 if no Closed
        private int _bheadO;  // first block of Open;   0 if no Open

        private int _quota;
        private int _quota0;

        private int _no_delete;

        private short[] _reject = new short[257];

        private ElementUnion u1;
        private SizeUnion u2;

        public int Capacity { get; private set; }
        public int Size { get; private set; }
        public int Length { get; private set; }

        public int TotalSize
        {
            get { return _nodeSize * Size + Size * Unsafe.SizeOf<ninfo>() + (Size >> 8) * Unsafe.SizeOf<block>(); }
        }

        public int UnitSize
        {
            get { return _nodeSize; }
        }

        public int NonZeroSize
        {
            get
            {
                int i = 0;
                for ( int to = 0; to < Size; to++)
                {
                    if (_array[to].Check >= 0)
                        i++;
                }
                return i;
            }
        }

        public int NonZeroLength
        {
            get
            {
                int i = 0;
                int j = 0;

                for (int to = 0; to < Size; to++)
                {
                    node n = _array[to];
                    if (n.Check >= 0 && _array[n.Check].Base != to && n.Base < 0)
                    {
                        j++;
                        for (byte* p = &u1.Tail[-n.Base]; *p != 0; p++)
                            i++;
                    }
                }

                return i + j * (1 + _tSize);
            }
        }

        public int NumberOfKeys
        {
            get
            {
                int i = 0;
                for (int to = 0; to < Size; to++)
                {
                    node n = _array[to];
                    if (n.Check >= 0 && (_array[n.Check].Base == to || n.Base < 0))
                        i++;
                }

                return i;
            }
        }

        public T ExactMatchSearch(byte[] key, int from = 0)
        {
            fixed (byte* keyPtr = key)
                return ExactMatchSearch(keyPtr, key.Length, from);
        }

        public int CommonPrefixSearch(byte[] key, out T result, int result_len, int from = 0)
        {
            fixed (byte* keyPtr = key)
                return CommonPrefixSearch(keyPtr, key.Length, out result, result_len, from);
        }

        public int CommonPrefixPredict( byte[] key, out T result, int result_len)
        {
            fixed (byte* keyPtr = key)
                return CommonPrefixPredict(keyPtr, key.Length, out result, result_len);
        }

        public T ExactMatchSearch(byte* key, int size, long from = 0)
        {
            throw new NotImplementedException();
        }

        public int CommonPrefixSearch(byte* key, int size, out T result, int result_len, long from = 0)
        {
            throw new NotImplementedException();
        }

        public int CommonPrefixPredict( byte* key, int size, out T result, int result_len)
        {
            throw new NotImplementedException();
        }

        public void Suffix( byte* key, int size, long to )
        {
            throw new NotImplementedException();
        }

        // test the validity of double array for debug
        internal void Test(long from = 0)
        {
            int @base = _array[from].Base;
            if ( @base < 0 )
            {
                // validate tail offset
                if (*u1.Length < (int)(-@base + 1 + Unsafe.SizeOf<T>()))
                    throw new Exception($"Fail in tail offset {from}");

                return;
            }
            byte c = _ninfo[from].Child;
            do
            {
                if (from != 0)
                {
                    if (_array[@base ^ c].Check != from)
                        throw new Exception($"");
                }
                if (c != 0)
                {
                    Test(@base ^ c);
                }

                c = _ninfo[@base ^ c].Sibling;
            }
            while (c != 0);
        }

        public void Update(byte[] key, T value, long from = 0, long to = 0)
        {
            fixed ( byte* keyPtr = key)
            {
                Update(keyPtr, key.Length, value, ref from, ref to);
            }            
        }

        public void Update(byte* key, int len, T value, long from = 0, long to = 0)
        {
            Update(key, len, value, ref from, ref to);
        }

        private void Update(byte* key, int len, T value, ref long from, ref long pos)
        {
            if (len == 0 && from == 0)
                throw new ArgumentException("failed to insert zero-length key");

            if (this._ninfo == null || this._block == null)
                Restore();

            long offset = from >> 32;
            if (offset == 0)
            {
                for (byte* keyPtr = key; _array[from].Base >= 0; pos++ )
                {
                    if ( pos == len )
                    {
                        int current = Follow(from, 0);
                        //_array[current].Value = value; // TODO: Here there can be some issues as the original code doesn't make sense in C#                        
                        throw new NotImplementedException();
                    }

                    from = Follow(from, key[pos]);
                }
                offset = -_array[from].Base;
            }

            long pos_orig;
            void* ptr;
            byte* tail = null;
            if ( offset >= sizeof(int) ) // go to _tail
            {
                long moved;
                pos_orig = pos;                

                tail = (u1.Tail + offset) - pos;
                while (pos < len && key[pos] == tail[pos])
                    pos++;

                if ( pos == len && tail[pos] == '\0')
                {
                    // we found an exact match
                    moved = pos - pos_orig;
                    if ( moved != 0 )
                    {
                        // search end on tail
                        from &= TAIL_OFFSET_MASK;
                        from |= (offset + moved) << 32;
                    }

                    ptr = tail + (len + 1);
                    T v1 = Unsafe.Read<T>(ptr);
                    Unsafe.Write(ptr, IncrementPrimitiveValue(v1, value));

                    return;
                }

                // otherwise, insert the common prefix in tail if any
                if ( from >> 32 != 0 )
                { 
                    from &= TAIL_OFFSET_MASK; // reset to update tail offset
                    for ( int offset_ = -_array[from].Base; offset_ < offset; )
                    {
                        from = Follow(from, u1.Tail[offset_]);
                        offset_++;

                        // this shows intricacy in debugging updatable double array trie
                        if ( _numTrackingNodes > 0) // keep the traversed node (on tail) updated
                        {
                            for ( int j = 0; _trackingNode[j] != 0; j++ )
                            {
                                if (_trackingNode[j] >> 32 == offset_)
                                    _trackingNode[j] = from;
                            }
                        }
                    }
                }

                for ( long pos_ = pos_orig; pos_ < pos; pos_++ )
                    from = Follow(from, key[pos_]);

                moved = pos - pos_orig;
                if ( tail[pos] != 0 )
                {
                    // remember to move offset to existing tail
                    long to_ = Follow(from, tail[pos]);
                    moved++;
                    _array[to_].Base = - (int)(offset + moved);
                    moved -= 1 + _tSize;  // keep record
                }

                moved += offset;
                for (int i = (int)offset; i <= moved; i += 1 + _tSize )
                {
                    *u2.Length0 += 1;
                    if (_quota0 == *u2.Length0)
                    {
                        _quota0 += _quota0;

                        // Reallocating Tail0
                        u2.Ptr = Reallocate<int>( u2.Ptr, _quota0, *u2.Length0);
                    }
                    u2.Tail0[*u2.Length0] = i;
                }
                if (pos == len || tail[pos] == '\0')
                {
                    long to = Follow(from, 0);
                    if ( pos == len )
                    {
                        T _toValue = GetNodeValue(_array + to);
                        SetNodeValue(_array + to, IncrementPrimitiveValue(_toValue, value));
                        return;
                    }
                    else
                    {
                        T _toValue = Unsafe.Read<T>(tail + pos + 1);
                        T _arrayValue = GetNodeValue(_array + to);
                        SetNodeValue(_array + to, IncrementPrimitiveValue(_arrayValue, _toValue));
                    }
                }

                from = Follow(from, key[pos]);
                pos++;
            }

            //
            int needed = (int)(len - pos + 1 + _tSize);
            if ( pos == len && *u2.Length0 != 0)
            {
                int offset0 = *(u2.Tail0 + *u2.Length0);
                tail[offset0] = 0;
                _array[from].Base = -offset0;
                (*u2.Length0)--;

                Unsafe.Write(tail + (offset0 + 1), value);
                return;
            }

            if ( _quota < *u1.Length + needed )
            {
                _quota += _quota >= needed ? _quota : needed;

                // Reallocating Tail.
                u1.Ptr = Reallocate<byte>( u1.Ptr, _quota, *u1.Length);
            }

            _array[from].Base = -*u1.Length;
            pos_orig = pos;

            tail = u1.Tail + *u1.Length - pos;
            if ( pos < len)
            {
                do
                    tail[pos] = key[pos];
                while (++pos < len);

                from |= ((long)(*u1.Length) + (len - pos_orig)) << 32;
            }

            *u1.Length += needed;

            ptr = tail + (len + 1);
            T v = Unsafe.Read<T>(ptr);
            Unsafe.Write(ptr, IncrementPrimitiveValue(v, value));
        }

        // TODO: Move this into a general T to struct converter. 
        static T IncrementPrimitiveValue(T op1, T op2)
        {
            if (typeof(T) == typeof(byte))
            {
                byte v1 = (byte)(object)op1;
                byte v2 = (byte)(object)op2;
                return (T)(object)(v1 + v2);
            }

            if (typeof(T) == typeof(short))
            {
                short v1 = (short)(object)op1;
                short v2 = (short)(object)op2;
                return (T)(object)(v1 + v2);
            }

            if (typeof(T) == typeof(int))
            {
                int v1 = (int)(object)op1;
                int v2 = (int)(object)op2;
                return (T)(object)(v1 + v2);
            }

            throw new NotImplementedException();
        }

        // TODO: Move this into a general T to struct converter. 
        static T CastToPrimitiveValue(T op)
        {
            if (typeof(T) == typeof(byte))
            {
                byte v = (byte)(object)op;
                return (T)(object)(v);
            }

            if (typeof(T) == typeof(short))
            {
                short v = (short)(object)op;
                return (T)(object)(v);
            }

            if (typeof(T) == typeof(int))
            {
                int v = (int)(object)op;
                return (T)(object)(v);
            }

            throw new NotImplementedException();
        }


        private int Follow(long from, byte label)
        {
            int to = 0;
            int @base = _array[from].Base;
            if (@base < 0 || _array[to = @base ^ label].Check < 0) // TODO: Check if the rules are the same here as in C++
            {
                to = _pop_enode(@base, label, (int)from);
                _push_sibling(from, to ^ label, label, @base >= 0);
            }
            else if (_array[to].Check != (int)from)
            {
                to = _resolve(from, @base, label);
            }                
            return to;
        }
        
        /// <summary>
        /// Resolve conflict on base_n ^ label_n = base_p ^ label_p
        /// </summary>
        private int _resolve(long from_n, int base_n, byte label_n)
        {
            // examine siblings of conflicted nodes

            int to_pn = base_n ^ label_n;
            int from_p = _array[to_pn].Check;
            int base_p = _array[from_p].Base;

            // whether to replace siblings of newly added
            bool flag = _consult(base_n, base_p, _ninfo[from_n].Child, _ninfo[from_p].Child);

            byte* child = stackalloc byte[256];
            byte* first = child;
            byte* last = flag ? _set_child(first, base_n, _ninfo[from_n].Child, label_n) : _set_child(first, base_p, _ninfo[from_p].Child );

            int @base = (first == last ? _find_place() : _find_place(first, last)) ^ *first;

            // replace & modify empty list
            int from = flag ? (int)from_n : from_p;
            int base_ = flag ? base_n : base_p;

            if (flag && *first == label_n)
                _ninfo[from].Child = label_n; // new child

            _array[from].Base = @base; // new base

            for ( byte* p = first; p <= last; p++)
            {
                // to_ => to
                int to = _pop_enode(@base, *p, from);
                int to_ = base_ ^ *p;

                _ninfo[to].Sibling = (byte) (p == last ? 0 : *(p + 1));

                if (flag && to_ == to_pn) // skip newcomer (no child)
                    continue;

                // TODO: moved node. 

                node* n = _array + to;
                node* n_ = _array + to_;

                n->Base = n_->Base;
                if (n_->Base > 0 && *p != 0 )
                {
                    // copy base; bug fix
                    byte c = _ninfo[to].Child = _ninfo[to_].Child;
                    do
                    {
                        _array[n->Base ^ c].Check = to;
                        c = _ninfo[n->Base ^ c].Sibling;
                    }
                    while (c != 0);
                }

                if ( !flag && to_ == (int) from_n) // parent node moved
                    from_n = (long)to;

                if ( !flag && to_ == to_pn)
                {
                    // the address is immediately used
                    _push_sibling(from_n, to_pn ^ label_n, label_n);
                    _ninfo[to_].Child = 0; // remember to reset child
                    if (label_n != 0)
                    {
                        n_->Base = -1;
                    }                        
                    else
                    {
                        SetNodeValue(n_, default(T)); 
                    }
                    n_->Check = (int)from_n;
                }
                else
                {
                    _push_enode(to_);
                }

                if ( _numTrackingNodes != 0 )
                {
                    for (int j = 0; j < _numTrackingNodes; j++)
                    {
                        if ( (int)(_trackingNode[j] & TAIL_OFFSET_MASK) == to_ )
                        {
                            _trackingNode[j] &= NODE_INDEX_MASK;
                            _trackingNode[j] |= (long)to;
                        }
                    }
                }
            }

            return flag ? @base ^ label_n : to_pn;
        }

        private void _push_enode(int e)
        {
            int bi = e >> 8;
            block* b = _block + bi;

            b->Num++;
            if ( b->Num == 1 )
            {
                // Full to Closed
                b->Ehead = e;
                _array[e] = new node(-e, -e);
                if (bi != 0)
                    _transfer_block(bi, ref _bheadF, ref _bheadC); // Full to Closed
            }
            else
            {
                int prev = b->Ehead;
                int next = -_array[prev].Check;
                _array[e] = new node(-prev, -next);
                _array[prev].Check = _array[next].Base = -e;
                if ( b->Num == 2 || b->Trial == _maxTrial)
                {
                    // Closed to Open
                    if (bi != 0)
                        _transfer_block(bi, ref _bheadC, ref _bheadO);
                }
                b->Trial = 0;
            }

            if (b->Reject < _reject[b->Num])
                b->Reject = _reject[b->Num];

            _ninfo[e] = new ninfo();
        }

        private int _find_place(byte* first, byte* last)
        {
            int bi = _bheadO;
            if ( bi != 0 )
            {
                int bz = _block[_bheadO].Prev;
                short nc = (short)(last - first + 1);
                while ( true )
                {
                    block* b = _block + bi;
                    if ( b->Num >= nc && nc < b->Reject) // explore configuration
                    {
                        int e = b->Ehead;
                        while ( true )
                        {
                            int @base = e ^ *first;
                            for ( byte* p = first; _array[ @base ^ *(++p)].Check < 0; )
                            {
                                if (p == last)
                                {
                                    b->Ehead = e;
                                    return e;
                                }       
                            }

                            e = -_array[e].Check;
                            if (e == b->Ehead)
                                break;
                        }
                    }

                    b->Reject = nc;
                    if (b->Reject < _reject[b->Num])
                        _reject[b->Num] = b->Reject;

                    int bi_ = b->Next;
                    b->Trial++;
                    if (b->Trial == _maxTrial)
                        _transfer_block(bi, ref _bheadO, ref _bheadC);

                    //Debug.Assert(b->Trial <= _maxTrial);
                    if (b->Trial > _maxTrial)
                        throw new Exception();

                    if (bi == bz)
                        break;

                    bi = bi_;
                }
            }

            return _add_block() << 8;
        }

        private readonly static byte MinusOne = unchecked( (byte) -1 );

        private byte* _set_child(byte* p, int @base, byte c)
        {
            unchecked
            {
                return _set_child(p, @base, c, MinusOne);
            };            
        }

        private byte* _set_child(byte* p, int @base, byte c, byte label)
        {
            p--;
            if ( c == 0 )
            {
                // 0 is a terminal character.
                p++;
                *p = c;
                c = _ninfo[@base ^ c].Sibling;
            }

            if ( typeof(TOrdered) == typeof(Ordered))
            {
                while ( c != 0 && c < label )
                {
                    p++;
                    *p = c;
                    c = _ninfo[@base ^ c].Sibling;
                }
            }

            if ( label != MinusOne)
            {
                p++;
                *p = label;
            }

            while ( c != 0 )
            {
                p++;
                *p = c;
                c = _ninfo[@base ^ c].Sibling;
            }

            return p;
        }

        private bool _consult(int base_n, int base_p, byte c_n, byte c_p)
        {
            do
            {
                c_n = _ninfo[base_n ^ c_n].Sibling;
                c_p = _ninfo[base_p ^ c_p].Sibling;
            }
            while (c_n != 0 && c_p != 0);

            return c_p != 0;
        }

        private void _push_sibling(long from, int @base, byte label, bool flag = true)
        {
            byte* c = &(_ninfo[from].Child);
            if ( flag && ((typeof(TOrdered) == typeof(Ordered)) ? label > *c : *c == 0))
            {
                do
                {
                    c = &_ninfo[@base ^ *c].Sibling;
                }
                while ((typeof(TOrdered) == typeof(Ordered)) && *c != 0 && *c < label);
            }

            _ninfo[@base ^ label].Sibling = *c;
            *c = label;
        }

        private int _pop_enode(int @base, byte label, int from)
        {
            int e = @base < 0 ? _find_place() : @base ^ label;
            int bi = e >> 8;

            node* n = _array + e;
            block* b = _block + bi;

            b->Num--;
            if (b->Num == 0)
            {
                if (bi != 0)
                    _transfer_block(bi, ref _bheadC, ref _bheadF); // Closed to Full
            }
            else // release empty node from empty ring
            {
                _array[-(n->Base)].Check = n->Check;
                _array[-(n->Check)].Base = n->Base;
                if (e == b->Ehead)
                    b->Ehead = -n->Check; // set ehead
                if (bi != 0 && b->Num == 1 && b->Trial != _maxTrial) // Open to Closed
                    _transfer_block(bi, ref _bheadO, ref _bheadC);
            }

            // initialize the released node
            if (label != 0)
            {
                n->Base = -1;
            }                    
            else
            {
                SetNodeValue(n, default(T));
            }                    

            n->Check = from;

            if (@base < 0)
                _array[from].Base = e ^ label;

            return e;
        }

        private T GetNodeValue(node* n)
        {
            return (T)(object)n->Base;
        }

        private void SetNodeValue(node* n, T value)
        {
            n->Base = (int)(object)value;
        }

        private void _transfer_block(int bi, ref int head_in, ref int head_out)
        {
            _pop_block(bi, ref head_in, bi == _block[bi].Next);
            _push_block(bi, ref head_out, head_out == 0 && _block[bi].Num != 0);
        }

        private int _find_place()
        {
            if (_bheadC != 0)
                return _block[_bheadC].Ehead;
            if (_bheadO != 0)
                return _block[_bheadO].Ehead;

            return _add_block() << 8;
        }

        private int _add_block()
        {
            if ( Size == Capacity)
            {
                Capacity += Capacity;

                _array = (node*) Reallocate<node>(_array, Capacity, Capacity);
                _ninfo = (ninfo*) Reallocate<ninfo>(_ninfo, Capacity, Size);
                _block = (block*) Reallocate<block>(_block, Capacity >> 8, Size >> 8, block.Create());                
            }

            _block[Size >> 8].Ehead = Size;
            _array[Size] = new node(-(Size + 255), -(Size + 1));
            for (int i = Size + 1; i < Size + 255; i++)
                _array[i] = new node(-(i - 1), -(i + 1));

            _array[Size + 255] = new node(-(Size + 254), -Size);
            _push_block(Size >> 8, ref _bheadO, _bheadO == 0);
            Size += 256;

            return (Size >> 8) - 1;
        }

        private void _pop_block(int bi, ref int head_in, bool last)
        {
            if ( last )
            {
                // last one poped; Closed or Open
                head_in = 0;
            }
            else
            {
                block* b = _block + bi;
                _block[b->Prev].Next = b->Next;
                _block[b->Next].Prev = b->Prev;
                if (bi == head_in)
                    head_in = b->Next;
            }
        }

        private void _push_block(int bi, ref int head_out, bool empty)
        {
            // TODO: Ensure this can be inlined copying the parameter. 

            block* b = _block + bi;
            if ( empty )
            {
                // the destination is empty
                head_out = b->Prev = b->Next = bi;
            }
            else
            {
                int tail_out = _block[head_out].Prev;
                b->Prev = tail_out;
                b->Next = head_out;
                head_out = tail_out = _block[tail_out].Next = bi;
            }
        }

        private void Restore()
        {
            throw new NotImplementedException();
        }

        private int Begin( ref int from, ref int len )
        {
            throw new NotImplementedException();
        }

        private int Next ( ref int from, ref int len, int root = 0)
        {
            throw new NotImplementedException();
        }

        private void Initialize()
        {
            // Original version
            _array = (node*)Reallocate<node>(_array, 256, 256);
            _ninfo = (ninfo*)Reallocate<ninfo>(_ninfo, 256); // Marshal.Alloc doesn't ensure memory is zeroed out. 
            _block = (block*)Reallocate<block>(_block, 1, 0, block.Create()); // We need to call the constructor, not the default(block)
            _trackingNode = (long*)Reallocate<long>(_trackingNode, _numTrackingNodes + 1);

            _array[0] = new node(0, -1);
            for (int i = 1; i < 256; ++i)
                _array[i] = new node(i == 1 ? -255 : -(i - 1), i == 255 ? -1 : -(i + 1));

            Capacity = Size = 256;

            u1.Ptr = Reallocate<byte>(u1.Tail, sizeof(int));
            u2.Ptr = Reallocate<int>(u2.Tail0, 1);            

            _block[0].Ehead = 1; // bug fix for erase

            this._quota = *u1.Length = sizeof(int);
            this._quota0 = 1;

            for (int i = 0; i < _numTrackingNodes; i++)
                _trackingNode[i] = 0;

            for (int i = 0; i < 256; i++)
                _reject[i] = (short)( i + 1 );
        }

        void* Reallocate<W>(void* ptr, int size_n, int size_p = 0, W value = default(W)) where W : struct
        {
            int wSize = Unsafe.SizeOf<W>();

            byte* tmp;
            if (ptr == null)
            {
                tmp = (byte*)Marshal.AllocHGlobal(wSize * size_n).ToPointer();                
                //Console.WriteLine($"allocating: {(long)tmp} with elements of size {wSize}, to size: {size_n} zero from {size_p}");
            }
            else
            {
                tmp = (byte*)Marshal.ReAllocHGlobal((IntPtr)ptr, (IntPtr)(wSize * size_n)).ToPointer();
                //Console.WriteLine($"ptr: {(long)ptr} with elements of size {wSize}, to size: {size_n} zero from {size_p}");
            }

            if (tmp == null)
                throw new Exception("memory reallocation failed");
            
            byte* current = tmp + (size_p * wSize);

            int count = size_n - size_p;
            for ( int i = 0; i < count; i++)
            {
                Unsafe.Write(current, value);
                current += wSize;
            }

            return tmp;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                SafeRelease(_array);
                SafeRelease(_block);
                SafeRelease(_ninfo);
                SafeRelease(_trackingNode);
                SafeRelease(u1.Ptr);
                SafeRelease(u2.Ptr);

                disposedValue = true;
            }
        }

        private void SafeRelease(void* ptr)
        {
            if ( ptr != null )
                Marshal.FreeHGlobal(new IntPtr(ptr));                
        }

        ~CedarTrie()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    struct node
    {
        public int Check; // negative means next empty index

        public int Base; // negative means prev empty index

        public node(int @base = 0, int check = 0)
        {
            this.Check = check;
            this.Base = @base;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    struct ninfo // x1.5 update speed; +.25 % memory (8n -> 10n)
    {
        public byte Sibling;  // right sibling (= 0 if not exist)
        public byte Child;    // first child

        public ninfo(byte sibling = 0, byte child = 0)
        {
            this.Sibling = sibling;
            this.Child = child;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    struct block // a block w/ 256 elements
    {
        public int Prev;   // prev block; 3 bytes
        public int Next;   // next block; 3 bytes
        public short Num;    // # empty elements; 0 - 256
        public short Reject; // minimum # branching failed to locate; soft limit
        public int Trial;  // # trial
        public int Ehead;  // first empty item

        public block(int prev, int next, short num, short reject, int trial, int ehead)
        {
            this.Prev = prev;
            this.Next = next;
            this.Num = num;
            this.Reject = reject;
            this.Trial = trial;
            this.Ehead = ehead;
        }

        public static block Create(int prev = 0, int next = 0, short num = 256, short reject = 257, int trial = 0, int ehead = 0)
        {
            return new block(prev, next, num, reject, trial, ehead);
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    unsafe struct ElementUnion
    {
        [FieldOffset(0)]
        public void* Ptr;

        [FieldOffset(0)]
        public byte* Tail;
        [FieldOffset(0)]
        public int* Length;

        public ElementUnion(void* ptr)
        {
            this.Ptr = ptr;
            this.Tail = (byte*)ptr;
            this.Length = (int*)ptr;
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    unsafe struct SizeUnion
    {
        [FieldOffset(0)]
        public void* Ptr;

        [FieldOffset(0)]
        public int* Tail0;
        [FieldOffset(0)]
        public int* Length0;

        public SizeUnion(void* ptr)
        {
            this.Ptr = ptr;
            this.Tail0 = (int*)ptr;
            this.Length0 = (int*)ptr;
        }
    }
}


