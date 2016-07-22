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

        public struct CedarNode
        {
            public int Check; // negative means next empty index

            public int Base; // negative means prev empty index

            // This is the reason why T cannot be bigger than int. 
            public T Value
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (T)(object)Base; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { Base = (int)(object)value; }
            }
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
        private int _capacity;
        private int _size;

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
            get { return _nodeSize * Size; }
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

                for (int to = 0; to < _size; to++)
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
            byte* tail;
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

                    throw new NotImplementedException();
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
                        u2.Ptr = Reallocate<int>( u2.Ptr, *u2.Length0);
                    }
                    u2.Tail0[*u2.Length0] = i;
                }
                if (pos == len || tail[pos] == '\0')
                {
                    long to = Follow(from, 0);
                    if ( pos == len )
                    {
                        IncrementNodeValue(_array + to, value);
                        return;
                    }

                    IncrementNodeValue(_array + to, (T)(object)tail[pos + 1]);                                                     
                }
                from = Follow(from, key[pos]);
                pos++;

            }

            //
            int needed = (int)(len - pos + 1 + _tSize);
            if ( pos == len && *u2.Length0 != 0)
            {
                throw new NotImplementedException();
            }

            if ( _quota < *u1.Length + needed )
            {
                _quota += _quota >= needed ? _quota : needed;

                // Reallocating Tail.
                u1.Ptr = Reallocate<int>( u1.Ptr, _quota, *u1.Length);
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

            u1.Length += needed;

            *(u1.Tail + (len + 1)) += (byte)(object)value;
        }



        private void IncrementNodeValue(node* node, T value)
        {
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

        private int _resolve(long from, int @base, byte label)
        {
            throw new NotImplementedException();
        }

        private void _push_sibling(long from, int @base, byte label, bool flag = true)
        {
            byte* c = &(_ninfo[from].Child);
            if ( flag && (typeof(TOrdered) == typeof(Ordered)) ? label > *c : *c == 0)
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
                
            if (b->Num - 1 == 0)
            {
                if (bi != 0)
                    _transfer_block(bi, _bheadC, _bheadF); // Closed to Full
            }
            else // release empty node from empty ring
            {
                _array[-(n->Base)].Check = n->Check;
                _array[-(n->Check)].Base = n->Base;
                if (e == b->Ehead)
                    b->Ehead = -n->Check; // set ehead
                if (bi != 0 && b->Num == 1 && b->Trial != _maxTrial) // Open to Closed
                    _transfer_block(bi, _bheadO, _bheadC);
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

        private void _transfer_block(int bi, int _bheadC, int _bheadF)
        {
            throw new NotImplementedException();
        }

        private int _find_place()
        {
            throw new NotImplementedException();
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
            _array = (node*)Reallocate<node>(_array, 256, 256);
            _ninfo = (ninfo*)Reallocate<ninfo>(_ninfo, 256, 256);
            _block = (block*)Reallocate<block>(_block, 1);
            _trackingNode = (long*)Reallocate<long>(_trackingNode, _numTrackingNodes + 1);

            _array[0] = new node(0, -1);
            for (int i = 1; i < 256; ++i)
                _array[i] = new node(i == 1 ? -255 : -(i - 1), i == 255 ? -1 : -(i + 1));

            Capacity = Size = 256;

            u1.Ptr = Reallocate<byte>(u1.Tail, sizeof(int));
            u2.Ptr = Reallocate<int>(u2.Tail0, 1);

            _block[0].Ehead = 1; // bug fix for erase

            this._quota = sizeof(int);
            this._quota0 = 1;

            for (int i = 0; i < _numTrackingNodes; i++)
                _trackingNode[i] = 0;

            for (int i = 0; i < 256; i++)
                _reject[i] = (short)( i + 1 );
        }

        void* Reallocate<W>(void* ptr, int size_n, int size_p = 0) where W : struct
        {
            int wSize = Unsafe.SizeOf<W>();

            byte* tmp;
            if (ptr == null)
            {
                tmp = (byte*)Marshal.AllocHGlobal(wSize * size_n).ToPointer();
            }
            else
            {
                tmp = (byte*)Marshal.ReAllocHGlobal((IntPtr)ptr, (IntPtr)(wSize * size_n)).ToPointer();
            }

            if (tmp == null)
                throw new Exception("memory reallocation failed");
            
            W W0 = default(W);

            byte* current = tmp + (size_p * wSize);

            int count = size_n - size_p;
            for ( int i = 0; i < count; i++)
            {
                Unsafe.Write(current, W0);
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

        public block(int prev = 0, int next = 0, short num = 256, short reject = 257, int trial = 0, int ehead = 0)
        {
            this.Prev = prev;
            this.Next = next;
            this.Num = num;
            this.Reject = reject;
            this.Trial = trial;
            this.Ehead = ehead;
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


