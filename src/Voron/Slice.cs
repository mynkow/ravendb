using Sparrow;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Voron
{
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct Slice
    {
        public ByteString Content;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Slice(SliceOptions options, ByteString content)
        {
            this.Content = content;
            Content.SetUserDefinedFlags((ByteStringType)options);               
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Slice(ByteString content)
        {
            this.Content = content;
        }

        public bool HasValue
        {
            get { return Content.HasValue; }
        }

        public int Size
        {
            get
            {
                Debug.Assert(Content.Length >= 0);
                return Content.Length;
            }
        }

        public SliceOptions Options
        {
            get
            {
                return (SliceOptions) (Content.Flags & ByteStringType.UserDefinedMask);
            }
        }

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Content.Ptr != null, "Uninitialized slice!");

                if (!Content.HasValue)
                    ThrowUninitialized();

                return *(Content.Ptr + sizeof(byte) * index);
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(Content.Ptr != null, "Uninitialized slice!");

                if (!Content.HasValue)
                    ThrowUninitialized();

                if (!Content.IsMutable)
                    ThrowImmutableCannotBeModified();

                if (index < 0 || index >= Content.Length)
                    ThrowArgumentIsOutOfRange(nameof(index));

                *(Content.Ptr + sizeof(byte) * index) = value;
            }
        }

        private void ThrowUninitialized()
        {
            throw new InvalidOperationException("Uninitialized slice!");
        }

        private void ThrowImmutableCannotBeModified()
        {
            throw new InvalidOperationException("Immutable slice cannot be modified!");
        }

        private void ThrowArgumentIsOutOfRange(string name)
        {
            throw new ArgumentOutOfRangeException(name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Same(Slice other)
        {
            return this.Size == other.Size && this.Content.Ptr == other.Content.Ptr;
        }

        public bool Equals(Slice other)
        {
            return (this.Size == other.Size) && (this.Content.Ptr == other.Content.Ptr || Memory.CompareInline(this.Content.Ptr, other.Content.Ptr, this.Size) == 0);
        }

        public Slice Clone(ByteStringContext context, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.Clone(this.Content, type));
        }

        public Slice Skip(ByteStringContext context, int bytesToSkip, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.Skip(this.Content, bytesToSkip, type));       
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            this.Content.CopyTo(from, dest, offset, count);
        }

        public void CopyTo(byte* dest)
        {
            this.Content.CopyTo(dest);
        }   
         
        public void CopyTo(byte[] dest)
        {
            this.Content.CopyTo(dest);
        }

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            this.Content.CopyTo(from, dest, offset, count);
        }

        public void CopyTo(Slice dest)
        {
            this.Content.CopyTo(dest.Content);
        }

        public void CopyTo(Slice dest, int count)
        {
            this.Content.CopyTo(dest.Content, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope Create(ByteStringContext context, int size, out Slice str, SliceOptions options = SliceOptions.Key)
        {
            ByteString s;
            var scope = context.Allocate(size, out s);
            str = new Slice(options, s);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, string value, out Slice str)
        {
            return From(context, value, ByteStringType.Mutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, string value, ByteStringType type, out Slice str)
        {
            ByteString s;
            var scope = context.From(value, type, out s);
            str = new Slice(s);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte[] value, out Slice str)
        {
            return From(context, value, ByteStringType.Immutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte[] value, ByteStringType type, out Slice str)
        {
            ByteString byteString;
            var scope = context.From(value, 0, value.Length, type, out byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte[] value, int offset, int count, ByteStringType type, out Slice str)
        {
            ByteString byteString;
            var scope = context.From(value, offset, count, type, out byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte* value, int size, out Slice str)
        {
            return From(context, value, size, ByteStringType.Immutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte* value, int size, ByteStringType type, out Slice str)
        {
            ByteString byteString;
            var scope = context.From(value, size, type, out byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, byte* value, int size, out Slice slice)
        {
            return External(context, value, size,  ByteStringType.Mutable | ByteStringType.External, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, byte* value, int size, ByteStringType type, out Slice slice)
        {
            ByteString str;
            var scope= context.FromPtr(value, size, type | ByteStringType.External, out str);
            slice = new Slice(str);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ByteStringContext context)
        {
            if (Content.IsExternal)
                context.ReleaseExternal(ref Content);
            else
                context.Release(ref Content);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Shrink(int length)
        {
            this.Content.Shrink(length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Expand(int length)
        {
            this.Content.Expand(length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSize(int length)
        {
            this.Content.SetLength(length);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            this.Content.Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueReader CreateReader()
        {
            return new ValueReader(Content.Ptr, Size);
        }

        public override int GetHashCode()
        {
            return this.Content.GetHashCode();
        }

        public override string ToString()
        {
            return this.Content.ToString(Encoding.UTF8);
        }

    }

    public static class Slices
    {
        private static readonly ByteStringContext SharedSliceContent = new ByteStringContext();

        public static readonly Slice AfterAllKeys;
        public static readonly Slice BeforeAllKeys;
        public static readonly Slice Empty;

        static Slices()
        {
            ByteString empty;
            SharedSliceContent.From(string.Empty, out empty);
            Empty = new Slice(SliceOptions.Key, empty);
            ByteString before;
            SharedSliceContent.From(string.Empty, out before);
            BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys, before);
            ByteString after;
            SharedSliceContent.From(string.Empty, out after);
            AfterAllKeys = new Slice(SliceOptions.AfterAllKeys, after);
        }
    }
}