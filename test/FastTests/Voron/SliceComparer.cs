using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Voron;
using Xunit;

namespace FastTests.Voron
{
    public class SliceComparerTest : StorageTest
    {
        [Fact]
        public void InBetween()
        {
            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "1"), Slices.BeforeAllKeys, Slices.AfterAllKeys));
            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "1"), Slices.BeforeAllKeys, Slice.From(this.Allocator, "1")));
            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "1"), Slices.BeforeAllKeys, Slice.From(this.Allocator, "2")));

            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "2"), Slice.From(this.Allocator, "1"), Slices.AfterAllKeys));
            Assert.False(SliceComparer.InBetween(Slice.From(this.Allocator, "2"), Slice.From(this.Allocator, "3"), Slices.AfterAllKeys));
            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "21"), Slice.From(this.Allocator, "2"), Slices.AfterAllKeys));

            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "2000000"), Slice.From(this.Allocator, "2000000"), Slices.AfterAllKeys));
            Assert.False(SliceComparer.InBetween(Slice.From(this.Allocator, "20000001"), Slice.From(this.Allocator, "20000002"), Slices.AfterAllKeys));
            Assert.True(SliceComparer.InBetween(Slice.From(this.Allocator, "20000002"), Slice.From(this.Allocator, "20000001"), Slices.AfterAllKeys));

        }
    }
}
