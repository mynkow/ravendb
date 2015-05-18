using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Linq
{
    public class RavenFlatBufferSerialization
    {
        public class Recursive
        {
            public int Id { get; set; }
            public Recursive Recurse { get; set; }
        }

        public class RecursiveWithArray
        {
            public int Id { get; set; }

            public Recursive[] Recurse { get; set; }
        }

        public class RecursiveWithList
        {
            public int Id { get; set; }

            public List<Recursive> Recurse { get; set; }
        }

        private class EmptyUser
        {
        }

        private class EmptyObjectThenArray
        {
            public EmptyUser User { get; set; }
            public int[] Array { get; set; }
        }

        private class SimplierUser
        {
            public string Id { get; set; }
        }

        private class SimplierObjectThenArray
        {
            public SimplierUser User { get; set; }
            public int[] Array { get; set; }
        }

        private class ObjectThenArray
        {
            public User User { get; set; }
            public int[] Array { get; set; }
        }

        [Fact]
        public void Flat_WithObject()
        {
            var user = RavenJObject.FromObject(new User { Id = "users/10203", Active = true, Created = DateTime.Now, Age = 38, Info = "Testing Info", Name = "This user" });

            EnsureIsEquivalentAfterSerialization(user);
        }

        [Fact]
        public void Flat_WithRecursiveObject()
        {
            var r = new Recursive
            {
                Id = 10,
                Recurse = new Recursive
                {
                    Id = 22,
                    Recurse = new Recursive
                    {
                        Id = 203
                    }
                }
            };

            EnsureIsEquivalentAfterSerialization(RavenJObject.FromObject(r));
        }

        [Fact]
        public void Flat_WithRecursiveArray()
        {
            var r = new RecursiveWithArray
            {
                Id = 10,
                Recurse = new[] 
                { 
                    new Recursive
                    {
                        Id = 22,
                        Recurse = new Recursive
                        {
                            Id = 203
                        }
                    },
                    new Recursive
                    {
                        Id = 322
                    }                                
                }
            };

            EnsureIsEquivalentAfterSerialization(RavenJObject.FromObject(r));
        }

        [Fact]
        public void Flat_WithRecursiveList()
        {
            var r = new RecursiveWithList
            {
                Id = 10,
                Recurse = new List<Recursive>
                { 
                    new Recursive
                    {
                        Id = 22,
                        Recurse = new Recursive
                        {
                            Id = 203
                        }
                    },
                    new Recursive
                    {
                        Id = 322
                    }                                
                }
            };

            EnsureIsEquivalentAfterSerialization(RavenJObject.FromObject(r));
        }


        [Fact]
        public void Flat_WithArray()
        {
            var users = new[] 
            { 
                new User { Id = "users/10203", Active = true, Created = DateTime.Now, Age = 38, Info = "Testing Info", Name = "This user" },
                new User { Id = "users/10313", Active = false, Created = DateTime.Now.AddDays(1), Age = 12, Info = "Another Testing Info", Name = "Other user" },
                new User { Id = "users/433", Active = false, Created = DateTime.MinValue, Age = -412322, Info = "Info", Name = "Other usersss" },
            };

            EnsureIsEquivalentAfterSerialization(RavenJArray.FromObject(users));
        }

        [Fact]
        public void Flat_WithEmptyObjectThenArray()
        {
            var @object = new EmptyObjectThenArray()
            {
                User = new EmptyUser(),
                Array = new int[] { 1, 2, 3, 4, 5 }
            };

            EnsureIsEquivalentAfterSerialization(RavenJToken.FromObject(@object));
        }

        [Fact]
        public void Flat_WithSimplierObjectThenArray()
        {
            var @object = new SimplierObjectThenArray()
            {
                User = new SimplierUser { Id = "users/10203" },
                Array = new int[] { 1, 2, 3, 4, 5 }
            };

            EnsureIsEquivalentAfterSerialization(RavenJToken.FromObject(@object));
        }

        [Fact]
        public void Flat_WithObjectThenArray()
        {
            var @object = new ObjectThenArray()
            {
                User = new User { Id = "users/10203", Active = true, Created = DateTime.Now, Age = 38, Info = "Testing Info", Name = "This user" },
                Array = new int[] { 1, 2, 3, 4, 5 }
            };

            EnsureIsEquivalentAfterSerialization(RavenJToken.FromObject(@object));
        }

        private void EnsureIsEquivalentAfterSerialization(RavenJToken token)
        {
            var memoryStream = new MemoryStream();
            token.WriteTo(new RavenFlatWriter(memoryStream));

            memoryStream.Position = 0;
            var deserializedObject = RavenJToken.Load(new RavenFlatReader(memoryStream));
                        
            Assert.True(RavenJToken.DeepEquals(token, deserializedObject), token.ToString() + " " + deserializedObject.ToString() );
        }
    }
}
