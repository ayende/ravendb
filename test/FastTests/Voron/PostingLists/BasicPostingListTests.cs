using System;
using System.Collections.Generic;
using Voron;
using Voron.Data.PostingList;
using Xunit;

namespace FastTests.Voron.PostingLists
{
    public class BasicPostingListTests : StorageTest
    {
        [Fact]
        public void EmptyPostingListHasNoEntries()
        {
            using (var tx = Env.WriteTransaction())
            {
                var reader = PostingListReader.Create(tx, "Name", "Oren");
                Assert.Equal(0, reader.NumberOfEntries);
                Assert.False(reader.ReadNext(out _));
            }
        }


        [Fact]
        public void MultipleWrites()
        {
            using (var tx = Env.WriteTransaction())
            {
                using (var w = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    w.Append(1);
                }
                using (var w = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    w.Append(2);
                    w.Append(13);
                    w.Append(19);
                    w.Append(27);
                    w.Append(31);
                    w.Append(36);
                }
            }
        }

        [Fact]
        public void CanAddAndReadItemsFromPostingList()
        {
            using (var tx = Env.WriteTransaction())
            {
                var list = new List<int>();
                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        list.Add(val);
                        writer.Append(val);
                    }
                }


                var reader = PostingListReader.Create(tx, "Name", "Oren");
                Assert.Equal(20, reader.NumberOfEntries);
                foreach (var item in list)
                {
                    Assert.True(reader.ReadNext(out var v));
                    Assert.Equal(item, v);
                }
                Assert.False(reader.ReadNext(out _));
            }
        }

        [Fact]
        public void CanSeek_ExactValue()
        {
            using (var tx = Env.WriteTransaction())
            {
                var list = new List<int>();
                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        list.Add(val);
                        writer.Append(val);
                    }
                }


                var reader = PostingListReader.Create(tx, "Name", "Oren");
                reader.Seek(list[12]);
                for (int i = 12; i < list.Count; i++)
                {
                    int item = list[i];
                    Assert.True(reader.ReadNext(out var v));
                    Assert.Equal(item, v);
                }
                Assert.False(reader.ReadNext(out _));
            }
        }

        [Fact]
        public void CanSeek_Range()
        {
            using (var tx = Env.WriteTransaction())
            {
                var list = new List<int>();
                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        list.Add(val);
                        writer.Append(val);
                    }
                }


                var reader = PostingListReader.Create(tx, "Name", "Oren");
                reader.Seek(Math.Max(list[12] - 13, list[11]+1));
                for (int i = 12; i < list.Count; i++)
                {
                    int item = list[i];
                    Assert.True(reader.ReadNext(out var v));
                    Assert.Equal(item, v);
                }
                Assert.False(reader.ReadNext(out _));
            }
        }


        [Fact]
        public void AddingToOneTermWontAffectAnother()
        {
            using (var tx = Env.WriteTransaction())
            {
                var list = new List<int>();
                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        list.Add(val);
                        writer.Append(val);
                    }
                }
                
                using (var writer = PostingListWriter.Create(tx, "Name", "Ayende"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        writer.Append(val);
                    }
                }

                // here we should only have the oren's terms
                var reader = PostingListReader.Create(tx, "Name", "Oren");
                Assert.Equal(20, reader.NumberOfEntries);
                foreach (var item in list)
                {
                    Assert.True(reader.ReadNext(out var v));
                    Assert.Equal(item, v);
                }
                Assert.False(reader.ReadNext(out _));
            }
        }
        
        [Fact]
        public void CanDelete()
        {
            const int notThereValue = 13844;

            using (var tx = Env.WriteTransaction())
            {
                var list = new List<int>();
                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        if (val == notThereValue)
                            val++;
                        list.Add(val);
                        writer.Append(val);
                    }
                }

                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = list[11];
                    list.RemoveAt(11);
                    writer.Delete(val);
                    writer.Delete(notThereValue);
                    
                    val = list[3];
                    list.RemoveAt(3);
                    writer.Delete(val);
                }

                // here we should only have the oren's terms
                var reader = PostingListReader.Create(tx, "Name", "Oren");
                Assert.Equal(18, reader.NumberOfEntries);
                for (int i = 0; i < list.Count; i++)
                {
                    int item = list[i];
                    Assert.True(reader.ReadNext(out var v));
                    Assert.Equal(item, v);
                }
                Assert.False(reader.ReadNext(out _));
            }
        }

        [Fact]
        public void CanDeleteInTheMiddle()
        {
            var list = new List<long>();

            using (var tx = Env.WriteTransaction())
            {
                using (var writer = PostingListWriter.Create(tx, "Foo", "Bar"))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        
                        list.Add(i + 1);
                        writer.Append(i + 1);
                    }
                }
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {                
                using (var writer = PostingListWriter.Create(tx, "Foo", "Bar"))
                {
                    writer.Delete(3L);
                    writer.Delete(4L);
                    list.Remove(3);
                    list.Remove(4);
                }
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                // here we should only have the oren's terms
                var reader = PostingListReader.Create(tx, "Foo", "Bar");
                Assert.Equal(8, reader.NumberOfEntries);
                var fetchedList = new List<long>();
                while(reader.ReadNext(out var v))
                    fetchedList.Add(v);

                for (int i = 0; i < list.Count; i++)
                {
                    Assert.Equal(list[i],fetchedList[i]);
                }
            }
        }

        [Fact]
        public void CanDeleteAll()
        {
            const int notThereValue = 13844;

            using (var tx = Env.WriteTransaction())
            {
                var list = new List<int>();
                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    var val = 0;
                    var random = new Random();
                    for (int i = 0; i < 20; i++)
                    {
                        val += random.Next(ushort.MaxValue);
                        if (val == notThereValue)
                            val++;
                        list.Add(val);
                        writer.Append(val);
                    }
                }

                using (var writer = PostingListWriter.Create(tx, "Name", "Oren"))
                {
                    foreach (var item in list)
                    {
                        writer.Delete(item);
                    }
                }

                // here we should only have the oren's terms
                var reader = PostingListReader.Create(tx, "Name", "Oren");
                Assert.Equal(0, reader.NumberOfEntries);
                Assert.Equal((0,0), reader.GetSize());
            }
        }
    }
}
