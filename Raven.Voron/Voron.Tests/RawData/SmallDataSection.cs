﻿using System;
using System.Collections.Generic;
using System.Text;
using Voron.Data.RawData;
using Voron.Tests.FixedSize;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.RawData
{
    public unsafe class SmallDataSection : StorageTest
    {
        [Fact]
        public void CanReadAndWriteFromSection()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long id;
            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                
                AssertValueMatches(section,id, "Hello There");
            }
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void CanAllocateMultipleValues(int seed)
        {
            var random = new Random(seed);

            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
                tx.Commit();
            }
            var dic = new Dictionary<long, int>();
            for (int i = 0; i < 100; i++)
            {
                long id;
                using (var tx = Env.WriteTransaction())
                {
                    var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                    Assert.True(section.TryAllocate(random.Next(16,256), out id));
                    WriteValue(section, id, i.ToString("0000000000000"));
                    dic[id] = i;
                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                    AssertValueMatches(section, id, i.ToString("0000000000000"));
                }
            }

            foreach (var kvp in dic)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                    AssertValueMatches(section, kvp.Key, kvp.Value.ToString("0000000000000"));
                }
            }
        }

        [Fact]
        public void CanReuseSeveralFreeSpacesInTheMiddle()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            var idsToFree = new List<long>();
            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                for (int i = 0; i < 1536; i++)
                {
                    long id;
                    Assert.True(section.TryAllocate(1020, out id));
                    if (i%50 == 0)
                        idsToFree.Add(id);
                }

                idsToFree.ForEach(freeId => section.Free(freeId));
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                foreach (var expectedId in idsToFree)
                {
                    long id;
                    Assert.True(section.TryAllocate(1020, out id));
                    Assert.Equal(id,expectedId);
                }
            }
        }

        [Fact]
        public void CanReuseFreespaceInTheMiddle()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long idToFree = -1;
            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                for (int i = 0; i < 1536; i++)
                {
                    long id;
                    Assert.True(section.TryAllocate(1020, out id));
                    if (i == 700)
                    {
                        idToFree = id;
                    }
                }

                section.Free(idToFree);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                long id;
                Assert.True(section.TryAllocate(1020, out id));
                Assert.Equal(idToFree,id);
            }
        }

        [Fact]
        public void CanAllocateEnoughToFillEntireSection()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long id, idToFree = -1;
            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                for (int i = 0; i < 1536; i++)
                {
                    Assert.True(section.TryAllocate(1020, out id));
                    if (i%77 == 0)
                    {
                        idToFree = id;
                    }
                }


                Assert.False(section.TryAllocate(1020, out id));

                section.Free(idToFree);

                Assert.True(section.TryAllocate(1020, out id));
            }
        }

        [Fact]
        public void CanReadAndWriteFromSection_SingleTx()
        {
            Env.Options.ManualFlushing = true;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);

                long id;
            
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
          

                AssertValueMatches(section, id, "Hello There");

                tx.Commit();
            }
            Env.FlushLogToDataFile();
        }


        [Fact]
        public void CanReadAndWriteFromSection_AfterFlush()
        {
            Env.Options.ManualFlushing = true;
            long pageNumber;
            long id;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
          
                //var section = new RawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
                tx.Commit();
            }
            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                AssertValueMatches(section, id, "Hello There");
            }
        }

        private static void WriteValue(ActiveRawDataSmallSection section, long id, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            fixed (byte* p = bytes)
            {
                Assert.True(section.TryWrite(id, p, bytes.Length));
            }
        }

        private static void AssertValueMatches(ActiveRawDataSmallSection section, long id, string expected)
        {
            int size;
            var p = section.DirectRead(id, out size);
            var chars = new char[Encoding.UTF8.GetMaxCharCount(size)];
            fixed (char* c = chars)
            {
                int bytesUsed;
                int charsUsed;
                bool completed;
                Encoding.UTF8.GetDecoder().Convert(p, size, c, chars.Length, true, out bytesUsed, out charsUsed, out completed);
                Assert.Equal(expected, new string(chars, 0, charsUsed));
            }
        }
    }
}