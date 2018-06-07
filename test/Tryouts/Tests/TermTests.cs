using System;
using Raven.Server.ServerWide.Context;
using Tryouts.Corax;
using Voron;
using Xunit;

namespace Tryouts.Tests
{
    public class TermTests
    {
        [Fact]
        public void Indexing_without_calling_CompleteIndexing_should_return_zero_freq()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);
                using (builder.BeginIndexing())
                {
                    builder.NewEntry("users/1");
                    builder.Term("Name", "John Doe");
                    builder.Term("Lang", "English");
                    builder.Term("Lang", "Hebrew");
                    builder.Term("Lang", "English");
                    builder.FinishEntry();
                }

                var reader = new IndexReader(pool);
                using (reader.BeginReading())
                {
                    Assert.Equal(0, reader.GetTermFreq("English"));
                }
            }
        }

        [Fact]
        public void Term_freq_for_double_term_in_single_entry_should_be_counted_twice()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);
                using (builder.BeginIndexing())
                {
                    builder.NewEntry("users/1");
                    builder.Term("Name", "John Doe");
                    builder.Term("Lang", "English");
                    builder.Term("Lang", "Hebrew");
                    builder.Term("Lang", "English");
                    builder.FinishEntry();

                    builder.CompleteIndexing();
                }

                var reader = new IndexReader(pool);
                using (reader.BeginReading())
                {
                    Assert.Equal(2, reader.GetTermFreq("English"));
                }
            }
        }

        [Fact]
        public void Term_freq_should_be_counted_properly()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            using (var pool = new TransactionContextPool(env))
            {
                CreateIndexData(pool);
                
                var reader = new IndexReader(pool);
                using (reader.BeginReading())
                {
                    Assert.Equal(2, reader.GetTermFreq("English"));
                    Assert.Equal(1, reader.GetTermFreq("Hebrew"));
                    Assert.Equal(3, reader.GetTermFreq("Yiddish"));
                }
            }
        }

        private static void CreateIndexData(TransactionContextPool pool)
        {
            var builder = new IndexBuilder(pool);
            using (builder.BeginIndexing())
            {
                builder.NewEntry("users/1");
                    builder.Term("Name", "John Doe");
                    builder.Term("Lang", "French");
                    builder.Term("Lang", "Yiddish");
                builder.FinishEntry();

                builder.NewEntry("users/2");
                    builder.Term("Name", "Jack Doe");
                    builder.Term("Lang", "English");
                    builder.Term("Lang", "Yiddish");
                builder.FinishEntry();

                builder.NewEntry("users/3");
                    builder.Term("Name", "Jane Doe");
                    builder.Term("Lang", "Spanish");
                    builder.Term("Lang", "Hebrew");
                    builder.Term("Lang", "Yiddish");
                    builder.Term("Lang", "English");
                builder.FinishEntry();

                builder.CompleteIndexing();
            }
        }
    }
}
