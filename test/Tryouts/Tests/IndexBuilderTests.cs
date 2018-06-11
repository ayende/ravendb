using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Server.ServerWide.Context;
using Tryouts.Corax;
using Tryouts.Corax.Queries;
using Voron;
using Xunit;

namespace Tryouts.Tests
{
    public class IndexBuilderTests
    {
        [Fact]
        public void Adding_terms_for_entry_should_work()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);
                AddIndexedData(builder);                             

                var reader = new IndexReader(pool);
                using (reader.BeginReading())
                {
                    Assert.Equal(10,reader.Query(new TermQuery(reader, "Name", "John Doe")).Count());
                }
            }
        }

        [Fact]
        public void Removing_entry_should_work()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);
                AddIndexedData(builder);                             

                using (builder.BeginIndexing())
                {
                    builder.DeleteEntry("users/2");
                    builder.DeleteEntry("users/3");
                    
                    builder.CompleteIndexing();
                }

                var reader = new IndexReader(pool);
                using (reader.BeginReading())
                {
                    Assert.Equal(8,reader.Query(new TermQuery(reader, "Name", "John Doe")).Count());
                }
            }
        }

        [Fact]
        public void Trying_to_remove_non_existing_entry_should_be_ignored()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);
                AddIndexedData(builder);                             

                using (builder.BeginIndexing())
                {
                    builder.DeleteEntry("users/non-existing-id");                    
                    builder.DeleteEntry("users/other-non-existing-id");                    
                    builder.CompleteIndexing();
                }

                var reader = new IndexReader(pool);
                using (reader.BeginReading())
                {
                    Assert.Equal(10,reader.Query(new TermQuery(reader, "Name", "John Doe")).Count());
                }
            }
        }

        private static void AddIndexedData(IndexBuilder builder)
        {
            using (builder.BeginIndexing())
            {
                for (int ix = 0; ix < 10; ix++)
                {
                    builder.NewEntry("users/" + ix);
                    builder.Term("Name", "John Doe");
                    builder.Term("Lang", "Hebrew");
                    builder.FinishEntry();
                }

                builder.CompleteIndexing();
            }
        }
    }
}
