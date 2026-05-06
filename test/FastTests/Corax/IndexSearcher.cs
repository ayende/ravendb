using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Corax;
using Corax.Analyzers;
using Corax.Querying;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Queries;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;
using VoronConstants = Voron.Global.Constants;

namespace FastTests.Corax
{
    public class IndexSearcherTest : StorageTest
    {
        public IndexSearcherTest(ITestOutputHelper output) : base(output)
        {
        }
        [RavenFact(RavenTestCategory.Corax)]
        public void CanDeleteDifferentLongAndDoubleInSingleEntry()
        {
            var entry1 = new IndexSingleEntry() {Id = "e/1", Content = "2023-08-02T12:01:34.2111452"};
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);
            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                using (var builder = indexWriter.Index(entry1.Id))
                {
                    builder.Write(IdIndex, PrepareString(entry1.Id));
                    var dateTime = DateTime.Parse(entry1.Content);
                    builder.Write(ContentIndex, Encodings.Utf8.GetBytes(entry1.Content), dateTime.Ticks, dateTime.Ticks);
                    double doubleVal = dateTime.Ticks;
                    Assert.NotEqual(dateTime.Ticks, (long)doubleVal);
                    builder.EndWriting();
                }

                indexWriter.Commit();
            }

            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                Assert.True(indexWriter.TryDeleteEntry("e/1"));
                indexWriter.Commit();
            }

            using (var indexSearcher = new IndexSearcher(Env, knownFields))
            {
                Assert.True(knownFields.TryGetByFieldId(ContentIndex, out var binding));
                var query = indexSearcher.BetweenQuery(binding.Metadata, double.MinValue, double.MaxValue, UnaryMatchOperation.GreaterThanOrEqual,
                    UnaryMatchOperation.LessThanOrEqual);
                Span<long> ids = stackalloc long[64];

                Assert.Equal(0, query.Fill(ids));
            }            
        }
        
        [RavenFact(RavenTestCategory.Corax)]
        public void GetTermFromEntryIdViaEntriesFields()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"muddy", "road"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Content", "road");
                Assert.Equal(2, match.Count);
                Assert.Equal(2, match.Fill(ids));
                using var reader = searcher.TermsReaderFor("Content");
                Assert.True(reader.TryGetTermFor(ids[0], out string term));
                Assert.Equal("lake", term);
                Assert.True(reader.TryGetTermFor(ids[1], out  term));
                Assert.Equal("muddy", term);
            }
        }
        
        [RavenFact(RavenTestCategory.Corax)]
        public void CanCompareEntriesDirectly()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"muddy", "road"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Content", "road");
                Assert.Equal(2, match.Count);
                Assert.Equal(2, match.Fill(ids));
                using var reader = searcher.TermsReaderFor("Content");
                Assert.True(ids[0] < ids[1]);

                var term0 = entry1.Content.OrderBy(x => x).First();
                var term1 = entry2.Content.OrderBy(x => x).First();

                var nullResults = -1;
                var cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[0]), reader.GetTerm(ids[1]), nullResults);
                Assert.Equal(string.Compare(term0, term1, StringComparison.Ordinal),Math.Sign(cmp));
                cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[1]), reader.GetTerm(ids[0]), nullResults);
                Assert.Equal(string.Compare(term1, term0, StringComparison.Ordinal), Math.Sign(cmp));
                cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[0]), reader.GetTerm(ids[0]), nullResults);
                Assert.Equal(string.Compare(term0, term0, StringComparison.Ordinal), Math.Sign(cmp));
                cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[1]), reader.GetTerm(ids[1]), nullResults);
                Assert.Equal(string.Compare(term1, term1, StringComparison.Ordinal), Math.Sign(cmp));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyTerm()
        {
            var entry = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Unknown", "1");
                Assert.Equal(0, match.Count);
                Assert.Equal(0, match.Fill(ids));

                match = searcher.TermQuery("Id", "1");
                Assert.Equal(0, match.Count);
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleTerm()
        {
            var entry = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Id", "entry/1");
                Assert.Equal(1, match.Count);
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SmallSetTerm()
        {
            var entries = new IndexEntry[16];
            for (int i = 0; i < entries.Length; i++)
            {
                entries[i] = new IndexEntry {Id = $"entry/{i}", Content = new string[] {"road"},};
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[12];
                ids.Fill(-1);

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Content", "road");

                Assert.Equal(16, match.Count);

                Assert.Equal(12, match.Fill(ids));
                Assert.False(ids.Contains(-1));

                ids.Fill(-1);
                Assert.Equal(4, match.Fill(ids));
                Assert.True(ids.Contains(-1));

                Assert.Equal(0, match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleAndNoDuplication()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('road', 'lake')");
                Assert.Equal(2, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                // RQL migration: AND query via QueryPlanBuilder
                // WHERE Id = 'entry/1' AND Content = 'mountain'
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");

                Assert.Equal(1, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                // RQL: WHERE Id = 'entry/1' AND Content = 'mountain'
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");

                Assert.Equal(2, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllAndWithEmpty()
        {
            var entries = Enumerable.Range(1, 10_000).Select(i => new IndexEntry {Id = $"entry/{i}", Content = new string[] {"road", "lake", "mountain"}}).ToArray();


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'Maciej'");
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllAndMemoized()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");
                Assert.Equal(2, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/3' OR Content = 'highway'");
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results1 = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' OR Content = 'highway'");
                Assert.Equal(1, results1.Count);

                var results2 = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/3' OR Content = 'mountain'");
                Assert.Equal(1, results2.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' OR Content = 'mountain'");
                Assert.Equal(2, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllOrInBatches()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"trail", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' OR Content = 'mountain'");
                Assert.Equal(3, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleAndOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"sky", "space"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE (Id = 'entry/1' AND Content = 'mountain') OR Id = 'entry/3'");
                Assert.Equal(2, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/3' OR (Id = 'entry/1' AND Content = 'mountain')");
                Assert.Equal(2, results.Count);
            }
        }


        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(new object[] {10, 3})]
        [InlineData(new object[] {8000, 18})]
        [InlineData(new object[] {1000, 8})]
        [InlineData(new object[] {1020, 7})]
        [InlineData(new object[] {201, 128})]
        public void SimpleAndOrForBiggerSet(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);
            var matches = new List<IndexEntry>();

            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] {"road", "lake", "mountain"},
                        1 => new string[] {"road", "mountain"},
                        2 => new string[] {"sky", "space", "lake"},
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };

                if (entry.Content.Contains("lake") && entry.Content.Contains("mountain") || entry.Content.Contains("space"))
                {
                    matches.Add(entry);
                }

                entriesToIndex[i] = entry;
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            var matchesId = matches.Select(x => x.IndexEntryId).ToList();
            matchesId.Sort();
            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE (Content = 'lake' AND Content = 'mountain') OR Content = 'space'");
                var sortedResults = results.ToArray();
                Array.Sort(sortedResults);

                Assert.Equal(matchesId.Count, results.Count);

                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(matchesId[i], sortedResults[i]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleInStatement()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"sky", "space"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('road', 'space')");
                Assert.Equal(3, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('sky', 'space')");
                Assert.Equal(1, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('road', 'mountain', 'space')");
                Assert.Equal(3, results.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(new object[] {1000, 8})]
        [InlineData(new object[] {300, 128})]
        [InlineData(new object[] {10000, 128})]
        public void AndInStatement(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);

            var matches = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] {"road", "lake", "mountain"},
                        1 => new string[] {"road", "mountain"},
                        2 => new string[] {"sky", "space", "lake"},
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };

                entriesToIndex[i] = entry;
                if ((entry.Content.Contains("lake") || entry.Content.Contains("mountain")) && entry.Content.Contains("sky"))
                {
                    matches.Add(entry);
                }
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            var matchIds = matches.Select(x => x.IndexEntryId).ToArray();
            Array.Sort(matchIds);

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE (Content IN ('lake', 'mountain')) AND Content = 'sky'");
                var resultsSorted = results.ToArray();
                Array.Sort(resultsSorted);

                Assert.Equal((setSize / 3), results.Count);

                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(matchIds[i], resultsSorted[i]);
                }
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'sky' AND (Content IN ('lake', 'mountain'))");
                var resultsSorted = results.ToArray();
                Array.Sort(resultsSorted);

                Assert.Equal((setSize / 3), results.Count);

                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(matchIds[i], resultsSorted[i]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllIn()
        {
            var entry0 = new IndexEntry
            {
                Id = "entry/0",
                Content = new string[]
                {
                    "quo", "consequatur?", "officia", "in", "pariatur.", "illo", "minim", "nihil", "consequuntur", "eum", "consequuntur", "error", "qui", "et",
                    "eos", "minim", "numquam", "commodo", "architecto", "ut", "Cicero", "deserunt", "Finibus", "sunt", "nesciunt.", "molestiae", "Quis",
                    "THIS_IS_UNIQUE_VALUE,", "eum", "in"
                },
            };
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[]
                {
                    "incididunt", "fugiat", "quia", "consequatur?", "magnam", "officia", "elit,", "illum", "ipsa", "of", "culpa", "ea", "voluptas", "Duis",
                    "voluptatem", "Lorem", "modi", "qui", "Sed", "veritatis", "written", "ea", "mollit", "sint", "porro", "ratione", "THIS_IS_UNIQUE_VALUE,",
                    "consectetur", "laudantium,", "aliquam"
                },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[]
                {
                    "laboris", "natus", "Neque", "consequatur,", "qui", "ut", "natus", "illo", "Quis", "voluptas", "eaque", "quasi", "", "aut", "esse", "sed",
                    "qui", "aut", "eos", "eius", "quia", "esse", "aliquip", "", "vel", "quia", "aliqua.", "quia", "consequatur,", "Sed"
                },
            };
            var entry3 = new IndexEntry
            {
                Id = "entry/3",
                Content = new string[]
                {
                    "enim", "aliquid", "voluptas", "Finibus", "eaque", "esse", "Duis", "aut", "voluptatem.", "reprehenderit", "ad", "illum", "consequatur?",
                    "architecto", "velit", "esse", "veniam,", "amet,", "voluptatem", "accusantium", "THIS_IS_UNIQUE_VALUE.", "dolore", "eum", "laborum.", "ipsam",
                    "of", "explicabo.", "voluptatem", "et", "quis"
                },
            };
            var entry4 = new IndexEntry
            {
                Id = "entry/4",
                Content = new string[]
                {
                    "incididunt", "id", "ratione", "inventore", "pariatur.", "molestiae", "dolor", "sit", "Nemo", "de", "nulla", "et", "proident,", "quae",
                    "ipsam", "iste", "in", "dolore", "culpa", "enim", "dolor", "consectetur", "veritatis", "of", "45", "fugiat", "magnam", "Bonorum", "dolor",
                    "beatae"
                },
            };
            var entry5 = new IndexEntry
            {
                Id = "entry/5",
                Content = new string[]
                {
                    "laboriosam,", "totam", "voluptate", "et", "sit", "culpa", "reprehenderit", "eius", "accusantium", "", "omnis", "beatae", "amet,", "nulla",
                    "tempor", "ullamco", "dolor", "ipsam", "vel", "THIS_IS_UNIQUE_VALUE", "quia", "", "consequatur,", "labore", "aliqua.", "dicta", "nostrum",
                    "ut", "dolorem", "Duis"
                },
            };
            var entry6 = new IndexEntry
            {
                Id = "entry/6",
                Content = new string[]
                {
                    "enim", "sed", "ad", "deserunt", "eu", "omnis", "voluptate", "in", "qui", "rem", "sunt", "tempor", "voluptatem", "vel", "enim", "velit",
                    "velit", "aliquip", "by", "in", "eum", "dolore", "incidunt", "commodi", "anim", "amet,", "quo", "est,", "ratione", "sit"
                },
            };
            var entry7 = new IndexEntry
            {
                Id = "entry/7",
                Content = new string[]
                {
                    "sed", "qui", "esse", "THIS_IS_UNIQUE_VALUE", "dolore", "totam", "Nemo", "veniam,", "reprehenderit", "consequuntur", "consequuntur",
                    "aperiam,", "fugiat", "sed", "corporis", "45", "culpa", "accusantium", "quae", "dolor", "voluptate", "dolor", "et", "explicabo.", "voluptate",
                    "Nemo", "tempora", "accusantium", "dolore", "in"
                },
            };
            var entry8 = new IndexEntry
            {
                Id = "entry/8",
                Content = new string[]
                {
                    "nihil", "velit", "quia", "amet,", "fugit,", "eiusmod", "magna", "aliqua.", "ullamco", "accusantium", "nulla", "ex", "sit", "quo", "sit",
                    "sit", "enim", "qui", "sunt", "aspernatur", "laboris", "autem", "voluptas", "amet,", "ipsa", "commodo", "minima", "consectetur,", "fugiat",
                    "voluptas"
                },
            };
            var entry9 = new IndexEntry
            {
                Id = "entry/9",
                Content = new string[]
                {
                    "dolorem", "ipsa", "in", "omnis", "ullamco", "ab", "esse", "aut", "rem", "eu", "iure", "ad", "consequuntur", "est", "adipisci", "velit",
                    "inventore", "nesciunt.", "ad", "vitae", "laborum.", "esse", "voluptate", "et", "fugiat", "fugiat", "voluptas", "quae", "dolor", "qui"
                },
            };
            var entries = new[] {entry0, entry1, entry2, entry3, entry4, entry5, entry6, entry7, entry8, entry9};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'quo' AND Content = 'in'");
                Assert.Equal(2, results.Count);
            }

            {
                // ALL IN requires every listed term to be present — only entry9 has all 27.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content ALL IN ('dolorem', 'ipsa', 'in', 'omnis', 'ullamco', 'ab', 'esse', 'aut', 'rem', 'eu', 'iure', 'ad', 'consequuntur', 'est', 'adipisci', 'velit', 'inventore', 'nesciunt.', 'vitae', 'laborum.', 'voluptate', 'et', 'fugiat', 'voluptas', 'quae', 'dolor', 'qui')");
                Assert.Equal(1, results.Count);
            }

            {
                // One term replaced with a unique value no entry has → 0 results.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content ALL IN ('dolorem', 'ipsa', 'in', 'omnis', 'ullamco', 'ab', 'esse', 'aut', 'rem', 'eu', 'iure', 'ad', 'consequuntur', 'est', 'adipisci', 'velit', 'inventore', 'nesciunt.', 'vitae', 'laborum.', 'voluptate', 'et', 'fugiat', 'voluptas', 'quae', 'dolor', 'THIS_IS_SUPER_UNIQUE_VALUE')");
                Assert.Equal(0, results.Count);
            }
        }


        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleStartWithStatement()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"a road", "a lake", "the mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"a road", "the mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"the sky", "the space", "an animal"},};


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match = searcher.StartWithQuery("Content", "a");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.StartWithQuery("Content", "the s");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.StartWithQuery("Content", "an");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.StartWithQuery("Content", "a");

                Span<long> ids = stackalloc long[2];

                int idCount = match.Fill(ids);
                Assert.NotEqual(0, idCount);
                idCount += match.Fill(ids);
                Assert.NotEqual(0, idCount);
                Assert.Equal(0, match.Fill(ids));

                Assert.Equal(3, idCount);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void MixedSortedMatchStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"4", "2"},};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry3}, CreateKnownFields(bsc));
            IndexEntries(bsc, new[] {entry2}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            OrderMetadata orderMetadata = new OrderMetadata(contentMetadata, true, MatchCompareFieldType.Sequence, fieldHasNoTerms: false);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata, take: 16, nullFirst: true);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [RavenFact(RavenTestCategory.Corax)]
        public void WillGetTotalNumberOfResultsInPagedQuery()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"4", "2"},};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry3}, CreateKnownFields(bsc));
            IndexEntries(bsc, new[] {entry2}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            OrderMetadata orderMetadata = new OrderMetadata(contentMetadata, true, MatchCompareFieldType.Sequence, fieldHasNoTerms: false);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata, nullFirst: true);

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));

                Assert.Equal(3, match.TotalResults);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void CanGetAllEntries()
        {
            var list = new List<IndexSingleEntry>();
            int i;
            for (i = 0; i < 1024; ++i)
            {
                list.Add(new IndexSingleEntry() {Id = $"entry/{i + 1}", Content = i.ToString()});
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, list, CreateKnownFields(bsc));
            IndexEntries(bsc, new[] {new IndexEntry() {Id = $"entry/{i + 1}"}}, CreateKnownFields(bsc));

            list.Add(new IndexSingleEntry() {Id = $"entry/{i + 1}"});

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var all = searcher.AllEntries();
                var results = new List<string>();
                int read;
                Span<long> ids = stackalloc long[256];
                while ((read = all.Fill(ids)) != 0)
                {
                    for (i = 0; i < read; ++i)
                    {
                        long id = ids[i];
                        results.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                    }
                }

                results.Sort();
                list.Sort((x, y) => x.Id.CompareTo(y.Id));
                Assert.Equal(list.Count, results.Count);
                for (i = 0; i < all.Count; ++i)
                    Assert.Equal(list[i].Id, results[i]);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleSortedMatchStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(bsc));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            OrderMetadata orderMetadata = new OrderMetadata(contentMetadata, true, MatchCompareFieldType.Sequence, fieldHasNoTerms: false);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata, nullFirst: true);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));

                long id = ids[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                long id1 = ids[1];
                Assert.Equal("entry/2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1));
                long id2 = ids[2];
                Assert.Equal("entry/1", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id2));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata, take: 16, nullFirst: true);

                Span<long> ids1 = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids1));
                long id = ids1[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                long id1 = ids1[1];
                Assert.Equal("entry/2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1));

                Span<long> ids2 = stackalloc long[2];
                Assert.Equal(1, match.Fill(ids2));
                long id2 = ids2[0];
                Assert.Equal("entry/1", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id2));

                Assert.Equal(0, match.Fill(ids2));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleOrdinalCompareStatementWithLongValue()
        {
            var list = new List<IndexSingleEntryDouble>();
            for (int i = 1; i < 1001; ++i)
                list.Add(new IndexSingleEntryDouble() {Id = $"entry/{i}", Content = (double)i});
            List<string> qids = new();
            IndexEntriesDouble(list);
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content < 25");
                qids = results.Select(id => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id)).ToList();

                foreach (IndexSingleEntryDouble indexSingleEntryDouble in list)
                {
                    bool isIn = qids.Contains(indexSingleEntryDouble.Id);
                    if (indexSingleEntryDouble.Content >= 25D)
                        Assert.False(isIn);
                    else
                        Assert.True(isIn);
                }
            }

            qids.Clear();
            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= 100 AND Content <= 200");
                qids = results.Select(id => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id)).ToList();

                foreach (IndexSingleEntryDouble indexSingleEntryDouble in list)
                {
                    bool isIn = qids.Contains(indexSingleEntryDouble.Id);
                    if (indexSingleEntryDouble.Content is >= 100L and <= 200L)
                        Assert.True(isIn);
                    else
                        Assert.False(isIn);
                }
            }
        }


        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleOrdinalCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content > '1'");
                Assert.Equal(2, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '1'");
                Assert.Equal(3, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content < '1'");
                Assert.Equal(0, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content <= '1'");
                Assert.Equal(1, results.Count);
            }
        }


        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleEqualityCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "1"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = '1'");
                Assert.Equal(2, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '1'");
                Assert.Equal(1, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = '4'");
                Assert.Equal(0, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '4'");
                Assert.Equal(3, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleWildcardStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "Testing"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "Running"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "Runner"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            using var _ = Slice.From(bsc, "ing", out var ingSlice);

            Slice.From(bsc, "1", out var one);
            Slice.From(bsc, "4", out var four);

            {
                var match = searcher.ContainsQuery(contentMetadata, ingSlice);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "er");
                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                long id = ids[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            }

            {
                var match = searcher.StartWithQuery(contentMetadata, "Run", true);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                long id = ids[0];
                Assert.Equal("entry/1", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            }

            {
                var match = searcher.EndsWithQuery(contentMetadata, "ing", false);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                long id = ids[0];
                long id1 = ids[1];
                var results = new[] {searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id), searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1)};
                Array.Sort(results);
                Assert.Equal("entry/1", results[0]);
                Assert.Equal("entry/2", results[1]);
            }

            {
                var match = searcher.EndsWithQuery(contentMetadata, "ing", true);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                long id = ids[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "Run");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "nn");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "run");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.EndsWithQuery(contentMetadata, "ing");
                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleBetweenCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};
            var entry4 = new IndexSingleEntry {Id = "entry/4", Content = "4"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3, entry4}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '1' AND Content <= '2'");
                Assert.Equal(2, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '0' AND Content <= '3'");
                Assert.Equal(3, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '0' AND Content <= '0'");
                Assert.Equal(0, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '1' AND Content <= '1'");
                Assert.Equal(1, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void BetweenWithCustomComparers()
        {
            var entries = Enumerable.Range(0, 100).Select(i => new IndexSingleEntryDouble() {Id = $"entry{i}", Content = Convert.ToDouble(i)}).ToList();
            IndexEntriesDouble(entries);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= 20 AND Content <= 30");
                Assert.Equal(entries.Count(i => i.Content is >= 20 and <= 30), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content > 20 AND Content <= 30");
                Assert.Equal(entries.Count(i => i.Content is > 20 and <= 30), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= 20 AND Content < 30");
                Assert.Equal(entries.Count(i => i.Content is >= 20 and < 30), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content > 20 AND Content < 30");
                Assert.Equal(entries.Count(i => i.Content is > 20 and < 30), results.Count);
            }
        }
        
        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(new object[] {1000, 8})]
        public void AndInStatementWithLowercaseAnalyzer(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);
            var entries = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => ["road", "Lake", "mounTain"],
                        1 => ["roAd", "mountain"],
                        2 => ["sky", "space", "laKe"],
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };
                entries.Add(entry);
                entriesToIndex[i] = entry;
            }

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            var analyzer = Analyzer.Create<KeywordTokenizer, LowerCaseTransformer>(ctx);

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc, analyzer));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('lake', 'mountain') AND Content = 'sky'");
                Assert.Equal((setSize / 3), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'sky' AND Content IN ('lake', 'mountain')");
                Assert.Equal((setSize / 3), results.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(new object[] {1000, 8})]
        public void AndInStatementAndWhitespaceTokenizer(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);

            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] {"road Lake mounTain  "},
                        1 => new string[] {"roAd mountain"},
                        2 => new string[] {"sky space laKe"},
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };

                entriesToIndex[i] = entry;
            }

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            var analyzer = Analyzer.Create<WhitespaceTokenizer, LowerCaseTransformer>(ctx);
            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer);
            using var mapping = builder.Build();

            IndexEntries(ctx, entriesToIndex, mapping);

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('lake', 'mountain') AND Content = 'sky'");
                Assert.Equal((setSize / 3), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'sky' AND Content IN ('lake', 'mountain')");
                Assert.Equal((setSize / 3), results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void StartsWithSingle()
        {
            var entry = new IndexSingleEntry {Id = $"entry/1", Content = "tester"};
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            var analyzer = Analyzer.Create<WhitespaceTokenizer, LowerCaseTransformer>(ctx);
            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer);
            using var mapping = builder.Build();

            IndexEntries(ctx, new[] {entry}, mapping);
            using (var searcher = new IndexSearcher(Env, mapping))
            {
                var match = searcher.StartWithQuery("Content", "test");
                var ids = new long[16];
                var matchEq = searcher.TermQuery("Content", "tester");
                Assert.Equal(1, matchEq.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void NotInTest()
        {
            var listToIndex = Enumerable.Range(000000, 1000).Select(i => new IndexSingleEntry {Id = $"entry/{i}", Content = i.ToString("000000")}).ToList();
            var listForNotIn = listToIndex.Where(p => p.Content.EndsWith("1")).ToList();
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, listToIndex, CreateKnownFields(bsc));

            {
                var inList = string.Join("', '", listForNotIn.Select(l => l.Content));
                // RQL requires "true AND NOT" to express negation at the top level.
                // "true AND NOT Content IN (...)" is the valid form for NOT IN.
                var results = ExecuteRQLQuery($"FROM TestIndex WHERE true AND NOT Content IN ('{inList}')");
                Assert.Equal(1000 - listForNotIn.Count(), results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleAndNot()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "Testing"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "Running"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "Runner"};
            var list = new[] {entry1, entry2, entry3};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, list, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                // true AND NOT ... anchors the top-level NOT (NOT alone is parsed as a method call).
                var results = ExecuteRQLQuery("FROM TestIndex WHERE true AND NOT startsWith(Content, 'Run')");
                Assert.Equal(1, results.Count);
                var item = searcher.TermsReaderFor("Id").GetTermFor(results[0]);
                Assert.Equal("entry/1", item);
            }

            {
                // Empty result case: exclude all 3 known IDs → 0 results
                var notAllResults = ExecuteRQLQuery("FROM TestIndex WHERE Id != 'entry/1' AND Id != 'entry/2' AND Id != 'entry/3'");
                Assert.Equal(0, notAllResults.Count);
            }

            {
                // No entries start with 'J', so true AND NOT startsWith(Content, 'J') keeps all.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE true AND NOT startsWith(Content, 'J')");
                Assert.Equal(3, results.Count);
                var uniqueIds = new HashSet<long>(results);
                Assert.Equal(3, uniqueIds.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void NotEqualWithList()
        {
            var entries = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[7];
            for (int i = 0; i < 7; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 7) switch
                    {
                        0 => ["1"],
                        1 => ["7"],
                        2 => ["1", "2"],
                        3 => ["1", "2", "3"],
                        4 => ["1", "2", "3", "5"],
                        5 => ["2", "5"],
                        6 => ["2", "5", "7"],
                        _ => throw new ArgumentOutOfRangeException()
                    }
                };
                entries.Add(entry);
                entriesToIndex[i] = entry;
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            {
                // Test: (Content != '8') OR (Content != '9') OR (Content != '10') = all entries
                // (no entry has 8, 9, or 10, so each != matches all 7)
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '8' OR Content != '9' OR Content != '10'");
                Assert.Equal(7, results.Count);
            }

            {
                // Test: NOT (1 IN doc OR 2 IN doc OR ... OR 7 IN doc) means
                // NOT (1 IN doc) OR NOT (2 IN doc) ... — at least one value absent from the set.
                // No entry has ALL of {1,2,3,5,7}, so OR of NOT-in gives all 7 entries.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '1' OR Content != '2' OR Content != '3' OR Content != '5' OR Content != '7'");
                Assert.Equal(entries.Count, results.Count);
            }

            {
                // Test: All entries have an Id starting with 'entry/'
                var results = ExecuteRQLQuery("FROM TestIndex WHERE startsWith(Id, 'entry/')");
                Assert.Equal(7, results.Count);
            }

            {
                // Test: startsWith(Id, 'entry/') AND NOT Content IN ('8', '9', '10') = all 7
                // None of the entries have content values 8, 9, or 10.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE startsWith(Id, 'entry/') AND NOT Content IN ('8', '9', '10')");
                Assert.Equal(7, results.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(100, 16)]
        [InlineData(1000, 128)]
        [InlineData(10_000, 128)]
        [InlineData(10_000, 256)]
        [InlineData(10_000, 512)]
        [InlineData(10_000, 1028)]
        public void MultiTermMatchWithBinaryOperations(int setSize, int stackSize)
        {
            var words = new[]
            {
                "torun", "pomorze", "maciej", "aszyk", "corax", "matt", "gracjan", "tomasz", "marcin", "tomtom", "ravendb", "poland", "israel", "pattern", "seen",
                "macios", "tests", "are", "cool", "arent", "they", "this", "should", "work", "every", "time"
            };
            var random = new Random(1000);
            var entries = Enumerable.Range(0, setSize).Select(i => new IndexEntry() {Id = $"entry/{i}", Content = GetContent()}).ToList();

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries.ToArray(), CreateKnownFields(bsc));

            {
                //MultiTermMatch And TermMatch
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('maciej', 'poland') AND Content = 'this'");
                var resultByLinq = entries.Where(x => (x.Content.Contains("maciej") || x.Content.Contains("poland")) && x.Content.Contains("this")).ToList();
                Assert.Equal(results.Count, results.Distinct().Count());
                Assert.Equal(resultByLinq.Count, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE startsWith(Content, 'ma') OR Content = 'torun'");
                var linqResult = entries.Where(x => x.Content.Any(z => z.StartsWith("ma") || z.Contains("torun"))).ToList();
                Assert.Equal(linqResult.Count, results.Count);
            }

            string[] GetContent()
            {
                var amount = random.Next(0, 10);
                return Enumerable.Range(0, amount).Select(i => words[random.Next(0, words.Count())]).ToArray();
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void UnaryMatch()
        {
            var entries = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[7];
            for (int i = 0; i < 7; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 7) switch
                    {
                        0 => new string[] {"1"},
                        1 => new string[] {null, "7"},
                        2 => new string[] {"2", "1"},
                        3 => new string[] {null, "1", "2", "3"},
                        4 => new string[] {"1", "2", "3", "5", null},
                        5 => new string[] {"2", "5"},
                        6 => new string[] {"2", "5", "7"},
                        _ => throw new ArgumentOutOfRangeException()
                    }
                };
                entries.Add(entry);
                entriesToIndex[i] = entry;
            }

            IndexEntries(Allocator, entries.ToArray(), CreateKnownFields(Allocator));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '1'");
                Assert.Equal(3, results.Count);
            }
            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '2'");
                var expected = entries.Count(x => x.Content.Contains("2") == false);
                Assert.Equal(expected, results.Count);
            }
        }
        
        
        /// <summary>
        /// Executes an RQL query through QueryPlanBuilder and returns matching entry IDs.
        /// This properly uses the new query execution pipeline: RQL → AST → QueryPlan → IL compilation → execution.
        /// </summary>
        private List<long> ExecuteRQLQuery(string rqlQuery)
        {
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            // Parse RQL query string into AST via QueryMetadata
            var queryMetadata = new QueryMetadata(rqlQuery, null, 0);

            // Build and compile the query plan through QueryPlanBuilder
            var planParams = new QueryPlanBuilder.PlanParameters
            {
                IndexSearcher = searcher,
                Metadata = queryMetadata,
                QueryParameters = null,
                Allocator = Allocator,
                Token = CancellationToken.None
            };

            // BuildAndCompile: RQL → QueryPlan → IL compilation → CompiledQueryMatch
            // Pass null for QueryBuilderParameters — the fallback uses FieldMetadataBuilder
            // directly from IndexSearcher, which is sufficient for the simple term/range/bool
            // queries exercised by these unit tests (no vector/spatial/dynamic fields).
            var compiledMatch = QueryPlanBuilder.BuildAndCompile(
                planParams,
                null,
                long.MaxValue,
                out _,
                highlightingTerms: null,
                CancellationToken.None);

            // Execute the compiled match to collect entry IDs
            var results = new List<long>();
            Span<long> buffer = stackalloc long[256];
            int count;

            while ((count = compiledMatch.Fill(buffer)) > 0)
            {
                for (int i = 0; i < count; i++)
                    results.Add(buffer[i]);
            }

            return results;
        }

        private class IndexEntry
        {
            public long IndexEntryId;
            public string Id;
            public string[] Content;
        }

        private class IndexSingleEntry
        {
            public string Id;
            public string Content;
        }

        private readonly struct StringArrayIterator : IReadOnlySpanIndexer
        {
            private readonly string[] _values;

            private static string[] Empty = new string[0];

            public StringArrayIterator(string[] values)
            {
                _values = values ?? Empty;
            }

            public StringArrayIterator(IEnumerable<string> values)
            {
                _values = values?.ToArray() ?? Empty;
            }

            public int Length => _values.Length;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }

            public ReadOnlySpan<byte> this[int i] => _values[i] != null ? Encoding.UTF8.GetBytes(_values[i]) : null;
        }

  
        public const int IdIndex = 0,
            ContentIndex = 1;

        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, Analyzer analyzer = null)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer);
            return builder.Build();
        }

        private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexEntry> list, IndexFieldsMapping mapping)
        {
            using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

            foreach (var entry in list)
            {
                using var builder = indexWriter.Index(entry.Id);
                builder.Write(IdIndex, PrepareString(entry.Id));
                if (entry.Content != null)
                {
                    foreach (string s in entry.Content)
                    {
                        if (s == null)
                        {
                            builder.WriteNull(ContentIndex, null);
                        }
                        else
                        {
                            builder.Write(ContentIndex, Encoding.UTF8.GetBytes(s));
                        }
                    }
                }

                entry.IndexEntryId = (long)builder.EntryId;
                builder.EndWriting();
            }
            indexWriter.Commit();
            mapping.Dispose();
        }

        private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexSingleEntry> list, IndexFieldsMapping mapping)
        {
            using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

            foreach (var entry in list)
            {
                using var builder = indexWriter.Index(entry.Id);
                builder.Write(IdIndex, PrepareString(entry.Id));
                builder.Write(ContentIndex, PrepareString(entry.Content));
                builder.EndWriting();
            }

            indexWriter.Commit();
        }

        private void IndexEntriesDouble(IEnumerable<IndexSingleEntryDouble> list)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);

            {
                using var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All);

                foreach (var entry in list)
                {
                    using var entryWriter = indexWriter.Index(entry.Id);
                    entryWriter.Write(IdIndex, PrepareString(entry.Id));
                    entryWriter.Write(ContentIndex, PrepareString(entry.Content.ToString(CultureInfo.InvariantCulture)), Convert.ToInt64(entry.Content), entry.Content);
                    entryWriter.EndWriting();
                }

                indexWriter.Commit();
            }
        }

        Span<byte> PrepareString(string value)
        {
            if (value == null)
                return Span<byte>.Empty;
            return Encoding.UTF8.GetBytes(value);
        }

        
        private class IndexSingleEntryDouble
        {
            public string Id;
            public double Content;
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void RandomOrderOnBitmapMatchProducesActualRandomOrder()
        {
            // Regression: the bitmap path for ORDER BY random() was calling
            // SortResultsFromBitmap<EntryComparerByTerm>, which sorted by term name instead
            // of shuffling. ReservoirSampleFromBitmap must be called instead.
            //
            // We use BitmapMatch directly (rather than going through RQL / CompiledQueryMatch)
            // so the test is independent of the QueryILEmitter.

            const int N = 32;
            var entries = Enumerable.Range(1, N)
                .Select(i => new IndexSingleEntry { Id = $"entry/{i:D3}", Content = i.ToString() })
                .ToList();

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            // Collect all entry IDs from a plain TermQuery that matches all docs.
            // We use ExistsQuery to get all entry IDs, then build a BitmapMatch from them.
            var allEntryIds = new List<long>();
            {
                Span<long> buf = stackalloc long[256];
                var exists = searcher.ExistsQuery(searcher.FieldMetadataBuilder("Content", ContentIndex));
                int r;
                while ((r = exists.Fill(buf)) > 0)
                    for (int i = 0; i < r; i++)
                        allEntryIds.Add(buf[i]);
            }

            Assert.Equal(N, allEntryIds.Count);
            var allEntryIdsSorted = allEntryIds.OrderBy(x => x).ToList();

            static List<long> RunOrder(IndexSearcher searcher, List<long> allEntryIds, ByteStringContext allocator, int seed)
            {
                // Build a BitmapMatch from the known entry IDs — this implements IBitmapQueryMatch,
                // which triggers the bitmap-specific code path in SortingMatch.
                var bitmapMatch = new BitmapMatch(allocator);
                foreach (long id in allEntryIds)
                    bitmapMatch.BitmapState.Add(id);

                var orderMeta = new OrderMetadata(seed);
                var sortMatch = searcher.OrderBy(bitmapMatch, orderMeta, nullFirst: false);
                var results = new List<long>();
                Span<long> buf = stackalloc long[256];
                int r;
                while ((r = sortMatch.Fill(buf)) > 0)
                    for (int i = 0; i < r; i++)
                        results.Add(buf[i]);
                return results;
            }

            // Same seed → identical order both times.
            var run1 = RunOrder(searcher, allEntryIds, Allocator, seed: 42);
            var run2 = RunOrder(searcher, allEntryIds, Allocator, seed: 42);
            Assert.Equal(run1, run2);

            // Different seed → different order (with 32 entries this is virtually certain).
            var run3 = RunOrder(searcher, allEntryIds, Allocator, seed: 99);
            Assert.NotEqual(run1, run3);

            // Must be a permutation — nothing lost, nothing duplicated.
            Assert.Equal(allEntryIdsSorted, run1.OrderBy(x => x).ToList());

            // Must NOT be in ascending entry-ID order — that was the bug (term sort behaviour).
            Assert.NotEqual(allEntryIdsSorted, run1);
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void RandomOrderOnBitmapMatchWithTakeSelectsCorrectSubset()
        {
            // Verify LIMIT is respected: reservoir sampling must return exactly _take
            // distinct entries, all from the original set.
            const int N = 32;
            const int Take = 7;

            var entries = Enumerable.Range(1, N)
                .Select(i => new IndexSingleEntry { Id = $"entry/{i:D3}", Content = i.ToString() })
                .ToList();

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var allEntryIds = new HashSet<long>();
            {
                Span<long> buf = stackalloc long[256];
                var exists = searcher.ExistsQuery(searcher.FieldMetadataBuilder("Content", ContentIndex));
                int r;
                while ((r = exists.Fill(buf)) > 0)
                    for (int i = 0; i < r; i++)
                        allEntryIds.Add(buf[i]);
            }

            var bitmapMatch = new BitmapMatch(Allocator);
            foreach (long id in allEntryIds)
                bitmapMatch.BitmapState.Add(id);

            var orderMeta = new OrderMetadata(7);
            var sortMatch = searcher.OrderBy(bitmapMatch, orderMeta, nullFirst: false, take: Take);
            var results = new List<long>();
            Span<long> buf2 = stackalloc long[256];
            int read;
            while ((read = sortMatch.Fill(buf2)) > 0)
                for (int i = 0; i < read; i++)
                    results.Add(buf2[i]);

            Assert.Equal(Take, results.Count);
            Assert.All(results, id => Assert.Contains(id, allEntryIds));
            Assert.Equal(Take, results.Distinct().Count());
        }
    }
}
