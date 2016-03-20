// -----------------------------------------------------------------------
//  <copyright file="BasicIndexUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using Raven.Server.Indexing.Corax.Queries;
using Raven.Server.Json.Parsing;
using Xunit;

namespace Tryouts.Corax.Tests
{
    public class BasicIndexUsage : CoraxTest
    {
        protected override IAnalyzer CreateAnalyzer()
        {
            return new NopAnalyzer();
        }

        [Fact]
        public void CanIndexAndQueryWithBoolean()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michael",
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Arek",
                }, "users/3");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new BooleanQuery(QueryOperator.Or,
                        new TermQuery("Name", "Arek"),
                        new TermQuery("Name", "Michael")
                        ),
                    Take = 2
                });
                Assert.Equal(new[] { "users/2", "users/3" }, ids);
            }
        }

        [Fact]
        public void CanIndexAndQuery()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michael",
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Arek",
                }, "users/3");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new TermQuery("Name", "Arek"),
                    Take = 2
                });
                Assert.Equal(new[] { "users/3" }, ids);
            }
        }

        [Fact]
        public void CanIndexAndQueryBigQuotedString()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Value"] = @"""An astronaut or cosmonaut is a person trained by a human spaceflight program to command, pilot, or serve as a crew member of a spacecraft. While generally reserved for professional space travelers, the terms are sometimes applied to anyone who travels into space, including scientists, politicians, journalists, and tourists.""",
                }, "statement/1");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Value"] = @"""By convention, an astronaut employed by the Russian Federal Space Agency (or its Soviet predecessor) is called a cosmonaut in English texts. The word is an anglicisation of the Russian word kosmonavt ( ), one who works in space outside the Earth's atmosphere, a space traveler, which derives from the Greek words kosmos (κόσμος), meaning universe, and nautes (ναύτης), meaning sailor. Other countries of the former Eastern Bloc use variations of the Russian word kosmonavt, such as the Polish kosmonauta.""",
                }, "statement/2");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new TermQuery("Value", "astronaut"),
                    Take = 2
                });
                Assert.Empty(ids);
            }
        }

    }
}