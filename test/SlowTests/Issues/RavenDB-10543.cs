﻿using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10543 : RavenTestBase
    {
        public RavenDB_10543(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public class Item
            {
                public int Value { get; set; }
            }

            public List<int> Properties;
            public List<Item> Items;
        }

        [Fact]
        public void CanHandleAverage()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article
                    {
                        Properties = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 }, //4.5
                        Items = new List<Article.Item>()
                        {
                            new Article.Item() { Value = 1 },
                            new Article.Item() { Value = 2 },
                            new Article.Item() { Value = 3 },
                            new Article.Item() { Value = 4 },
                            new Article.Item() { Value = 5 },
                            new Article.Item() { Value = 6 },
                            new Article.Item() { Value = 7 },
                            new Article.Item() { Value = 8 }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                                let test = 1
                                select new
                                {
                                    Average1 = x.Properties.Average(),          //4.5
                                    Average2 = x.Items.Average(a => a.Value),   //4.5
                                    Average3 = x.Properties.Average(a => a)     //4.5
                                };

                    Assert.Equal($"declare function output(x) {{{Environment.NewLine}\tvar test = 1;{Environment.NewLine}\treturn {{ Average1 : x.Properties.reduce((a, b) => a + b, 0)/(x.Properties.length||1), Average2 : x.Items.map(a=>a.Value).reduce((a, b) => a + b, 0)/(x.Items.length||1), Average3 : x.Properties.map(a=>a).reduce((a, b) => a + b, 0)/(x.Properties.length||1) }};{Environment.NewLine}}}{Environment.NewLine}from 'Articles' as x select output(x)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(4.5, result[0].Average1);
                    Assert.Equal(4.5, result[0].Average2);
                    Assert.Equal(4.5, result[0].Average3);
                }
            }
        }
    }
}
