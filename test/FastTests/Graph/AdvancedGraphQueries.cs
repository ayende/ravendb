﻿using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Order = FastTests.Server.Basic.Entities.Order;
using Product = FastTests.Server.Basic.Entities.Product;
using OrderLine = FastTests.Server.Basic.Entities.OrderLine;

namespace FastTests.Graph
{
    public class AdvancedGraphQueries : RavenTestBase
    {
        [Fact]
        public void Can_query_with_edge_defined_in_embedded_object()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var moviesQueryResult = session.Advanced.RawQuery<Movie>(@"
                        match (u:Users(id() = 'users/2'))-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList();

                    Assert.Equal(2, moviesQueryResult.Count);
                    Assert.Contains(moviesQueryResult.Select(x => x.Name), name => name == "Firefly Serenity" || name == "Indiana Jones and the Temple Of Doom");
                }
            }
        }


        [Fact]
        public void Graph_query_can_handle_edges_defined_in_property_with_whitespaces()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                //create properties with whitespace in their name
                var operation = store
                    .Operations
                    .Send(new PatchByQueryOperation(@"
                                      from Orders as o
                                      update
                                      {
                                          o['Order Lines'] = o.Lines
                                      }"));

                operation.WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    WaitForUserToContinueTheTest(store);
                    //note the whitespace in edge property name in the graph query
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:'Order Lines'.Product]->(p:Products) select p.Name as Name").ToList();
                    var productNamesFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).ToArray();
                    Assert.Equal(4, productNamesFromMatch.Length); //sanity check

                    var query = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = query.Select(r => r["Lines"])
                        .SelectMany(x => x)
                        .Select(x => x.ToObject<OrderLine>().Product).ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);

                    //note : OrderByDescending is required because graph and document queries may give results in different order
                    Assert.Equal(productNamesFromDocumentQuery.OrderByDescending(x => x), productNamesFromMatch.OrderByDescending(x => x));
                }

                //
            }
        }

        [Fact]
        public void Graph_query_should_return_data_in_proper_form()
            //note: for more information see http://issues.hibernatingrhinos.com/issue/RavenDB-12088
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var moviesQueryResult = session.Advanced.RawQuery<Movie>(@"
                        match (u:Users(id() = 'users/2'))-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList();

                    Assert.Equal(2, moviesQueryResult.Count); //sanity check

                    //If the data retrieved has proper json format, Ids here won't be null as they will be populated
                    //by the same client-side code that handles document query results
                    Assert.False(moviesQueryResult.Any(x => x.Id == null));
                    Assert.Contains("movies/2", moviesQueryResult.Select(x => x.Id));
                    Assert.Contains("movies/3", moviesQueryResult.Select(x => x.Id));
                }
            }
        }



        [Fact]
        public void Graph_query_missing_FROM_vertex_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match [:HasRated(Score > 1).Movie]->(m:Movies) 
                        ").ToList());
                }
            }
        }

        [Fact(Skip = "Should not work until RavenDB-12075 is implemented")]
        public void Graph_query_missing_TO_vertex_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match (u1:Users)-[:HasRated(Score > 1).Movie]
                        ").ToList());
                }
            }
        }

        [Fact]
        public void Query_with_duplicate_implicit_aliases_in_select_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/1", "dogs/2"}
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/2"}
                    }; //dogs/3

                    session.Store(arava, "dogs/1");
                    session.Store(oscar, "dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name,b.Name") // <- this is wrong because we have two implicit "Name" aliases in select clause
                            .ToList());
                }
            }
        }

        [Fact]
        public void Query_with_duplicate_explicit_aliases_in_select_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/1", "dogs/2"}
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/2"}
                    }; //dogs/3

                    session.Store(arava, "dogs/1");
                    session.Store(oscar, "dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name AS Foo,b.Name AS Foo") // <- this is wrong because we have two explicit "Foo" aliases in select clause
                            .ToList());
                }
            }
        }

        [Fact]
        public void Query_with_multiple_hops_in_the_same_direction_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/1", "dogs/2"}
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/2"}
                    }; //dogs/3

                    session.Store(arava, "dogs/1");
                    session.Store(oscar, "dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    //note : such query implies implicit intersection between
                    // a -[likes]-> b and b -[likes]-> c, but it doesn't execute interesection-related code
                    var friends = session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name as A,b.Name as B,c.Name as C
                        ")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        A = x["A"]?.Value<string>(),
                        B = x["B"]?.Value<string>(),
                        C = x["C"]?.Value<string>()
                    }).ToArray();

                    Assert.Equal(2, resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.A == "Arava" && item.B == "Arava" && item.C == "Arava");
                    Assert.Contains(resultPairs, item => item.A == "Arava" && item.B == "Arava" && item.C == "Oscar");
                }
            }
        }

        [Fact]
        public void Query_with_multiple_hops_that_are_cycle_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/2"}
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar",
                        Likes = new[] {"dogs/3"}

                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/1"}
                    }; //dogs/3

                    session.Store(arava, "dogs/1");
                    session.Store(oscar, "dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    //note : such query implies implicit intersection between
                    // a -[likes]-> b and b -[likes]-> c, but it doesn't execute interesection-related code
                    var friends = session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name as A,b.Name as B,c.Name as C
                        ")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        A = x["A"]?.Value<string>(),
                        B = x["B"]?.Value<string>(),
                        C = x["C"]?.Value<string>()
                    }).ToArray();

                    Assert.Equal(3, resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.A == "Arava" && item.B == "Oscar" && item.C == "Pheobe");
                    Assert.Contains(resultPairs, item => item.A == "Oscar" && item.B == "Pheobe" && item.C == "Arava");
                    Assert.Contains(resultPairs, item => item.A == "Pheobe" && item.B == "Arava" && item.C == "Oscar");
                }
            }
        }

        [Fact]
        public void FindTwoFriendliesWhoPointToTheSameVertex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava"
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe"
                    }; //dogs/3

                    session.Store(arava);
                    session.Store(oscar);
                    session.Store(pheobe);

                    //dogs/1 => dogs/3
                    arava.Likes = new[] {pheobe.Id};

                    //dogs/2 => dogs/3
                    oscar.Likes = new[] {pheobe.Id};

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs)")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        From = x["fst"]["Name"].Value<string>(),
                        To = x["snd"]["Name"].Value<string>()
                    }).ToArray();

                    Assert.Equal(2, resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.From == "Arava" && item.To == "Pheobe");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Pheobe");
                }
            }
        }

        [Fact]
        public void FindFriendlies()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);

                using (var session = store.OpenSession())
                {
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs)")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        From = x["fst"]["Name"].Value<string>(),
                        To = x["snd"]["Name"].Value<string>()
                    }).ToArray();

                    //arava -> oscar
                    //oscar -> oscar, phoebe
                    //phoebe -> oscar
                    Assert.Equal(4, resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.From == "Arava" && item.To == "Oscar");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Oscar");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Pheobe");
                    Assert.Contains(resultPairs, item => item.From == "Pheobe" && item.To == "Oscar");
                }
            }
        }

        [Fact]
        public void Only_undefined_alias_in_SELECT_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);

                using (var session = store.OpenSession())
                {
                    //should throw because "foobar" is not defined in the query
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs) select foobar").ToArray());
                }
            }
        }

        [Fact]
        public void Proper_and_undefined_alias_in_SELECT_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);

                using (var session = store.OpenSession())
                {
                    //should throw because "foobar" is not defined in the query
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs) select fst,foobar,snd").ToArray());
                }
            }
        }

        [Fact]
        public void FindFriendlies_with_javascript_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);
                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs) select { a : fst, b: snd }")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        From = x["a"]["Name"].Value<string>(),
                        To = x["b"]["Name"].Value<string>()
                    }).ToArray();

                    //arava -> oscar
                    //oscar -> oscar, phoebe
                    //phoebe -> oscar
                    Assert.Equal(4, resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.From == "Arava" && item.To == "Oscar");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Oscar");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Pheobe");
                    Assert.Contains(resultPairs, item => item.From == "Pheobe" && item.To == "Oscar");
                }
            }
        }

        [Fact]
        public void Match_without_any_parameters_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var one = store.OpenSession())
                using (var two = store.OpenSession())
                {
                    var orderFromMatch = one.Advanced.RawQuery<Order>(@"match (o:Orders (id() = 'orders/825-A'))").First();

                    var orderFromLoad = two.Load<Order>("orders/825-A");

                    //compare some meaningful properties, just to be sure
                    Assert.Equal(orderFromLoad.Id, orderFromMatch.Id);
                    Assert.Equal(orderFromLoad.Company, orderFromMatch.Company);
                    Assert.Equal(orderFromLoad.Employee, orderFromMatch.Employee);
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_with_array_brackets_syntax_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines[].Product]->(p:Products) select p.Name as Name").ToList();
                    var productNamesFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).ToArray();
                    Assert.Equal(4, productNamesFromMatch.Length); //sanity check

                    var query = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = query.Select(r => r["Lines"])
                        .SelectMany(x => x)
                        .Select(x => x.ToObject<OrderLine>().Product).ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);

                    //note : OrderByDescending is required because graph and document queries may give results in different order
                    Assert.Equal(productNamesFromDocumentQuery.OrderByDescending(x => x), productNamesFromMatch.OrderByDescending(x => x));
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_with_array_brackets_syntax_and_edge_filter_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines(ProductName = 'Chang')[].Product]->(p:Products) select p.Name as Name")
                        .ToList();
                    var productNameFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).First();

                    var query = session.Advanced.RawQuery<OrderLine>(@"
                        declare function FilterOnProductName(lines,productName) {
                            for(var i = 0; i < lines.length; i++){
                                if(lines[i].ProductName == productName){
                                    return lines[i]
                                }
                            }
                            return null;
                        }

                        from Orders as o 
                        where id() = 'orders/825-A'
                        select FilterOnProductName(Lines,'Chang')
                    ").ToArray();
                    var productsIdFromDocumentQuery = query[0].Product;
                    var productNameFromDocumentQuery = session.Load<Product>(productsIdFromDocumentQuery).Name;

                    Assert.Equal(productNameFromDocumentQuery, productNameFromMatch);
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_without_array_brackets_syntax_and_edge_filter_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines(ProductName = 'Chang').Product]->(p:Products) select p.Name as Name")
                        .ToList();
                    var productNameFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).First();

                    var query = session.Advanced.RawQuery<OrderLine>(@"
                        declare function FilterOnProductName(lines,productName) {
                            for(var i = 0; i < lines.length; i++){
                                if(lines[i].ProductName == productName){
                                    return lines[i]
                                }
                            }
                            return null;
                        }

                        from Orders as o 
                        where id() = 'orders/825-A'
                        select FilterOnProductName(Lines,'Chang')
                    ").ToArray();
                    var productsIdFromDocumentQuery = query[0].Product;
                    var productNameFromDocumentQuery = session.Load<Product>(productsIdFromDocumentQuery).Name;

                    Assert.Equal(productNameFromDocumentQuery, productNameFromMatch);
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_without_array_brackets_syntax_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines.Product]->(p:Products) select p.Name as Name").ToList();
                    var productNamesFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).ToArray();
                    Assert.Equal(4, productNamesFromMatch.Length); //sanity check

                    var query = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = query.Select(r => r["Lines"])
                        .SelectMany(x => x)
                        .Select(x => x.ToObject<OrderLine>().Product).ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);

                    //note : OrderByDescending is required because graph and document queries may give results in different order
                    Assert.Equal(productNamesFromDocumentQuery.OrderByDescending(x => x), productNamesFromMatch.OrderByDescending(x => x));
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_and_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var matchQueryResultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[l:Lines.Product]->(p:Products) 
                                                    select o,l,p").ToList();
                    Assert.Equal(4, matchQueryResultsAsJson.Count); //sanity check                    

                    var orderFromMatchQuery = matchQueryResultsAsJson.First()["o"].ToObject<Order>();
                    var productNamesFromMatchQuery = matchQueryResultsAsJson.Select(x => x["p"].ToObject<Product>().Name).ToArray();
                    var orderLinesFromMatchQuery = matchQueryResultsAsJson.Select(x => x["l"]).Select(t => t.ToObject<OrderLine>()).ToArray();

                    var orderFromDocumentQuery = session.Load<Order>("orders/825-A");
                    var linesQuery = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromOrderLinesDocumentQuery = linesQuery.Select(r => r["Lines"]).First()
                        .Select(x => x.ToObject<OrderLine>())
                        .Select(x => x.Product)
                        .ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromOrderLinesDocumentQuery).Select(x => x.Value.Name);

                    //compare orders
                    Assert.Equal(orderFromDocumentQuery.Lines.Count, orderFromMatchQuery.Lines.Count);
                    Assert.Equal(orderFromDocumentQuery.Employee, orderFromMatchQuery.Employee);
                    Assert.Equal(orderFromDocumentQuery.Company, orderFromMatchQuery.Company);

                    //compare product names
                    Assert.Equal(productNamesFromDocumentQuery.OrderBy(x => x), productNamesFromMatchQuery.OrderBy(x => x));

                    //compare order lines
                    Assert.Equal(orderLinesFromMatchQuery.Select(x => x.Product).OrderBy(x => x), productsIdsFromOrderLinesDocumentQuery.OrderBy(x => x));
                }
            }
        }

        [Fact]
        public void Multi_hop_query_with_unlimited_hops_and_no_matching_paths_and_single_destination_should_return_empty_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //intentionally, no edges between vertices
                    session.Store(new Dog
                    {
                        Name = "Arava"
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<Dog>(@"
                       match (d1:Dogs(id() = 'dogs/1-A'))-[l:Likes *]->(d2:Dogs(id() = 'dogs/2-A'))").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Multi_hop_query_with_unlimited_hops_and_no_matching_paths_and_multiple_possible_destination_should_return_empty_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //intentionally, no edges between vertices
                    session.Store(new Dog
                    {
                        Name = "Arava"
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar"
                    });
                    session.Store(new Dog
                    {
                        Name = "Pheobe"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<Dog>(@"
                       match (d1:Dogs)-[l:Likes *]->(d2:Dogs)").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Make_sure_cycles_are_handled_in_multi_hop_queries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/2-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Likes = new[] {"dogs/3-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/1-A"}
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (d1:Dogs)-[l:Likes *]->(d2:Dogs) 
                       select d1.Name as d1,l.Name as l, d2.Name as d2").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var interpretedResults = results.Select(x => new
                    {
                        D1 = x["d1"].Value<string>(),
                        L = x["l"].First.Value<string>(), //we have only one item in pathes in this use-case
                        D2 = x["d2"].Value<string>()
                    }).ToArray();

                    Assert.Contains(interpretedResults, item => item.D1 == "Arava" && item.L == "Oscar" && item.D2 == "Arava");
                    Assert.Contains(interpretedResults, item => item.D1 == "Oscar" && item.L == "Pheobe" && item.D2 == "Oscar");
                    Assert.Contains(interpretedResults, item => item.D1 == "Pheobe" && item.L == "Arava" && item.D2 == "Pheobe");
                }
            }
        }

        [Fact(Skip = "Should work when RavenDB-12187 is fixed")]
        public void Make_sure_cycles_and_where_expression_are_handled_in_multi_hop_queries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/2-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Likes = new[] {"dogs/3-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/1-A"}
                    });
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (d1:Dogs)-[l:Likes *]->(d2:Dogs) 
                       where d1 <> d2
                       select d1.Name as d1,l.Name as l, d2.Name as d2").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var interpretedResults = results.Select(x => new
                    {
                        D1 = x["d1"].Value<string>(),
                        L = x["l"].First.Value<string>(), //we have only one item in pathes in this use-case
                        D2 = x["d2"].Value<string>()
                    }).ToArray();

                    Assert.Contains(interpretedResults, item => item.D1 == "Arava" && item.L == "Oscar" && item.D2 == "Pheobe");
                    Assert.Contains(interpretedResults, item => item.D1 == "Oscar" && item.L == "Pheobe" && item.D2 == "Arava");
                    Assert.Contains(interpretedResults, item => item.D1 == "Pheobe" && item.L == "Arava" && item.D2 == "Oscar");
                }
            }
        }

        public class Person
        {
            public string Name { get; set; }
            public string Ancestor { get; set; }
        }

        [Fact]
        public void Multi_hop_without_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "John",
                        Ancestor = "people/2-a"
                    });
                    session.Store(new Person
                    {
                        Name = "Jill",
                        Ancestor = "people/3-a"
                    });
                    session.Store(new Person
                    {
                        Name = "Joe"                        
                    });

                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>("match (s:People)-[:Ancestor *]->(a:People)").ToArray();
                    Assert.NotEmpty(results);

                    var stronglyTypedResults = results.Select(x => new
                    {
                        S = x["s"].ToObject<Person>(),
                        A = x["a"].ToObject<Person>()
                    }).ToArray();

                    //we have only two distinct paths that have ancestors (Joe doesn't have ancestors)
                    Assert.Equal(2,stronglyTypedResults.Length); 
                    Assert.Contains(stronglyTypedResults, item => item.S.Name == "John" && item.A.Name == "Joe");
                    Assert.Contains(stronglyTypedResults, item => item.S.Name == "Jill" && item.A.Name == "Joe");
                }
            }
        }

        [Fact]
        public void Make_sure_cycles_are_handled_in_multi_hop_queries_with_multiple_multihop_edge_clauses()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/2-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Likes = new[] {"dogs/3-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/1-A"}
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (d1:Dogs)-[l:Likes *]->(d2:Dogs)-[l2:Likes *]->(d3:Dogs)
                       select d1.Name as d1,l.Name as l, d2.Name as d2, l2.Name as l2, d3.Name as d3").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var interpretedResults = results.Select(x => new
                    {
                        D1 = x["d1"].Value<string>(),
                        L = x["l"].First.Value<string>(), //we have only one item in pathes in this use-case
                        D2 = x["d2"].Value<string>(),
                        L2 = x["l2"].First.Value<string>(),
                        D3 = x["d3"].Value<string>()
                    }).ToArray();

                    Assert.Contains(interpretedResults, item => item.D1 == "Arava" && item.L == "Oscar" && item.D2 == "Arava" && item.L2 == "Oscar" && item.D3 == "Arava");
                    Assert.Contains(interpretedResults, item => item.D1 == "Oscar" && item.L == "Pheobe" && item.D2 == "Oscar" && item.L2 == "Pheobe" && item.D3 == "Oscar");
                    Assert.Contains(interpretedResults, item => item.D1 == "Pheobe" && item.L == "Arava" && item.D2 == "Pheobe"  && item.L2 == "Arava" && item.D3 == "Pheobe");
                }
            }
        }
    }
}
