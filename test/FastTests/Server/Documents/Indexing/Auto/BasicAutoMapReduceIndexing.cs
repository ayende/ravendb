﻿using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Auto
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class BasicAutoMapReduceIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task CanUseSimpleReduction()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {
                CreateUsers(db, 2, "Poland");
                
                mri.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery(), context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("Location", out location));
                    Assert.Equal("Poland", location);

                    var count = result["Count"] as LazyDoubleValue;

                    Assert.NotNull(count);
                    Assert.Equal(2.0, count);
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx2 TO Lx10]"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);

                    queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx10 TO NULL]"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(0, queryResult.Results.Count);
                }
            }
        }

        [Theory]
        [InlineData(100, new[] { "Poland", "Israel", "USA" })]
        [InlineData(50000, new[] { "Canada", "France" })] // reduce key tree with depth 3
        public async Task MultipleReduceKeys(int numberOfUsers, string[] locations)
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {
                Assert.True(db.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap >= numberOfUsers); // ensure all docs will be indexed in a single run

                db.Configuration.Indexing.DocumentProcessingTimeout = new TimeSetting(1, TimeUnit.Minutes);

                CreateUsers(db, numberOfUsers, locations);

                var batchStats = new IndexingRunStats();
                var scope = new IndexingStatsScope(batchStats);

                index.DoIndexingWork(scope, CancellationToken.None);

                Assert.Equal(numberOfUsers, batchStats.MapAttempts);
                Assert.Equal(numberOfUsers, batchStats.MapSuccesses);
                Assert.Equal(0, batchStats.MapErrors);
                Assert.Equal(numberOfUsers, batchStats.ReduceAttempts);
                Assert.Equal(numberOfUsers, batchStats.ReduceSuccesses);
                Assert.Equal(0, batchStats.ReduceErrors);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery
                    {
                        WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                    }, context, OperationCancelToken.None);

                    Assert.False(queryResult.IsStale);

                    var results = queryResult.Results;

                    Assert.Equal(locations.Length, results.Count);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                        double expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                        Assert.Equal(expected, ((LazyDoubleValue)results[i].Data["Count"]));
                    }
                }
            }
        }

        [Fact]
        public async Task CanDelete()
        {
            const int numberOfUsers = 10;

            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {

                CreateUsers(db, numberOfUsers, "Poland");
                
                // index 10 users
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers, (double)(LazyDoubleValue)results[0].Data["Count"]);
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        db.DocumentsStorage.Delete(context, "users/0", null);

                        tx.Commit();
                    }
                }

                // one document deleted
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers - 1, (double)(LazyDoubleValue)results[0].Data["Count"]);
                }

                CreateUsers(db, 1, "Poland");

                // document added again
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers, (double)(LazyDoubleValue)results[0].Data["Count"]);
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        for (int i = 0; i < numberOfUsers; i++)
                        {
                            db.DocumentsStorage.Delete(context, $"users/{i}", null);
                        }

                        tx.Commit();
                    }
                }

                // all documents removed
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(0, results.Count);
                }

                CreateUsers(db, numberOfUsers, "Poland");

                // documents added back
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers, (double)(LazyDoubleValue)results[0].Data["Count"]);
                }
            }
        }

        [Fact]
        public void DefinitionOfAutoMapReduceIndexIsPersisted()
        {
            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var count = new IndexField
                {
                    Name = "Count",
                    Highlighted = true,
                    Storage = FieldStorage.Yes,
                    SortOption = SortOptions.NumericDefault,
                    MapReduceOperation = FieldMapReduceOperation.Count
                };

                var location = new IndexField
                {
                    Name = "Location",
                    Highlighted = true,
                    Storage = FieldStorage.Yes,
                    SortOption = SortOptions.String,
                };

                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(new [] { "Users" }, new[] { count }, new[] { location })));

                var sum = new IndexField
                {
                    Name = "Sum",
                    Highlighted = false,
                    Storage = FieldStorage.Yes,
                    SortOption = SortOptions.NumericDefault,
                    MapReduceOperation = FieldMapReduceOperation.Sum
                };

                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(new[] { "Users" }, new[] { count, sum }, new[] { location })));

                var index2 = database.IndexStore.GetIndex(2);
                index2.SetLock(IndexLockMode.LockedError);
                index2.SetPriority(IndexingPriority.Disabled);
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x => x.IndexId)
                    .ToList();

                Assert.Equal(2, indexes.Count);

                Assert.Equal(1, indexes[0].IndexId);
                Assert.Equal(1, indexes[0].Definition.Collections.Length);
                Assert.Equal("Users", indexes[0].Definition.Collections[0]);
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Equal("Count", indexes[0].Definition.MapFields["Count"].Name);
                Assert.Equal(SortOptions.NumericDefault, indexes[0].Definition.MapFields["Count"].SortOption);
                Assert.True(indexes[0].Definition.MapFields["Count"].Highlighted);
                Assert.Equal(FieldMapReduceOperation.Count, indexes[0].Definition.MapFields["Count"].MapReduceOperation);

                var definition = indexes[0].Definition as AutoMapReduceIndexDefinition;

                Assert.NotNull(definition);

                Assert.Equal(1, definition.GroupByFields.Count);
                Assert.Equal("Location", definition.GroupByFields["Location"].Name);
                Assert.Equal(SortOptions.String, definition.GroupByFields["Location"].SortOption);

                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexingPriority.Normal, indexes[0].Priority);
                
                Assert.Equal(2, indexes[1].IndexId);
                Assert.Equal(1, indexes[1].Definition.Collections.Length);
                Assert.Equal("Users", indexes[1].Definition.Collections[0]);

                Assert.Equal(2, indexes[1].Definition.MapFields.Count);
                Assert.Equal("Count", indexes[1].Definition.MapFields["Count"].Name);
                Assert.Equal(FieldMapReduceOperation.Count, indexes[1].Definition.MapFields["Count"].MapReduceOperation);
                Assert.Equal(SortOptions.NumericDefault, indexes[1].Definition.MapFields["Count"].SortOption);
                Assert.Equal("Sum", indexes[1].Definition.MapFields["Sum"].Name);
                Assert.Equal(FieldMapReduceOperation.Sum, indexes[1].Definition.MapFields["Sum"].MapReduceOperation);
                Assert.Equal(SortOptions.NumericDefault, indexes[1].Definition.MapFields["Sum"].SortOption);

                definition = indexes[0].Definition as AutoMapReduceIndexDefinition;

                Assert.NotNull(definition);

                Assert.Equal(1, definition.GroupByFields.Count);
                Assert.Equal("Location", definition.GroupByFields["Location"].Name);
                Assert.Equal(SortOptions.String, definition.GroupByFields["Location"].SortOption);

                Assert.Equal(IndexLockMode.LockedError, indexes[1].Definition.LockMode);
                Assert.Equal(IndexingPriority.Disabled, indexes[1].Priority);
            }
        }

        [Fact]
        public async Task MultipleAggregationFunctionsCanBeUsed()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    MapReduceOperation = FieldMapReduceOperation.Count,
                    Storage = FieldStorage.Yes
                },
                new IndexField
                {
                    Name = "TotalCount",
                    MapReduceOperation = FieldMapReduceOperation.Count,
                    Storage = FieldStorage.Yes
                },
                new IndexField
                {
                    Name = "Age",
                    MapReduceOperation = FieldMapReduceOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            }), db))
            {
                CreateUsers(db, 2, "Poland");

                mri.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery(), context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("Location", out location));
                    Assert.Equal("Poland", location);

                    var count = result["Count"] as LazyDoubleValue;

                    Assert.NotNull(count);
                    Assert.Equal(2.0, count);

                    var totalCount = result["TotalCount"] as LazyDoubleValue;

                    Assert.NotNull(totalCount);
                    Assert.Equal(2.0, totalCount);

                    var age = result["Age"] as LazyDoubleValue;

                    Assert.NotNull(age);
                    Assert.Equal("41.0", age.Inner.ToString());
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx2 TO Lx10]"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);

                    queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx10 TO NULL]"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(0, queryResult.Results.Count);
                }
            }
        }

        [Fact]
        public async Task CanGroupByNestedFieldAndAggregateOnCollection()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(
                new [] { "Orders" }, 
                new []
                {
                    new IndexField
                    {
                        Name = "Lines,Quantity",
                        MapReduceOperation = FieldMapReduceOperation.Sum,
                        Storage = FieldStorage.Yes
                    },
                    new IndexField
                    {
                        Name = "Lines,Price",
                        MapReduceOperation = FieldMapReduceOperation.Sum,
                        Storage = FieldStorage.Yes
                    }
                },
                new []
                {
                    new IndexField
                    {
                        Name = "ShipTo.Country",
                        Storage = FieldStorage.Yes
                    }, 
                }), db))
            {
                CreateOrders(db, 5, new[] { "Poland", "Israel" });

                mri.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "ShipTo_Country:Poland"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("ShipTo_Country", out location));
                    Assert.Equal("Poland", location);

                    var price = result["Lines_Price"] as LazyDoubleValue;

                    Assert.NotNull(price);
                    Assert.Equal(63.6, price);

                    var quantity = result["Lines_Quantity"] as LazyDoubleValue;

                    Assert.NotNull(quantity);
                    Assert.Equal(9.0, quantity);
                }
            }
        }

        [Fact]
        public void CanStoreAndReadReduceStats()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {
                index._indexStorage.UpdateStats(SystemTime.UtcNow, new IndexingRunStats
                {
                    ReduceAttempts = 1000,
                    ReduceSuccesses = 900,
                    ReduceErrors = 100,
                });

                var stats = index.GetStats();

                Assert.Equal(1000, stats.ReduceAttempts);
                Assert.Equal(900, stats.ReduceSuccesses);
                Assert.Equal(100, stats.ReduceErrors);
            }
        }

        private static void CreateUsers(DocumentDatabase db, int numberOfUsers, params string[] locations)
        {
            using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    for (int i = 0; i < numberOfUsers; i++)
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Name"] = $"User-{i}",
                            ["Location"] = locations[i % locations.Length],
                            ["Age"] = 20 + i,
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Users"
                            }
                        }, $"users/{i}"))
                        {
                            db.DocumentsStorage.Put(context, $"users/{i}", null, doc);
                        }
                    }

                    tx.Commit();
                }
            }
        }

        [Fact]
        public async Task CanUpdateByChangingValue()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Age",
                    MapReduceOperation = FieldMapReduceOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
                    {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            }), db))
            {
                CreateUsers(db, 2, "Poland");

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(41.0, (LazyDoubleValue)results[0].Data["Age"]);
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Name"] = "modified",
                            ["Location"] = "Poland",
                            ["Age"] = 30,
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Users"
                            }
                        }, "users/0"))
                        {
                            db.DocumentsStorage.Put(context, "users/0", null, doc);
                        }

                        tx.Commit();
                    }
                }

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(51.0, (LazyDoubleValue)results[0].Data["Age"]);
                }
            }
        }

        [Fact]
        public async Task CanUpdateByChangingReduceKey()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Age",
                    MapReduceOperation = FieldMapReduceOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                    new IndexField
                    {
                        Name = "Location",
                        Storage = FieldStorage.Yes
                    },
            }), db))
            {
                CreateUsers(db, 2, "Poland");

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(41.0, (LazyDoubleValue)results[0].Data["Age"]);
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Name"] = "James",
                            ["Location"] = "Israel",
                            ["Age"] = 20,
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Users"
                            }
                        }, "users/0"))
                        {
                            db.DocumentsStorage.Put(context, "users/0", null, doc);
                        }

                        tx.Commit();
                    }
                }

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery() { SortedFields = new []{ new SortedField("Location") }}, context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(2, results.Count);

                    Assert.Equal("Israel", results[0].Data["Location"].ToString());
                    Assert.Equal(20.0, (LazyDoubleValue)results[0].Data["Age"]);

                    Assert.Equal("Poland", results[1].Data["Location"].ToString());
                    Assert.Equal(21.0, (LazyDoubleValue)results[1].Data["Age"]);
                }
            }
        }

        [Fact]
        public async Task GroupByMultipleFields()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(new[] { "Orders" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    MapReduceOperation = FieldMapReduceOperation.Count,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                    new IndexField
                    {
                        Name = "Employee",
                        Storage = FieldStorage.Yes
                    },
                    new IndexField
                    {
                        Name = "Company",
                        Storage = FieldStorage.Yes
                    },
            }), db))
            {
                CreateOrders(db, 10, employees: new [] { "employees/1", "employees/2" }, companies: new [] { "companies/1", "companies/2", "companies/3"});

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var results = (await index.Query(new IndexQuery(), context, OperationCancelToken.None)).Results;

                    Assert.Equal(6, results.Count);

                    for (int i = 0; i < 6; i++)
                    {
                        var employeeNumber = i % 2 + 1;
                        var companyNumber = i % 3 + 1;
                        results = (await index.Query(new IndexQuery
                        {
                            Query = $"Employee:employees/{employeeNumber} AND Company:companies/{companyNumber}"
                        }, context, OperationCancelToken.None)).Results;

                        Assert.Equal(1, results.Count);
                        
                        double expectedCount;

                        if ((employeeNumber == 1 && companyNumber == 2) || (employeeNumber == 2 && companyNumber == 3))
                            expectedCount = 1.0;
                        else
                            expectedCount = 2.0;

                        Assert.Equal(expectedCount, (LazyDoubleValue)results[0].Data["Count"]);
                    }
                } 
            }
        }

        private static void CreateOrders(DocumentDatabase db, int numberOfOrders, string[] countries = null, string[] employees = null, string[] companies = null)
        {
            using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    for (int i = 0; i < numberOfOrders; i++)
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Employee"] = employees?[i % employees.Length],
                            ["Company"] = companies?[i % companies.Length],
                            ["ShipTo"] = new DynamicJsonValue
                            {
                                ["Country"] = countries?[i % countries.Length],
                            },
                            ["Lines"] = new DynamicJsonArray
                            {
                                new DynamicJsonValue
                                {
                                    ["Price"] = 10.5,
                                    ["Quantity"] = 1
                                },
                                new DynamicJsonValue
                                {
                                    ["Price"] = 10.7,
                                    ["Quantity"] = 2
                                }
                            },
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Orders"
                            }
                        }, $"orders/{i}"))
                        {
                            db.DocumentsStorage.Put(context, $"orders/{i}", null, doc);
                        }
                    }

                    tx.Commit();
                }
            }
        }

        private static AutoMapReduceIndexDefinition GetUsersCountByLocationIndexDefinition()
        {
            return new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    MapReduceOperation = FieldMapReduceOperation.Count,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            });
        }
    }
}