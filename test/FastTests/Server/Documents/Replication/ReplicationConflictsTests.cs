using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Replication;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationConflictsTests : ReplicationTestsBase
    {
        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void All_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 11 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 12 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void All_local_etags_lower_than_remote_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status_with_different_order()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_larger_size_than_local_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 40 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_with_different_dbId_set_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() };
            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 10 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_all_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_some_remote_etags_higher_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3000 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 100 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 200 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 300 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }


        [Fact]
        public async Task Conflict_same_time_with_master_slave()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                var conflicts = await WaitUntilHasConflict(store2, "foo/bar");
                Assert.Equal(2, conflicts["foo/bar"].Count);
            }
        }



        [Fact]
        public async Task Conflict_should_work_on_master_slave_slave()
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }
                using (var s3 = store3.OpenSession())
                {
                    s3.Store(new User(), "foo/bar");
                    s3.SaveChanges();
                }

                SetupReplication(store1, store3);
                SetupReplication(store2, store3);

                var conflicts = await WaitUntilHasConflict(store3, "foo/bar", 3);

                Assert.Equal(3, conflicts["foo/bar"].Count);
            }
        }	

        private async Task<Dictionary<string, List<ChangeVectorEntry[]>>> WaitUntilHasConflict(
                DocumentStore store,
                string docId,
                int count = 1,
                int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;
            Dictionary<string, List<ChangeVectorEntry[]>> conflicts;
            var sw = Stopwatch.StartNew();
            do
            {
                conflicts = await GetConflicts(store, docId);

                List<ChangeVectorEntry[]> list;
                if (conflicts.TryGetValue(docId, out list) == false)
                    list = new List<ChangeVectorEntry[]>();
                if (list.Count >= count)
                    break;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    Assert.False(true,
                        "Timed out while waiting for conflicts on " + docId + " we have " + list.Count + " conflicts");
                }

            } while (true);
            return conflicts;
        }
    }
}

