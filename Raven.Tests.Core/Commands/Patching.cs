using System;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Commands
{
    public class Patching : RavenCoreTestBase
    {
        [Fact]
        public async Task CanSkipPatchIfEtagMismatch()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync(
                    "companies/1",
                    null,
                    RavenJObject.FromObject(new Company
                    {
                        Name = "testname",
                        Phone = 1,
                        Contacts = new List<Contact> { new Contact { }, new Contact { } },
                        Address1 = "To be removed.",
                        Address2 = "Address2"
                    }),
                    new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                var result = await store.AsyncDatabaseCommands.BatchAsync(
                    new ICommandData[]
                    {
                        new PatchCommandData
                        {
                            Key = "companies/1",
                            Patches = new PatchRequest[]{ new PatchRequest
                                {
                                    Type = PatchCommandType.Add,
                                    Name = "NewArray",
                                    Value = "NewValue"
                                }, },
                            Etag = Etag.InvalidEtag,
                            SkipPatchIfEtagMismatch = true
                        }
                    });


                Assert.Equal(PatchResult.Skipped, result[0].PatchResult);
            }
        }

        [Fact]
        public async Task CanDoSimplePatching()
        {
            using (var store = GetDocumentStore())
            {
                var putResult = await store.AsyncDatabaseCommands.PutAsync(
                    "companies/1",
                    null,
                    RavenJObject.FromObject(new Company
                    {
                        Name = "testname",
                        Phone = 1,
                        Contacts = new List<Contact> { new Contact { }, new Contact { } },
                        Address1 = "To be removed.",
                        Address2 = "Address2"
                    }),
                    new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.PatchAsync(
                    "companies/1",
                    new[]
                        {
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Add,
                                    Name = "NewArray",
                                    Value = "NewValue"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Copy,
                                    Name = "Name",
                                    Value = "CopiedName"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Inc,
                                    Name = "Phone",
                                    Value = -1
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Insert,
                                    Name = "Contacts",
                                    Position = 1,
                                    Value = RavenJObject.FromObject( new Contact { FirstName = "TestFirstName" } )
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Modify,
                                    Name = "Contacts",
                                    Position = 0,
                                    Nested = new[]
                                    {
                                        new PatchRequest
                                        {
                                            Type = PatchCommandType.Set,
                                            Name = "FirstName",
                                            Value = "SomeFirstName"
                                        }
                                    }
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Rename,
                                    Name = "Address2",
                                    Value = "Renamed"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Unset,
                                    Name = "Address1"
                                }
                        },
                    null);

                var item1 = await store.AsyncDatabaseCommands.GetAsync("companies/1");
                Assert.NotNull(item1);
                Assert.Equal("NewValue", item1.DataAsJson.Value<RavenJArray>("NewArray")[0]);
                Assert.Equal("testname", item1.DataAsJson.Value<string>("CopiedName"));
                Assert.Equal(0, item1.DataAsJson.Value<int>("Phone"));
                Assert.Equal("TestFirstName", item1.DataAsJson.Value<RavenJArray>("Contacts")[1].Value<string>("FirstName"));
                Assert.Equal("SomeFirstName", item1.DataAsJson.Value<RavenJArray>("Contacts")[0].Value<string>("FirstName"));
                Assert.Null(item1.DataAsJson.Value<string>("Address1"));
                Assert.Null(item1.DataAsJson.Value<string>("Address2"));
                Assert.Equal("Address2", item1.DataAsJson.Value<string>("Renamed"));
            }
        }

        [Fact]
        public void CanDoScriptedPatching()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    "posts/1",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    }),
                    new RavenJObject());

                var comment = new Post
                {
                    Title = "comment 1"
                };

                store.DatabaseCommands.Patch(
                    "posts/1",
                    new ScriptedPatchRequest()
                    {
                        Script = @"this.Comments.push(comment1)",
                        Values = { { "comment1", comment } }
                    });
                var result = store.DatabaseCommands.Get("posts/1");
                var comments = result.DataAsJson.Value<RavenJArray>("Comments");
                Assert.Equal(1, comments.Length);
                Assert.Equal("comment 1", comments[0].Value<string>("Title"));


                store.DatabaseCommands.Put(
                    "posts/2",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 2",
                        AttachmentIds = new string[] { "id1", "id2" }
                    }),
                    new RavenJObject());

                store.DatabaseCommands.Patch(
                    "posts/2",
                    new ScriptedPatchRequest()
                    {
                        Script = @"this.AttachmentIds.Remove(tagToRemove)",
                        Values = { { "tagToRemove", "id2" } }
                    });
                result = store.DatabaseCommands.Get("posts/2");
                Assert.Equal(1, result.DataAsJson.Value<RavenJArray>("AttachmentIds").Length);
                Assert.Equal("id1", result.DataAsJson.Value<RavenJArray>("AttachmentIds")[0]);


                store.DatabaseCommands.Patch(
                    "posts/1",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            this.Comments.RemoveWhere(function(comment) {
                                return comment.Title === 'comment 1';
                            });",
                    });
                result = store.DatabaseCommands.Get("posts/1");
                comments = result.DataAsJson.Value<RavenJArray>("Comments");
                Assert.Equal(0, comments.Length);


                var comment1 = new Post
                {
                    Title = "Comment 1",
                    Desc = "Some post without searched phrase inside."
                };
                var comment2 = new Post
                {
                    Title = "Comment 2",
                    Desc = "Some post with Raven phrase inside."
                };

                store.DatabaseCommands.Put(
                    "posts/3",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 3",
                        Comments = new Post[] { comment1, comment2 }
                    }),
                    new RavenJObject());
                store.DatabaseCommands.Patch(
                    "posts/3",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            this.Comments.Map(function(comment) {  
                                if(comment.Desc.indexOf(""Raven"") != -1)
                                {
                                    comment.Title = ""[Raven] "" + comment.Title;
                                }
                                return comment;
                            });
                        "
                    });
                result = store.DatabaseCommands.Get("posts/3");
                comments = result.DataAsJson.Value<RavenJArray>("Comments");
                Assert.Equal(2, comments.Length);
                Assert.Equal("Comment 1", comments[0].Value<string>("Title"));
                Assert.Equal("[Raven] Comment 2", comments[1].Value<string>("Title"));


                store.DatabaseCommands.Put(
                    "posts/4",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 4",
                        AttachmentIds = new string[] { "posts/5" }
                    }),
                    new RavenJObject());
                store.DatabaseCommands.Put(
                    "posts/5",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 5"
                    }),
                    new RavenJObject());
                store.DatabaseCommands.Patch(
                    "posts/4",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            var loaded = LoadDocument(this.AttachmentIds[0]);
                            this.Title = loaded.Title;
                        "
                    });
                result = store.DatabaseCommands.Get("posts/4");
                Assert.Equal("Post 5", result.DataAsJson.Value<string>("Title"));


                var output = store.DatabaseCommands.Patch(
                    "posts/4",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            var loaded = LoadDocument(this.AttachmentIds[0]);
                            this.Title = loaded.Title;
                            output(this.Title); 
                        "
                    });
                var debugInfo = output.Value<RavenJArray>("Debug");
                Assert.Equal("Post 5", debugInfo[0]);

                store.DatabaseCommands.Patch(
                    "posts/4",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            PutDocument('posts/4',
                                { 'Title' : 'new title' }
                            );"
                    });
                var post = store.DatabaseCommands.Get("posts/4");
                Assert.NotNull(post);
                Assert.Equal("new title", post.DataAsJson.Value<string>("Title"));
            }
        }

        [Fact]
        public void CanGenerteDynamicIdsOnPutDocument()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    "posts/1",
                    null,
                    RavenJObject.FromObject(new Post
                    {
                        Title = "Post 1",
                        Comments = new Post[] { }
                    }),
                    new RavenJObject());
                var output = store.DatabaseCommands.Patch(
                    "posts/1",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            var postId = PutDocument('posts/',
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"


                    });
                using (var session = store.OpenSession())
                {
                    var debugInfo = output.Value<RavenJArray>("Debug");
                    var postId = debugInfo[0].ToString();
                    var post = session.Load<Post>("posts/1");
                    Assert.Equal(postId, post.Title);
                }
                output = store.DatabaseCommands.Patch(
                    "posts/1",
                    new ScriptedPatchRequest()
                    {
                        Script = @"
                            var postId = PutDocument(null,
                                { 'Title' : 'unknown post id' }
                            );
                            this.Title = postId;
                            output(postId);"
                    });
                using (var session = store.OpenSession())
                {
                    var debugInfo = output.Value<RavenJArray>("Debug");
                    var postId = debugInfo[0].ToString();
                    var post = session.Load<Post>("posts/1");
                    Assert.Equal(postId, post.Title);
                    Guid id;
                    Assert.True(Guid.TryParse(postId, out id));
                }
            }
        }
    }
}
