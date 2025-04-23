using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.GenAi;

public class GenAiTestScript(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanTestGenAiScript(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testAiGenScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = new()
                    {
                        Name = "Check blog comments spam",
                        Connection = new AiConnectionString
                        {
                            Name = "ollama-local-deepseek-r1",
                            Identifier = "ollama-local-deepseek-r1",
                            OllamaSettings = new OllamaSettings
                            {
                                Uri = "http://127.0.0.1:11434/",
                                Model = "llama3.2:latest"
                            }
                        },
                        Collection = "Posts",
                        Prompt = "Check if the following blog post comment is spam or not",
                        SampleObject = JsonConvert.SerializeObject(new
                        {
                            Blocked = true,
                            Reason = "Concise reason for why this comment was marked as spam or harmful"
                        }),
                        Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
else 
{
    this.Comments[idx].AiHash = $aiHash; // remember this decision
}",
                        GenAiTransformation = new GenAiTransformation
                        {
                            Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                        }
                    }
                };

                var result = GenAiTask.TestScript(testAiGenScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(result);
                Assert.Equal(3, result.Results.Count);

                Assert.NotNull(result.InputDocument);
                Assert.True(result.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                var spamComments = 0;
                foreach (var item in result.Results)
                {
                    Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(item.ModelOutput.Output.TryGet("Reason", out string r));
                    Assert.NotNull(r);

                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }

                Assert.NotNull(result.OutputDocument);
                Assert.True(result.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

                var expected = 3 - spamComments;
                Assert.Equal(expected, comments.Length);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanTestGenAiScript_ViaEndpoint(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
            {
                Name = "ollama-local-deepseek-r1",
                Identifier = "ollama-local-deepseek-r1",
                OllamaSettings = new OllamaSettings
                {
                    Uri = "http://127.0.0.1:11434/",
                    Model = "llama3.2:latest"
                }
            }));

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testAiGenScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = new()
                    {
                        Name = "Check blog comments spam",
                        ConnectionStringName = "ollama-local-deepseek-r1",
                        Collection = "Posts",
                        Prompt = "Check if the following blog post comment is spam or not",
                        SampleObject = JsonConvert.SerializeObject(new
                        {
                            Blocked = true,
                            Reason = "Concise reason for why this comment was marked as spam or harmful"
                        }),
                        Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
else 
{
    this.Comments[idx].AiHash = $aiHash; // remember this decision
}",
                        GenAiTransformation = new GenAiTransformation
                        {
                            Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                        }
                    }
                };

                var bjro = store.Conventions.Serialization.DefaultConverter.ToBlittable(testAiGenScript, context);
                var cmd = new GenAiTestCmd(DocumentConventions.DefaultForServer, bjro);
                using var requestExecutor = store.GetRequestExecutor();
                await requestExecutor.ExecuteAsync(cmd, context);

                var result = cmd.Result;
                Assert.NotNull(result);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.Results), out BlittableJsonReaderArray resultItems));
                Assert.Equal(3, resultItems.Length);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.InputDocument), out BlittableJsonReaderObject input));
                Assert.NotNull(input);

                Assert.True(input.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                var spamComments = 0;
                foreach (var item in resultItems)
                {
                    var asBlittable = (BlittableJsonReaderObject)item;
                    Assert.NotNull(asBlittable);

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.ModelOutput), out BlittableJsonReaderObject modelOutput));
                    Assert.True(modelOutput.TryGet(nameof(GenAiResultItem.ModelOutput.Output), out BlittableJsonReaderObject output));

                    Assert.True(output.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(output.TryGet("Reason", out string r));
                    Assert.NotNull(r);

                    Assert.True(modelOutput.TryGet(nameof(GenAiResultItem.ModelOutput.Usage), out BlittableJsonReaderObject usage));
                    Assert.NotNull(usage);

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.ContextOutput), out BlittableJsonReaderObject contextOutput));
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.AiHash), out string hash));
                    Assert.NotNull(hash);
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.IsCached), out bool cached));
                    Assert.False(cached);

                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.Context), out BlittableJsonReaderObject ctxDoc));
                    Assert.True(ctxDoc.TryGet("Text", out string _));
                    Assert.True(ctxDoc.TryGet("Author", out string _));
                    Assert.True(ctxDoc.TryGet("Id", out string _));
                }

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.OutputDocument), out BlittableJsonReaderObject outputDoc));
                Assert.NotNull(outputDoc);
                Assert.True(outputDoc.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

                var expected = 3 - spamComments;
                Assert.Equal(expected, comments.Length);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TestGenAiScript_ShouldNotSendCachedItems(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave"),
                        new GenAiBasics.Comment("Hey Dave, yes — IO_Uring has come a long way since then. We've been using it in production for about 6 months now with good results, especially on high-throughput workloads.", "ayende")

                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testGenAiScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = new()
                    {
                        Name = "Check blog comments spam",
                        Connection = new AiConnectionString
                        {
                            Name = "ollama-local-deepseek-r1",
                            Identifier = "ollama-local-deepseek-r1",
                            OllamaSettings = new OllamaSettings
                            {
                                Uri = "http://127.0.0.1:11434/",
                                Model = "llama3.2:latest"
                            }
                        },
                        Collection = "Posts",
                        Prompt = "Check if the following blog post comment is spam or not",
                        SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" }),
                        Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments[idx].Spam = true;
}
this.Comments[idx].AiHash = $aiHash;
",
                        GenAiTransformation = new GenAiTransformation
                        {
                            Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                        }
                    }
                };

                var result = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(result);
                Assert.Equal(4, result.Results.Count);

                foreach (var item in result.Results)
                {
                    Assert.NotNull(item.ModelOutput?.Output);
                    Assert.NotNull(item.ContextOutput?.Context);
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);
                }

                // save the output doc as a new document
                var newId = id + "/new";
                using (var requestExecutor = store.GetRequestExecutor())
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    await requestExecutor.ExecuteAsync(new PutDocumentCommand(store.Conventions, newId, changeVector: null, result.OutputDocument), ctx);
                }

                // re-run the test script on the output document
                testGenAiScript.DocumentId = newId;
                result = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(result);
                Assert.Equal(4, result.Results.Count);

                // should not send anything to model, all comments are cached 
                foreach (var item in result.Results)
                {
                    Assert.Null(item.ModelOutput);
                    Assert.True(item.ContextOutput.IsCached);
                }
                Assert.Null(result.OutputDocument);

            }

        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TestGenAiScript_ShouldNotSendCachedItems2(Options options)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            var post = new GenAiBasics.Post(
                [
                    new("This is a legit comment", "user1"),
                    new("You won a FREE iPhone, click now!", "spammer")
                ],
                "Title", "Body");
            await session.StoreAsync(post, id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);

        using var contextPool = database.DocumentsStorage.ContextPool;
        database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        var testGenAiScript = new TestGenAiScript
        {
            DocumentId = id,
            Configuration = new()
            {
                Name = "Check blog comments spam",
                Connection = new AiConnectionString
                {
                    Name = "ollama-local-deepseek-r1",
                    Identifier = "ollama-local-deepseek-r1",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://127.0.0.1:11434/",
                        Model = "llama3.2:latest"
                    }
                },
                Collection = "Posts",
                Prompt = "Check if the following blog post comment is spam or not",
                SampleObject = JsonConvert.SerializeObject(
                    new
                    {
                        Blocked = true,
                        Reason = "Concise reason for why this comment was marked as spam or harmful"
                    }),
                Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments[idx].Spam = true;
}
this.Comments[idx].AiHash = $aiHash;
",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                }
            },
            ApplyUpdateScript = false
        };

        var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.NotNull(firstRun);
        Assert.Equal(2, firstRun.Results.Count);

        Assert.NotNull(firstRun.InputDocument);
        Assert.True(firstRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
        Assert.Equal(2, comments.Length);

        foreach (var item in firstRun.Results)
        {
            Assert.NotNull(item.ContextOutput.AiHash);
            Assert.False(item.ContextOutput.IsCached);

            Assert.NotNull(item.ContextOutput?.Context);
            Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
            Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
            Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));

            Assert.NotNull(item.ModelOutput?.Output);
            Assert.NotNull(item.ModelOutput?.Usage);

            Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
            Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
        }

        // test again, using the context objects from previous run
        // nothing should be sent to model (everything is cached)

        testGenAiScript.Results = firstRun.Results;

        // we are setting these to null in order to verify that we don't get model results in the 2nd run
        testGenAiScript.Results[0].ModelOutput = null;
        testGenAiScript.Results[1].ModelOutput = null;

        testGenAiScript.CreateContextObjects = false;

        var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.Equal(2, secondRun.Results.Count);

        foreach (var item in secondRun.Results)
        {
            Assert.NotNull(item.ContextOutput.AiHash);
            Assert.True(item.ContextOutput.IsCached);

            Assert.Null(item.ModelOutput);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanReuseContextFromPreviousRun(Options options)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            var post = new GenAiBasics.Post(
                [
                    new("This is a legit comment", "user1"),
                    new("You won a FREE iPhone, click now!", "spammer")
                ],
                "Title", "Body");
            await session.StoreAsync(post, id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);

        using var contextPool = database.DocumentsStorage.ContextPool;
        database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        var testGenAiScript = new TestGenAiScript
        {
            DocumentId = id,
            Configuration = new()
            {
                Name = "Check blog comments spam",
                Connection = new AiConnectionString
                {
                    Name = "ollama-local-deepseek-r1",
                    Identifier = "ollama-local-deepseek-r1",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://127.0.0.1:11434/",
                        Model = "llama3.2:latest"
                    }
                },
                Collection = "Posts",
                Prompt = "Check if the following blog post comment is spam or not",
                SampleObject = JsonConvert.SerializeObject(
                    new
                    {
                        Blocked = true, 
                        Reason = "Concise reason for why this comment was marked as spam or harmful"
                    }),
                Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments[idx].Spam = true;
}
this.Comments[idx].AiHash = $aiHash;
",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                }
            },
            CreateContextObjects = true,
            SendToModel = false,
            ApplyUpdateScript = false
        };

        // test context objects creation, without sending to model
        var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.NotNull(firstRun);
        Assert.Equal(2, firstRun.Results.Count);

        Assert.NotNull(firstRun.InputDocument);
        Assert.True(firstRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
        Assert.Equal(2, comments.Length);

        foreach (var item in firstRun.Results)
        {
            Assert.NotNull(item.ContextOutput?.Context);
            Assert.Null(item.ContextOutput.AiHash);
            Assert.False(item.ContextOutput.IsCached);
            Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
            Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
            Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));

            Assert.Null(item.ModelOutput);
        }

        using (var session = store.OpenAsyncSession())
        {
            // add another comment to the document 

            var p = await session.LoadAsync<GenAiBasics.Post>(id);
            p.Comments.Add(new GenAiBasics.Comment("3rd comment", "aviv"));

            await session.SaveChangesAsync();
        }

        // test again, this time with sending to the model and without creating context objects

        testGenAiScript.Results = firstRun.Results;
        testGenAiScript.CreateContextObjects = false;
        testGenAiScript.SendToModel = true;

        var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        // we should still have 2 context objects like before, not 3 (context objects were not created in the 2nd test run)
        Assert.Equal(2, secondRun.Results.Count);

        foreach (var item in secondRun.Results)
        {
            Assert.NotNull(item.ModelOutput?.Output);
            Assert.NotNull(item.ModelOutput?.Usage);

            Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
            Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));

            Assert.NotNull(item.ContextOutput.AiHash);
            Assert.False(item.ContextOutput.IsCached);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanReuseModelOutputFromPreviousRun(Options options)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("spam message $$$", "bot"),
                    new GenAiBasics.Comment("normal comment", "real_user")]
                , "Spam Check", "Some content"), id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        var testGenAiScript = new TestGenAiScript
        {
            DocumentId = id,
            Configuration = new()
            {
                Name = "Check blog comments spam",
                Connection = new AiConnectionString
                {
                    Name = "ollama-local-deepseek-r1",
                    Identifier = "ollama-local-deepseek-r1",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://127.0.0.1:11434/",
                        Model = "llama3.2:latest"
                    }
                },
                Collection = "Posts",
                Prompt = "Check if the following blog post comment is spam or not",
                SampleObject = JsonConvert.SerializeObject(
                    new
                    {
                        Blocked = true,
                        Reason = "Concise reason for why this comment was marked as spam or harmful"
                    }),
                Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments[idx].Spam = true;
}
this.Comments[idx].AiHash = $aiHash;
",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                }
            },
            CreateContextObjects = true,
            SendToModel = true,
            ApplyUpdateScript = false
        };


        var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        foreach (var item in firstRun.Results)
        {
            Assert.NotNull(item.ContextOutput?.Context);
            Assert.NotNull(item.ModelOutput?.Output);

            Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
            Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
        }

        Assert.Null(firstRun.OutputDocument);

        testGenAiScript.Results = firstRun.Results;
        testGenAiScript.CreateContextObjects = false;
        testGenAiScript.SendToModel = false;
        testGenAiScript.ApplyUpdateScript = true;

        // intentionally change the schema of the model output - in order to verify that the 2nd test run skips the model call
        testGenAiScript.Configuration.JsonSchema = null;
        testGenAiScript.Configuration.SampleObject = JsonConvert.SerializeObject(new
        {
            IsSpam = true, 
            Explanation = "Concise reason for why this comment was marked as spam or harmful"
        });

        var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        foreach (var item in secondRun.Results)
        {
            Assert.NotNull(item.ContextOutput?.Context);
            Assert.NotNull(item.ModelOutput?.Output);

            // model output should remain the same as in previous run

            Assert.False(item.ModelOutput.Output.TryGet("IsSpam", out bool _));
            Assert.False(item.ModelOutput.Output.TryGet("Explanation", out string _));

            Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
            Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
        }

        Assert.NotNull(secondRun.OutputDocument);
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanModifyUpdateScript(Options options)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("spam message $$$", "bot"),
                    new GenAiBasics.Comment("normal comment", "real_user")]
                , "Spam Check", "Some content"), id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        var testGenAiScript = new TestGenAiScript
        {
            DocumentId = id,
            Configuration = new()
            {
                Name = "Check blog comments spam",
                Connection = new AiConnectionString
                {
                    Name = "ollama-local-deepseek-r1",
                    Identifier = "ollama-local-deepseek-r1",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://127.0.0.1:11434/",
                        Model = "llama3.2:latest"
                    }
                },
                Collection = "Posts",
                Prompt = "Check if the following blog post comment is spam or not",
                SampleObject = JsonConvert.SerializeObject(
                    new
                    {
                        Blocked = true,
                        Reason = "Concise reason for why this comment was marked as spam or harmful"
                    }),
                Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Spam = $output.Blocked;
this.Comments[idx].AiHash = $aiHash;
",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                }
            },
            CreateContextObjects = true,
            SendToModel = true,
            ApplyUpdateScript = true
        };

        var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.Equal(2, firstRun.Results.Count);
        Assert.NotNull(firstRun.OutputDocument);

        Assert.True(firstRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
        Assert.Equal(2, comments.Length);
        foreach (var item in comments)
        {
            var comment = item as BlittableJsonReaderObject;
            Assert.NotNull(comment);

            Assert.True(comment.TryGet("Spam", out bool b));
            Assert.True(comment.TryGet("AiHash", out string h));
        }

        // change the update script and run again (skip context objects creation and model call)

        testGenAiScript.Results = firstRun.Results;
        testGenAiScript.CreateContextObjects = false;
        testGenAiScript.SendToModel = false;
        testGenAiScript.ApplyUpdateScript = true;

        testGenAiScript.Configuration.Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Reason = $output.Reason;
";

        var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.Equal(2, secondRun.Results.Count);
        Assert.NotNull(secondRun.OutputDocument);

        Assert.True(secondRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out comments));
        Assert.Equal(2, comments.Length);
        foreach (var item in comments)
        {
            var comment = item as BlittableJsonReaderObject;
            Assert.NotNull(comment);

            Assert.False(comment.TryGet("Spam", out bool b));
            Assert.False(comment.TryGet("AiHash", out string h));

            Assert.True(comment.TryGet("Reason", out string r));
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanModifyPromptAndSchema(Options options)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("Thanks for this article! I’ve been struggling with slow queries in RavenDB and your explanation of static vs. auto indexes really helped clarify the differences.", "tech_reader"),
                new GenAiBasics.Comment("🚨 HOT DEAL! Make $10,000 a week working from home. Limited time offer!!!", "spammer101"),
                new GenAiBasics.Comment("I found the indexing section insightful, especially the part about fanout indexes. I implemented one and saw query time drop by 80%.", "dev_jenny"),
                new GenAiBasics.Comment("Check out my blog for crazy RavenDB hacks and tricks they don’t want you to know 😈 raven-haxx.biz", "seo_bot")
                ],
                "Advanced RavenDB Indexing Strategies",
                "A deep dive into index design, fanout indexing, and performance tuning in RavenDB."
            ), id);

            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        var testGenAiScript = new TestGenAiScript
        {
            DocumentId = id,
            Configuration = new()
            {
                Name = "Check blog comments spam",
                Connection = new AiConnectionString
                {
                    Name = "ollama-local-deepseek-r1",
                    Identifier = "ollama-local-deepseek-r1",
                    OllamaSettings = new OllamaSettings
                    {
                        Uri = "http://127.0.0.1:11434/",
                        Model = "llama3.2:latest"
                    }
                },
                Collection = "Posts",
                Prompt = "Check if the following blog post comment is spam or not",
                SampleObject = JsonConvert.SerializeObject(new
                {
                    Blocked = true,
                    Reason = "Concise reason for why this comment was marked as spam or harmful"
                }),
                Update = @"
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Spam = $output.Blocked;
this.Comments[idx].AiHash = $aiHash;
",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
                }
            },
            CreateContextObjects = true,
            SendToModel = true,
            ApplyUpdateScript = false
        };

        var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
        Assert.Equal(4, firstRun.Results.Count);

        foreach (var item in firstRun.Results)
        {
            Assert.NotNull(item.ContextOutput?.Context);
            Assert.NotNull(item.ModelOutput?.Output);
            Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
            Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
        }

        // now modify prompt + schema
        testGenAiScript.Results = firstRun.Results;

        testGenAiScript.Configuration.Prompt = @"
Check if the following blog post comment is legit or not (spam/harmful/bot). 
Provide an explanation, confidence level (0.0–1.0), and summarize the comment in one sentence.";

        testGenAiScript.Configuration.JsonSchema = null;
        testGenAiScript.Configuration.SampleObject = JsonConvert.SerializeObject(new
        {
            LegitComment = true,
            Explanation = "Concise reason for why this comment is legit",
            ConfidenceLevel = 0.95,
            Summary = "Summary of the comment's content"
        });

        var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.Equal(4, secondRun.Results.Count);

        foreach (var item in secondRun.Results)
        {
            var output = item.ModelOutput?.Output;
            Assert.NotNull(output);

            Assert.True(output.TryGet("LegitComment", out bool legit));
            Assert.True(output.TryGet("Explanation", out string explanation));
            Assert.True(output.TryGet("ConfidenceLevel", out double confidence));
            Assert.True(output.TryGet("Summary", out string summary));

            Assert.False(string.IsNullOrWhiteSpace(explanation));
            Assert.InRange(confidence, 0.0, 1.0);
            Assert.False(string.IsNullOrWhiteSpace(summary));
        }
    }


    private class GenAiTestCmd : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly DocumentConventions _conventions;
        private readonly BlittableJsonReaderObject _testScript;
        public override bool IsReadRequest => true;

        public GenAiTestCmd(DocumentConventions conventions, BlittableJsonReaderObject testScript)
        {
            _conventions = conventions;
            _testScript = testScript;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/genai/test";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteObject(_testScript);
                    }
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }
    }
}
