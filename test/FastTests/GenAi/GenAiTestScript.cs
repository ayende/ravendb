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
                                Model = "deepseek-r1:1.5b"
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
                    Assert.True(item.ModelOutput.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(item.ModelOutput.TryGet("Reason", out string r));
                    Assert.False(string.IsNullOrEmpty(r));

                    Assert.NotNull(item.AiHash);
                    Assert.False(item.IsCached);

                    Assert.True(item.Context.TryGet("Text", out string _));
                    Assert.True(item.Context.TryGet("Author", out string _));
                    Assert.True(item.Context.TryGet("Id", out string _));
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
                    Model = "deepseek-r1:1.5b"
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
                    Assert.True(modelOutput.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(modelOutput.TryGet("Reason", out string r));
                    Assert.False(string.IsNullOrEmpty(r));

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.AiHash), out string hash));
                    Assert.NotNull(hash);

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.IsCached), out bool cached));
                    Assert.False(cached);

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.Context), out BlittableJsonReaderObject ctx));
                    Assert.True(ctx.TryGet("Text", out string _));
                    Assert.True(ctx.TryGet("Author", out string _));
                    Assert.True(ctx.TryGet("Id", out string _));
                }

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.OutputDocument), out BlittableJsonReaderObject output));
                Assert.NotNull(output);
                Assert.True(output.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

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
                            OllamaSettings = new OllamaSettings { Uri = "http://127.0.0.1:11434/", Model = "deepseek-r1:1.5b" }
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

                var result = GenAiTask.TestScript(testAiGenScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(result);
                Assert.Equal(4, result.Results.Count);

                foreach (var item in result.Results)
                {
                    Assert.NotNull(item.ModelOutput);
                    Assert.NotNull(item.AiHash);
                    Assert.False(item.IsCached);
                }

                // save the output doc as a new document
                var newId = id + "/new";
                using (var requestExecutor = store.GetRequestExecutor())
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    await requestExecutor.ExecuteAsync(new PutDocumentCommand(store.Conventions, newId, changeVector: null, result.OutputDocument), ctx);
                }

                // re-run the test script on the output document
                testAiGenScript.DocumentId = newId;
                result = GenAiTask.TestScript(testAiGenScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(result);
                Assert.Equal(4, result.Results.Count);

                // should not send anything to model, all comments are cached 
                foreach (var item in result.Results)
                {
                    Assert.Null(item.ModelOutput);
                    Assert.Null(item.AiHash);
                    Assert.True(item.IsCached);
                }

            }

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
