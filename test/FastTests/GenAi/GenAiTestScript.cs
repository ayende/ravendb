using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.GenAi;

public class GenAiTestScript(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanTestAiGenScript(Options options)
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


                var testResult = GenAiTask.TestScript(testAiGenScript, database, database.ServerStore, context);

                var result = (GenAiTestScriptResult)testResult;
                Assert.Equal(3, result.Results.Count);
            }
        }
    }
}
