using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiBasics(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public void CanCreateGenAiTask()
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "ollama-local",
            Identifier = "ollama-local",
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));
        
    
        store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local",
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
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
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
"
            }
        }));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CanProcessDocuments()
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "ollama-local",
            Identifier = "ollama-local",
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));
        
        var etl = Etl.WaitForEtlToComplete(store);

        store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local",
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
            }),
            Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}",
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            }
        }));
        
        using (var session = store.OpenSession())
        {
            var p = new Post(
            [
                new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                new Comment("Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.", "Oren Eini")
            ], "I, pencil", "A B52 pencil...");
            session.Store(p);
            session.SaveChanges();
        }

        etl.Wait(CancellationToken.None);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetGenAiOngoingTask()
    {
        const string connectionStrName = "ollama-local";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));


        var configuration = new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = connectionStrName,
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
            }),
            Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}",
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            }
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        var op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi);
        var taskInfo = await store.Maintenance.SendAsync(op);

        Assert.NotNull(taskInfo);
        Assert.Equal(configuration.Name, taskInfo.TaskName);
        Assert.Equal(OngoingTaskType.GenAi, taskInfo.TaskType);
        Assert.Equal(OngoingTaskConnectionStatus.Active, taskInfo.TaskConnectionStatus);

        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(configuration.ConnectionStringName, genAiTaskInfo.ConnectionStringName);
        Assert.Equal(configuration.Collection, genAiTaskInfo.Configuration.Collection);
        Assert.Equal(configuration.Prompt, genAiTaskInfo.Configuration.Prompt);
        Assert.Equal(configuration.SampleObject, genAiTaskInfo.Configuration.SampleObject);
        Assert.Equal(configuration.Update, genAiTaskInfo.Configuration.Update);
        Assert.Equal(configuration.AiConnectorType, genAiTaskInfo.Configuration.AiConnectorType);
        Assert.Equal(configuration.GenAiTransformation.Script, genAiTaskInfo.Configuration.GenAiTransformation.Script);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanEditGenAiTask()
    {
        const string connectionStrName = "ollama-local";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));


        var configuration = new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = connectionStrName,
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
            }),
            Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
",
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            }
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        var op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi);
        var taskInfo = await store.Maintenance.SendAsync(op);
        var taskId = taskInfo.TaskId;

        var newUpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].LegitComment = $output.Blocked == false;
";
        configuration.Update = newUpdateScript;

        store.Maintenance.Send(new UpdateEtlOperation<AiConnectionString>(taskId, configuration));

        op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi);
        taskInfo = await store.Maintenance.SendAsync(op);

        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(newUpdateScript, genAiTaskInfo.Configuration.Update);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanDeleteGenAiTask()
    {
        const string connectionStrName = "ollama-local";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));


        var configuration = new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = connectionStrName,
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
            }),
            Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
",
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            }
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi));
        Assert.NotNull(taskInfo);
        var taskId = taskInfo.TaskId;

        store.Maintenance.Send(new DeleteOngoingTaskOperation(taskId, OngoingTaskType.GenAi));
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi));
        Assert.Null(taskInfo);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanToggleGenAiTaskState()
    {
        const string connectionStrName = "ollama-local";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));

        var configuration = new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = connectionStrName,
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
            }),
            Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}",
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            }
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
        var taskId = taskInfo.TaskId;

        // disable task
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: true));

        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);

        // re-enable task
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: false));
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ShouldTrackAiHashesInMetadata()
    {
        using var store = GetDocumentStore();

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = "ollama-local",
            Identifier = "ollama-local",
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "llama3.2:latest"
            }
        }));

        var etl = Etl.WaitForEtlToComplete(store);

        const string taskName = "Check blog comments spam";

        store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
        {
            Name = taskName,
            ConnectionStringName = "ollama-local",
            Prompt = "Check if the following blog post comment is spam or not",
            Collection = "Posts",
            SampleObject = JsonConvert.SerializeObject(new
            {
                Blocked = true,
                Reason = "Concise reason for why this comment was marked as spam or ham"
            }),
            Update = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}",
            GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            }
        }));

        const string docId = "posts/1";

        var post = new Post([
            new Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
            new Comment("Great article. Helped me understand indexing in RavenDB.", "alex"),
            new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
        ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful...");

        using (var session = store.OpenSession())
        {
            session.Store(post, docId);
            session.SaveChanges();
        }

        etl.Wait();

        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(GenAiTask.GenAiHashesMetadataKey, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashes));
            Assert.NotNull(hashes);

            var expectedHashes = GetExpectedHashes(post, docId);
            var actualHashes = hashes.Select(h => h.ToString()).ToList();

            Assert.Equal(expectedHashes.Count, actualHashes.Count);
            foreach (var expected in expectedHashes)
                Assert.Contains(expected, actualHashes);
        }

        etl = Etl.WaitForEtlToComplete(store);

        // Update the document to trigger a new context output
        using (var session = store.OpenSession())
        {
            post = session.Load<Post>(docId);
            post.Comments.Add(new Comment("Nice summary. Bookmarked for future reference.", "emma"));
            session.SaveChanges();
        }

        etl.Wait();

        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(GenAiTask.GenAiHashesMetadataKey, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashes));
            Assert.NotNull(hashes);

            var expectedHashes = GetExpectedHashes(post, docId);
            var actualHashes = hashes.Select(h => h.ToString()).ToList();

            Assert.Equal(expectedHashes.Count, actualHashes.Count);
            foreach (var expected in expectedHashes)
                Assert.Contains(expected, actualHashes);
        }

        static List<string> GetExpectedHashes(Post post, string docId)
        {
            var results = new List<string>();
            using var context = JsonOperationContext.ShortTermSingleUse();

            foreach (var comment in post.Comments)
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Comment.Text)] = comment.Text,
                    [nameof(Comment.Author)] = comment.Author,
                    [nameof(Comment.Id)] = comment.Id
                };

                using var ctxDoc = context.ReadObject(djv, docId);
                var json = ctxDoc.ToString();
                var hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(json.AsSpan()));

                results.Add(hash);
            }

            return results;
        }
    }


    internal record Comment(string Text, string Author)
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
    }
    internal record Post(List<Comment> Comments, string Title, string Body);
}
