using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Utils;
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

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CanGetGenAiStats()
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

        var etlDone = Etl.WaitForEtlToComplete(store);

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
this.Comments[idx].IsSpam = $output.Blocked;
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
        }));


        var db = await GetDatabase(store.Database);

        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

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

        etlDone.Wait();

        var stats = etlProcess.GetPerformanceStats()
            .Where(x => x.NumberOfLoadedItems > 0)
            .ToArray();

        Assert.Equal(1, stats[0].NumberOfExtractedItems[EtlItemType.Document]);

        var loadDetails = stats[0].Details.Operations[^1];

        Assert.Equal("Load", loadDetails.Name);

        var genAiStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.NotNull(genAiStats);

        Assert.Equal(3, genAiStats.NumberOfContextObjects);
        Assert.Equal(0, genAiStats.TotalCachedContexts);
        Assert.Equal(3, genAiStats.TotalSentToModel);

        Assert.True(genAiStats.CompletionTokensUsed > 0);
        Assert.True(genAiStats.PromptTokensUsed > 0);
        var expectedTotalTokens = genAiStats.CompletionTokensUsed + genAiStats.PromptTokensUsed;
        Assert.Equal(expectedTotalTokens, genAiStats.TotalTokensUsed);

        using (var session = store.OpenAsyncSession())
        {
            etlDone = Etl.WaitForEtlToComplete(store);

            // add a new comment

            var doc = await session.LoadAsync<Post>(docId);
            doc.Comments.Add(new Comment("new spam comment", "evil hacker"));

            var etag = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(doc), db.DbBase64Id);

            await session.SaveChangesAsync();

            Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

            var stats2 = etlProcess.GetPerformanceStats()
                .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag > etag)
                .ToArray();

            Assert.Equal(1, stats2[^1].NumberOfExtractedItems[EtlItemType.Document]);

            var loadDetails2 = stats2[^1].Details.Operations[^1];

            Assert.Equal("Load", loadDetails2.Name);

            var genAiStats2 = loadDetails2.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
            Assert.NotNull(genAiStats2);

            // only the newly added comment should be sent to model, the rest should be cached
            Assert.Equal(4, genAiStats2.NumberOfContextObjects);
            Assert.Equal(3, genAiStats2.TotalCachedContexts);
            Assert.Equal(1, genAiStats2.TotalSentToModel);

            Assert.True(genAiStats2.CompletionTokensUsed > 0);
            Assert.True(genAiStats2.PromptTokensUsed > 0);
            Assert.True(genAiStats2.TotalTokensUsed > 0);
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ShouldResendContextWhenPromptChanges()
    {
        await ShouldResendContextOnConfigChange(
            changeConfig: config => config.Prompt = "please convert the text to Hebrew"
        );
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ShouldResendContextWhenSchemaChanges()
    {
        await ShouldResendContextOnConfigChange(
            changeConfig: config =>
            {
                var newSample = JsonConvert.SerializeObject(new
                {
                    Translation = "translated sentence",
                    OriginalLanguage = "the original language of the provided text",
                    TranslatedTo = "the language that you translated the text to"
                });
                config.JsonSchema = AbstractChatCompletionClient.GetSchemaFor(newSample);
            }
        );
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ShouldResendContextWhenUpdateScriptChanges()
    {
        await ShouldResendContextOnConfigChange(
            changeConfig: config => config.Update = "this.Translated = $output.Translation;"
        );
    }


    private async Task ShouldResendContextOnConfigChange(Action<GenAiConfiguration> changeConfig)
    {
        using var store = GetDocumentStore();
        const string taskName = "ConfigChangeTest";
        const string docId = "posts/1";

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

        var sampleObject = JsonConvert.SerializeObject(new { Translation = "translated text" });
        var schema = AbstractChatCompletionClient.GetSchemaFor(sampleObject);

        var config = new GenAiConfiguration
        {
            Name = taskName,
            ConnectionStringName = "ollama-local",
            Prompt = "Translate this text to Polish",
            JsonSchema = schema,
            Update = "this.TextInPolish = $output.Translation;",
            Collection = "Posts",
            GenAiTransformation = new GenAiTransformation
            {
                Script = "context({ Text: this.Body });"
            }
        };

        store.Maintenance.Send(new AddGenAiOperation(config));

        var etlDone = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenSession())
        {
            session.Store(new Post([new Comment("RavenDB is amazing", "Alex")], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful..."), docId);
            session.SaveChanges();
        }

        Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

        string originalHash;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(GenAiTask.GenAiHashesMetadataKey, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashesArray));
            Assert.NotNull(hashesArray);
            originalHash = hashesArray.Last().ToString();
        }

        var db = await GetDatabase(store.Database);
        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        var stats = etlProcess.GetPerformanceStats()
            .Where(x => x.NumberOfLoadedItems > 0)
            .ToArray();

        Assert.NotEmpty(stats);
        var loadDetails = stats[0].Details.Operations[^1];
        var genAiStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.Equal(1, genAiStats?.NumberOfContextObjects);
        Assert.Equal(1, genAiStats?.TotalSentToModel);

        var taskId = etlProcess.TaskId;

        // disable task
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: true));

        var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);

        // update the configuration
        changeConfig(config);
        store.Maintenance.Send(new UpdateEtlOperation<AiConnectionString>(taskId, config));

        // re-enable task
        await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.GenAi, disable: false));
        taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.GenAi));
        Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);

        var genAiTaskInfo = taskInfo as Raven.Client.Documents.Operations.OngoingTasks.GenAi;
        Assert.NotNull(genAiTaskInfo);
        Assert.Equal(genAiTaskInfo.Configuration.Prompt, config.Prompt);
        Assert.Equal(genAiTaskInfo.Configuration.JsonSchema, config.JsonSchema);
        Assert.Equal(genAiTaskInfo.Configuration.Update, config.Update);

        WaitForUserToContinueTheTest(store);

        etlDone = Etl.WaitForEtlToComplete(store);
        long etag = 0;
        using (var session = store.OpenSession())
        {
            // modify the doc to trigger etl 
            // the post's Body remains the same - this change won't affect the generated context object 
            // context should be resent because of the hash-mismatch, not because of the comments addition

            var doc = session.Load<Post>(docId);
            doc.Comments.Add(new Comment("spam comment", "evil bot"));

            etag = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(doc), db.DbBase64Id);

            session.SaveChanges();
        }

        Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

        // assert that context was sent again

        WaitForUserToContinueTheTest(store);


        etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        var stats2 = etlProcess.GetPerformanceStats()
            .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag > etag)
            .ToArray();
        Assert.NotEmpty(stats2);

        Assert.Equal(1, stats2[^1].NumberOfExtractedItems[EtlItemType.Document]);

        var loadDetails2 = stats2[^1].Details.Operations[^1];
        var genAiStats2 = loadDetails2.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.NotNull(genAiStats2);

        Assert.Equal(1, genAiStats2.NumberOfContextObjects);
        Assert.Equal(1, genAiStats2.TotalSentToModel);
        Assert.Equal(0, genAiStats2.TotalCachedContexts);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(GenAiTask.GenAiHashesMetadataKey, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashesArray));
            Assert.NotNull(hashesArray);

            var newHash = hashesArray.Last().ToString();
            Assert.NotEqual(originalHash, newHash);
        }
    }



    internal record Comment(string Text, string Author)
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
    }
    internal record Post(List<Comment> Comments, string Title, string Body);
}
