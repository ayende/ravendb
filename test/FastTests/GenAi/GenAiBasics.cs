using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.GenAi;

public class GenAiBasics(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public void CanCreateGenAiTask()
    {
        using var store = GetDocumentStore();
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
        
    
        store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local-deepseek-r1",
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
            Name = "ollama-local-deepseek-r1",
            Identifier = "ollama-local-deepseek-r1",
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "deepseek-r1:1.5b"
            }
        }));
        
        var etl = Etl.WaitForEtlToComplete(store);

        store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local-deepseek-r1",
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
        const string connectionStrName = "ollama-local-deepseek-r1";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "deepseek-r1:1.5b"
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
        const string connectionStrName = "ollama-local-deepseek-r1";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "deepseek-r1:1.5b"
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
        const string connectionStrName = "ollama-local-deepseek-r1";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "deepseek-r1:1.5b"
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
        const string connectionStrName = "ollama-local-deepseek-r1";

        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
        {
            Name = connectionStrName,
            Identifier = connectionStrName,
            OllamaSettings = new OllamaSettings
            {
                Uri = "http://127.0.0.1:11434/",
                Model = "deepseek-r1:1.5b"
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

    internal record Comment(string Text, string Author)
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
    }
    internal record Post(List<Comment> Comments, string Title, string Body);
}
