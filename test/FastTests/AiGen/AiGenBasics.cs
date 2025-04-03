using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.AI.AiGen;
using Raven.Server.Documents.ETL.Providers.AI.AiGen.Test;
using Raven.Server.Documents.ETL.Providers.OLAP.Test;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.AiGen;

public class AiGenBasics(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public void CanCreateAiGetTask()
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
        
    
        store.Maintenance.Send(new AddAiGenOperation(new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local-deepseek-r1",
            Prompt = "Check if the following blog post comment is spam or not",
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
            Transforms =
            {
                new Transformation
                {
                    Collections = ["Posts"],
                    Name = "script",
                    Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
",
                }
            }
        }));
    }

    public record Comment(string Text, string Author)
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
    }
    public record Post(List<Comment> Comments, string Title, string Body);
    
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

        store.Maintenance.Send(new AddAiGenOperation(new GenAiConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local-deepseek-r1",
            Prompt = "Check if the following blog post comment is spam or not",
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
            Transforms =
            {
                new Transformation
                {
                    Collections = ["Posts"],
                    Name = "script",
                    Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
",
                }
            }
        }));
        
        using (var session = store.OpenSession())
        {
            var p = new Post(
            [
                new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
                {
                    
                },
                new Comment("Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.", "Oren Eini")
            ], "I, pencil", "A B52 pencil...");
            session.Store(p);
            session.SaveChanges();
        }

        WaitForUserToContinueTheTest(store);
        
        etl.Wait(CancellationToken.None);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CanProcessDocuments2()
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

        store.Maintenance.Send(new AddAiGenOperation(new AiGenConfiguration
        {
            Name = "Check blog comments spam",
            ConnectionStringName = "ollama-local-deepseek-r1",
            Prompt = "Check if the following blog post comment is spam or not",
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
            Transforms =
            {
                new Transformation
                {
                    Collections = ["Posts"],
                    Name = "script",
                    Script = @"
for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}
",
                }
            }
        }));

        using (var session = store.OpenSession())
        {
            var comment1 = new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage");

            var comment2 = new Comment("Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.", "Oren Eini");

            var comment3 = new Comment("Great post! I enjoyed reading it. Thanks :).", "aviv86");

            var p = new Post(
            [
                comment1,
                comment2,
                comment3,
            ], "I, pencil", "A B52 pencil...");
            session.Store(p, "posts/1");
            session.SaveChanges();
        }


        etl.Wait(CancellationToken.None);

        WaitForUserToContinueTheTest(store);

        using (var session = store.OpenSession())
        {
            var post = session.Load<Post>("posts/1");
            Assert.Equal(1, post.Comments.Count);

            Assert.Contains("Great post! I enjoyed reading it", post.Comments.Single().Text);
        }
    }
}
