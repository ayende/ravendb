#pragma warning disable CS1998
using System;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Raven.Server.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;
using FastTests;

namespace Tryouts.Fast;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine($"PID: {Process.GetCurrentProcess().Id}");
        var sources = EventSource.GetSources();
        var runtime = sources.FirstOrDefault(x => x.Name == "System.Runtime");
        runtime?.Dispose();

        try
        {
            using var testOutputHelper = new ConsoleTestOutputHelper();
            await using var test = new ScoreDebug(testOutputHelper);
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            test.Run();
        }
        catch (Exception e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e);
            Console.ForegroundColor = ConsoleColor.White;
        }
    }
}

public record Person(string Name);

public class ScoreDebug : RavenTestBase
{
    public ScoreDebug(ITestOutputHelper output) : base(output) { }

    public void Run()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Person("Maciej"));
            session.Store(new Person("Marika"));
            session.SaveChanges();
        }

        Console.WriteLine("=== No boosting ===");
        using (var session = store.OpenSession())
        {
            var q = session.Query<Person>()
                .ToDocumentQuery()
                .WaitForNonStaleResults()
                .WhereEquals(i => i.Name, "Maciej")
                .OrElse()
                .WhereEquals(i => i.Name, "marika");
            Console.WriteLine($"RQL: {q}");
            var results = q.ToList();
            for (int i = 0; i < results.Count; i++)
                Console.WriteLine($"  [{i}] {results[i].Name}");
        }

        Console.WriteLine("\n=== With boosting (Maciej=1, marika=1000) ===");
        using (var session = store.OpenSession())
        {
            var q = session.Query<Person>()
                .ToDocumentQuery()
                .WaitForNonStaleResults()
                .WhereEquals(i => i.Name, "Maciej").Boost(1)
                .OrElse()
                .WhereEquals(i => i.Name, "marika").Boost(1000);
            Console.WriteLine($"RQL: {q}");
            var results = q.ToList();
            for (int i = 0; i < results.Count; i++)
                Console.WriteLine($"  [{i}] {results[i].Name}");
        }

        Console.WriteLine("\n=== With boosting + explicit OrderByScore ===");
        using (var session = store.OpenSession())
        {
            var q = session.Query<Person>()
                .ToDocumentQuery()
                .WaitForNonStaleResults()
                .WhereEquals(i => i.Name, "Maciej").Boost(1)
                .OrElse()
                .WhereEquals(i => i.Name, "marika").Boost(1000)
                .OrderByScore();
            Console.WriteLine($"RQL: {q}");
            var results = q.ToList();
            for (int i = 0; i < results.Count; i++)
                Console.WriteLine($"  [{i}] {results[i].Name}");
        }
    }
}
