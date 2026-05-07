#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
using System;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Raven.Server.Utils;
using Xunit;
using FastTests;
using SlowTests.Corax;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        // XunitLogging removed in xUnit v3 migration
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        var sources = EventSource.GetSources();
        var runtime = sources.FirstOrDefault(x => x.Name == "System.Runtime");
        runtime?.Dispose();
        for (int i = 0; i < 1; i++)
        {
            Console.WriteLine($"Starting to run {i}");

            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                await using (var test = new RavenDB_22953(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    test.StreamingQueryMustNotThrowIndexOutOfRangeException(
                        RavenTestBase.Options.ForSearchEngine(RavenSearchEngineMode.Corax));
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }

    private static void TryRemoveDatabasesFolder()
    {
        var p = System.AppDomain.CurrentDomain.BaseDirectory;
        var dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
        }
    }
}
