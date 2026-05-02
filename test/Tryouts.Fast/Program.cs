#pragma warning disable CS1998
using System;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Raven.Server.Utils;
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

        for (int run = 0; run < 10; run++)
        {
            Console.WriteLine($"=== Run {run} ===");
            try
            {
                using var testOutputHelper = new ConsoleTestOutputHelper();
                await using var test = new FastTests.Corax.IndexSearcherTest(testOutputHelper);
                DebuggerAttachedTimeout.DisableLongTimespan = true;
                test.SingleOr();
                test.AllOr();
                test.SingleAnd();
                test.AllAnd();
                test.AllAndMemoized();
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Run {run}: {e.Message}");
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
        Console.WriteLine("Done");
    }
}
