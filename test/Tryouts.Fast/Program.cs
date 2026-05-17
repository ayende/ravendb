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

        // Run many Corax tests to trigger the SIGABRT crash
        var methods = typeof(FastTests.Corax.IndexSearcherTest).GetMethods()
            .Where(m => m.GetCustomAttributes(typeof(RavenFactAttribute), false).Length > 0
                     || m.GetCustomAttributes(typeof(RavenTheoryAttribute), false).Length > 0)
            .Where(m => m.GetParameters().Length == 0)
            .ToArray();

        Console.WriteLine($"Found {methods.Length} parameterless test methods");

        for (int run = 0; run < 3; run++)
        {
            Console.WriteLine($"\n=== Run {run} ===");
            foreach (var method in methods)
            {
                try
                {
                    using var testOutputHelper = new ConsoleTestOutputHelper();
                    await using var test = new FastTests.Corax.IndexSearcherTest(testOutputHelper);
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    method.Invoke(test, null);
                    Console.Write(".");
                }
                catch (Exception e)
                {
                    Console.Write("x");
                }
            }
        }
        Console.WriteLine("\nDone — no crash");
    }
}
