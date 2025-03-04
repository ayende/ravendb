using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using RachisTests;
using Raven.Server.Utils;
using SlowTests.Issues;
using Tests.Infrastructure;
using Xunit;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);

        for (int i = 0; i < 1000; i++)
            try
            {
                Console.WriteLine(i);
                using (ConsoleTestOutputHelper testOutputHelper = new())
                using (RavenDB_22659 test = new(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;

                    await test.CannotDeleteDatabaseWhenRestoreCancelledOnNonResponsibleNode();
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
    }

    private static void TryRemoveDatabasesFolder()
    {
        string p = AppDomain.CurrentDomain.BaseDirectory;
        string dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
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
