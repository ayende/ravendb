using System;
using System.Diagnostics;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {

            for (int i = 0; i < 100; i++)
            {
                using (var testClass = new Rachis.Tests.CommandsTests())
                {
                    try
                    {
                        Console.WriteLine($"Test {i} is starting");
                        testClass.Command_not_committed_after_timeout_CompletionTaskSource_is_notified();
                        Console.WriteLine($"Test {i} is done");
                    }
                    catch (Exception e)
                    {
                        Debugger.Break();
                    }
                    Console.WriteLine($"Test {i} is done");
                }
            }
        }
    }
}
