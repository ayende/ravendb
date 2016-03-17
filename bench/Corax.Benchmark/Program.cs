using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Voron;

namespace Corax.Benchmark
{
    public class Program 
    {
        public static void Main(string[] args)
        {
#if DEBUG
            var oldfg = Console.ForegroundColor;
            var oldbg = Console.BackgroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = ConsoleColor.Yellow;

            Console.WriteLine("Dude, you are running benchmark in debug mode?!");
            Console.ForegroundColor = oldfg;
            Console.BackgroundColor = oldbg;
#endif
            var corax = new CoraxBenchmark();
            corax.Run();            

            Console.ReadLine();
        }


    }
}
