using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Corax.Benchmark
{
    public interface IBenchmark
    {
        int Documents { get; }
        int SizeInKb { get; }

        void Clean();
    }

    public static class Benchmark
    {
        public static void Time(string name, Action<Stopwatch> action, IBenchmark bench, bool delete = true)
        {
            if (delete)
                bench.Clean();

            var sp = new Stopwatch();
            Console.Write("{0,-35}: running...", name);
            action(sp);

            Console.WriteLine("\r{0,-35}: {1,10:#,#} ms {2,10:#,#} docs / sec", name, sp.ElapsedMilliseconds, bench.Documents / sp.Elapsed.TotalSeconds);
        }

        public static void DeleteDirectory(string dir)
        {
            if (Directory.Exists(dir) == false)
                return;

            for (int i = 0; i < 10; i++)
            {
                try
                {
                    Directory.Delete(dir, true);
                    return;
                }
                catch (DirectoryNotFoundException)
                {
                    return;
                }
                catch (Exception)
                {
                    Thread.Sleep(13);
                }
            }

            Directory.Delete(dir, true);
        }
    }
}
