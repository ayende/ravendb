using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Voron.Benchmark.Corax;

namespace Voron.Benchmark
{
    public class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "quick-roaring")
            {
                RoaringBitmapQuickBench.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "profile-roaring")
            {
                RoaringBitmapProfileBench.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "trace-roaring")
            {
                RoaringBitmapTraceBench.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "corax-roaring")
            {
                RoaringBitmapCoraxBench.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "corax-query")
            {
                CoraxQueryBitmapBench.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "contains-bench")
            {
                ContainsArrayBench.Run();
                return;
            }

#if DEBUG
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new DebugInProcessConfig());
#else
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
#endif
        }
    }
}
