using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Server.Documents.ETL.Raven;
using Tests.Infrastructure;
using Voron;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static unsafe void Main()
        {
            using var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly());
            using var wtc = env.WriteTransaction();
            byte* buf = stackalloc byte[8192];
            var page = new Page(buf);
            var leaf = new SetLeafPage(page);
            leaf.Init(512);

           var list = new SortedList<int,int>();
            
            for (int i = 0; i < 1024*16*100; i++)
            {
                var a = 812 + (i%5 * 7) + i;
                if (i == 256)
                {
                    Console.WriteLine();
                }
                if (leaf.Add(wtc.LowLevelTransaction, a) == false)
                {
                    Validate(ref leaf, list.Keys);
                    Console.WriteLine("Hey");
                    return;
                }
                list.Add(a,a);

                if (i % 256 == 0)
                    Validate(ref leaf, list.Keys);
            }
        }

        private static void Validate(ref SetLeafPage p, IList<int> expected)
        {
            Span<int> scratch = stackalloc int[128];
            var it = p.GetIterator(scratch);
            for (int i = 0; i < expected.Count; i++)
            {
                if (i == 269 && expected.Count == 513)
                {
                    Console.WriteLine();
                }
                if (it.MoveNext(out var cur) == false ||
                    cur != expected[i])
                {
                    Console.WriteLine("Opps");
                }
            }

            if (it.MoveNext(out var a))
            {
                Console.WriteLine("Um...");
            }
        }
    }
}
