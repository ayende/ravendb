using System;
using System.Linq;
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
using Voron.Data.CompactTrees;
using Voron.Debugging;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static void Main(string[] args)
        {
            var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly());
            using (var tx = env.WriteTransaction())
            {
                var ct = CompactTree.Create(tx.LowLevelTransaction, "test");
                for (int i = 0; i < 400000; i++)
                {
                    ct.Add("hi" + i, i);
                }
                Validate(ct);
                for (int i = 0; i < 400000; i++)
                {
                    if(i == 230)
                    {
                        ct.Render();
                        Console.WriteLine(9);
                    }
                    if(ct.Remove("hi" + i, out var l) == false || l != i)
                    {
                        Console.WriteLine("Opps: " + i);
                    }
                }
                ct.Render();

                Console.WriteLine("Done!");
            }
        }

        private static void Validate(CompactTree ct)
        {
            ct.Seek("");
            {
                int index = 0;
                var set = new Dictionary<long, int>();
                while (ct.Next(out _, out var l))
                {
                    if (set.TryAdd(l, index++) == false)
                    {
                        Console.WriteLine("Duplicate");
                    }
                }
                var list = set.Keys.ToList();
                list.Sort();
                for (int i = 0; i < 400000; i++)
                {
                    if (list[i] != i)
                    {
                        Console.WriteLine("err @ " + i);
                    }
                }
                if (list.Count != 400000)
                {
                    Console.WriteLine("Missing");
                }
            }
            {
                for (int i = 0; i < 400000; i++)
                {
                    ct.Seek("hi" + i);
                    if (ct.Next(out _, out var val2) == false || val2 != i)
                        Console.WriteLine("failed at: " + i);
                }
            }
        }
    }
}
