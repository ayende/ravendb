using System;
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
            for (int i = 0; i < 512; i++)
            {
                var a = 812 + (i%5 * 7) + i;
                leaf.Add(wtc.LowLevelTransaction, a);
            }
        }
    }
}
