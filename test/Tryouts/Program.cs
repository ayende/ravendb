using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Issues;
using FastTests.Server.Replication;
using Lucene.Net.Store;
using SlowTests.Server.Rachis;
using Sparrow.Logging;
using Sparrow.Platform.Win32;
using Voron.Global;
using Directory = System.IO.Directory;

namespace Tryouts
{
    public unsafe class Program
    {
        public static void Main(string[] args)
        {
            //Console.WriteLine("Press any key to start.");
            //Console.ReadKey();
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new CRUD())
                {
                    test.CRUD_Operations();
                }
                //LoggingSource.Instance.SetupLogMode(LogMode.Information, "logs");
                //Parallel.For(0, 10, _ =>
                //{
                //    using (var a = new BasicTests())
                //    {
                //        a.CanApplyCommitAcrossAllCluster(amount: 7).Wait();
                //    }
                //});
                //LoggingSource.Instance.SetupLogMode(LogMode.None, "logs");
                //Directory.Delete("logs", true);

            }
        }
    }
}