using System;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.Replication;
using Lucene.Net.Store;
using SlowTests.Server.Rachis;
using Sparrow.Logging;
using Directory = System.IO.Directory;
using FastTests.Client.Indexing;
using Raven.Client.Util;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                using (var test = new StaticIndexesFromClient())
                {
                    try
                    {
                        AsyncHelpers.RunSync(() =>
                            test.Can_Put_And_Replace());
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                    Console.WriteLine(i);
                }
            }
        }
    }
}