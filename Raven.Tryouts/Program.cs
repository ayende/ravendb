using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Tests.FileSystem;
#if !DNXCORE50
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{
    public class Doc
    {
        public string Data { get; set; }
    }
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "TestDB"
            })
            {
                store.Initialize();
                var random = new Random();
                var mre = new ManualResetEventSlim();
                var data = new string('a',50 * 1024);
                int inx = 0;

                for (int t = 0; t < Environment.ProcessorCount; t++)
                {
                    Task.Run(() =>
                    {
                        var tId = Interlocked.Increment(ref inx);
                        while (!mre.IsSet)
                        {
                            using (var session = store.OpenSession())
                            {
                                for (var i = 1; i <= 100; i++)
                                {
                                    var docId = String.Format("doc/{0}/{1}",tId,i);
                                    var doc = session.Load<Doc>(docId);
                                    if (doc == null)
                                    {
                                        session.Store(new Doc
                                        {
                                            Data = data
                                        }, docId);
                                    }
                                    else
                                    {
                                        doc.Data = new string('x', 50*1024);
                                    }
                                }
                                session.SaveChanges();
                            }
                        }
                    });
                    Console.WriteLine("Started load task : {0}", t);
                }
                Console.WriteLine("Press any key to stop...");
                Console.ReadLine();
            }
        }
    }
}
