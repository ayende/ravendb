using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{

	public class MassiveStreamsTest : RavenTestBase
	{
		private const int CountOfDocs = 100000;
		public class FooBar
		{
			public string Foo { get; set; }
			public string Bar { get; set; }
		}

		private readonly CancellationTokenSource cts = new CancellationTokenSource();

		[Fact]
		public void Lots_of_streams_should_not_cause_issues()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < i++; i++)
					{
						bulkInsert.Store(new FooBar
						{
							Foo = "Bar",
							Bar = "Foo"
						});
					}
				}

				int count = 0;
				var cd = new CountdownEvent(35);				
				Task.Run(() =>
				{
					while (count < 100)	
						Task.Run(() =>
						{
							DoStreaming(store, cts.Token,cd);
							Interlocked.Increment(ref count);
						});
					while(!cts.IsCancellationRequested)
						Thread.SpinWait(5);
				});

				cd.Wait(5000);

				if(File.Exists("C:\\Work\\massive_streams.dmp"))
					File.Delete("C:\\Work\\massive_streams.dmp");

				DumpHelper.WriteTinyDumpForThisProcess("C:\\Work\\massive_streams.dmp");

				using (var session = store.OpenSession())
				{
					var sw = Stopwatch.StartNew();
					var q = session.Query<FooBar>().ToList();
					Assert.True(sw.ElapsedMilliseconds < 1000,"Should be sw.ElapsedMilliseconds < 1000, but sw.ElapsedMilliseconds is " + sw.ElapsedMilliseconds);
					Console.WriteLine("sw.ElapsedMilliseconds = " + sw.ElapsedMilliseconds);
				}

				cts.Cancel();
			}

		}

		private void DoStreaming(IDocumentStore store, CancellationToken ct, CountdownEvent cd)
		{
			ct.ThrowIfCancellationRequested();
			using (var session = store.OpenSession())
			using (var stream = session.Advanced.Stream(session.Query<FooBar>()))
			{
				int x = 0;
				do
				{
					if (x == 3)
						cd.Signal();
					ct.ThrowIfCancellationRequested();
					Thread.SpinWait(5);
					x++;
				} while (stream.MoveNext());
			}
		}
	}
}
