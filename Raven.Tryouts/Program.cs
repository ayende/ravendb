using Raven.Client.Embedded;
using Raven.Database.Extensions;

namespace Raven.Tryouts
{
	static class Program
	{
		public class Foo
		{
			public string Bar { get; set; }
		}

		public const int NumOfDocsToInsert = 250000;

		static void Main(string[] args)
		{
			IOExtensions.DeleteDirectory("\\FooBar");
			using (var store = new EmbeddableDocumentStore
			{
				DataDirectory = "\\FooBar",
				UseEmbeddedHttpServer = false
			})
			{
				store.Initialize();
				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < NumOfDocsToInsert; i++)
					{
						bulkInsert.Store(new Foo
						{
							Bar = "Bar/" + i
						});
					}
				}
			}
		}
	}
}
