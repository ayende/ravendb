using System;
using System.ComponentModel.Composition.Hosting;
using Raven.Bundles.ScriptedTriggers;
using Raven.Client.Embedded;

namespace Raven.Bundles.Tests.ScriptedTriggers
{
	public abstract class ScriptedTriggerTestBase : IDisposable
	{

		protected EmbeddableDocumentStore DocumentStore { get; set; }

		protected ScriptedTriggerTestBase()
		{
			var catalog = new AssemblyCatalog(typeof (ScriptedPutTrigger).Assembly);

			DocumentStore = new EmbeddableDocumentStore
									 {
										 RunInMemory = true,
										 Configuration =
											 {
												 Catalog = {Catalogs = {catalog}}
											 }
									 };
			DocumentStore.Initialize();
		}

		public void Dispose()
		{
			DocumentStore.Dispose();
		}
	}
}
