using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Search;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class IndexSearcherHolder
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private volatile IndexSearcherHoldingState current;

		public ManualResetEvent SetIndexSearcher(IndexSearcher searcher, bool wait)
		{
			var old = current;
			current = new IndexSearcherHoldingState(searcher);

			if (old == null)
				return null;

			Interlocked.Increment(ref old.Usage);
			using (old)
			{
				if(wait)
					return old.MarkForDisposalWithWait();
				old.MarkForDisposal();
				return null;
			}
		}

		public IDisposable GetSearcher(out IndexSearcher searcher)
		{
			var indexSearcherHoldingState = GetCurrentStateHolder();
			try
			{
				searcher = indexSearcherHoldingState.IndexSearcher;
				return indexSearcherHoldingState;
			}
			catch (Exception e)
			{
				Log.ErrorException("Failed to get the index searcher.", e);
				indexSearcherHoldingState.Dispose();
				throw;
			}
		}

		public IDisposable GetSearcherAndTermDocs(out IndexSearcher searcher, out RavenJObject[] termDocs)
		{
			var indexSearcherHoldingState = GetCurrentStateHolder();
			try
			{
				searcher = indexSearcherHoldingState.IndexSearcher;
				termDocs = indexSearcherHoldingState.GetOrCreateTerms();
				return indexSearcherHoldingState;
			}
			catch (Exception)
			{
				indexSearcherHoldingState.Dispose();
				throw;
			}
		}

		private IndexSearcherHoldingState GetCurrentStateHolder()
		{
			while (true)
			{
				var state = current;
				Interlocked.Increment(ref state.Usage);
				if (state.ShouldDispose)
				{
					state.Dispose();
					continue;
				}

				return state;
			}
		}


		private class IndexSearcherHoldingState : IDisposable
		{
			public readonly IndexSearcher IndexSearcher;

			public volatile bool ShouldDispose;
			public int Usage;
			private RavenJObject[] readEntriesFromIndex;
			private readonly Lazy<ManualResetEvent> disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));

			public IndexSearcherHoldingState(IndexSearcher indexSearcher)
			{
				IndexSearcher = indexSearcher;
			}

			public void MarkForDisposal()
			{
				ShouldDispose = true;
			}

			public ManualResetEvent MarkForDisposalWithWait()
			{
				var x = disposed.Value;//  first create the value
				ShouldDispose = true;
				return x;
			}

			public void Dispose()
			{
				if (Interlocked.Decrement(ref Usage) > 0)
					return;
				if (ShouldDispose == false)
					return;
				DisposeRudely();
			}

			private void DisposeRudely()
			{
				if (IndexSearcher != null)
				{
					using (IndexSearcher)
					using (IndexSearcher.IndexReader){}
				}
				if(disposed.IsValueCreated)
					disposed.Value.Set();
			}

			[MethodImpl(MethodImplOptions.Synchronized)]
			public RavenJObject[] GetOrCreateTerms()
			{
				if (readEntriesFromIndex != null)
					return readEntriesFromIndex;

				var indexReader = IndexSearcher.IndexReader;
				readEntriesFromIndex = IndexedTerms.ReadAllEntriesFromIndex(indexReader);
				return readEntriesFromIndex;
			}
		}
	}
}
