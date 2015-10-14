// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Util
{
	public static class AsyncHelpers
	{
		private static readonly ConcurrentQueue<LightweightSynchronizationContext> contextCache = new ConcurrentQueue<LightweightSynchronizationContext>(); 

		public static void RunSync(Func<Task> task)
		{
			AggregateException ex = null;
			var mre = new ManualResetEventSlim();

			LightweightSynchronizationContext sync;
			if(!contextCache.TryDequeue(out sync))
				sync = new LightweightSynchronizationContext();

			sync.Post(async _ =>
			{
				var t = task();
				await t;
				if (t.IsFaulted)
					ex = t.Exception;

				mre.Set();
			}, null);

			mre.Wait();

			contextCache.Enqueue(sync);
			if(ex != null)
			{
				var exception = ex.ExtractSingleInnerException();
				ExceptionDispatchInfo.Capture(exception).Throw();
			}
		}

		public static T RunSync<T>(Func<Task<T>> task)
		{
			var sp = Stopwatch.StartNew();
			AggregateException ex = null;
			var result = default(T);
			var mre = new ManualResetEventSlim();

			LightweightSynchronizationContext sync;
			if (!contextCache.TryDequeue(out sync))
				sync = new LightweightSynchronizationContext();

			sync.Post(async _ =>
			{
				var t = task();
				await t;
				if (t.IsFaulted)
					ex = t.Exception;

				mre.Set();
				result = t.Result;
			}, null);

			mre.Wait();

			contextCache.Enqueue(sync);
			if (ex != null)
			{
				var exception = ex.ExtractSingleInnerException();
				if (exception is OperationCanceledException)
					throw new TimeoutException("Operation timed out after: " + sp.Elapsed, ex);
				ExceptionDispatchInfo.Capture(exception).Throw();
			}

			return result;
		}

		private class LightweightSynchronizationContext : SynchronizationContext
		{
			private readonly ConcurrentQueue<Tuple<SendOrPostCallback,object>> executionQueue = new ConcurrentQueue<Tuple<SendOrPostCallback, object>>();
			public override void Send(SendOrPostCallback d, object state)
			{
				throw new NotSupportedException("Executing synchronous actions is not supported.");
			}

			public override void Post(SendOrPostCallback d, object state)
			{
				executionQueue.Enqueue(Tuple.Create(d,state));
				ExecuteOne();
			}

			private void ExecuteOne()
			{
				var originalContext = Current;
				try
				{
					int @try = 0;
					Tuple<SendOrPostCallback, object> actionData;
					while(!executionQueue.TryDequeue(out actionData) && @try++ < 10)
						Thread.SpinWait(1);

					if(actionData == null)
						throw new ApplicationException(@"Could not dequeue action for Synchronization Context. 
													This should not happen and is probably a bug");

					SetSynchronizationContext(this);
					actionData.Item1(actionData.Item2);
				}
				finally
				{
					SetSynchronizationContext(originalContext);
				}
			}

			public override SynchronizationContext CreateCopy()
			{
				return this;
			}
		}
	}
}
