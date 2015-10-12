// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Util
{
	public static class AsyncHelpers
	{

		public static void RunSync(Func<Task> task)
		{
			var oldContext = SynchronizationContext.Current;
			try
			{
				var synch = new LightweightSynchronizationContext();
				SynchronizationContext.SetSynchronizationContext(synch);
				var mre = new ManualResetEventSlim();
				synch.Post(async _ =>
				{
					try
					{
						await task();
					}
					catch (Exception e)
					{
						synch.InnerException = e;
						throw;
					}
					finally
					{
						synch.EndMessageLoop();
						mre.Set();
                    }
				}, null);
				synch.BeginMessageLoop();
				mre.Wait();
			}
			catch (AggregateException ex)
			{
				var exception = ex.ExtractSingleInnerException();
				ExceptionDispatchInfo.Capture(exception).Throw();
			}
			finally
			{
				SynchronizationContext.SetSynchronizationContext(oldContext);
			}
		}

		public static T RunSync<T>(Func<Task<T>> task)
		{
			var result = default(T);
		    Stopwatch sp = Stopwatch.StartNew();
			var oldContext = SynchronizationContext.Current;
			try
			{
				var synch = new LightweightSynchronizationContext();
				SynchronizationContext.SetSynchronizationContext(synch);
				var mre = new ManualResetEventSlim();
				synch.Post(async _ =>
				{
					try
					{
						result = await task();
					}
					catch (Exception e)
					{
						synch.InnerException = e;
						throw;
					}
					finally
					{
                        sp.Stop();
						synch.EndMessageLoop();
						mre.Set();
                    }
				}, null);
				synch.BeginMessageLoop();
				mre.Wait();
			}
			catch (AggregateException ex)
			{
				var exception = ex.ExtractSingleInnerException();
			    if (exception is OperationCanceledException)
			        throw new TimeoutException("Operation timed out after: " + sp.Elapsed, ex);
				ExceptionDispatchInfo.Capture(exception).Throw();
			}
			finally
			{
				SynchronizationContext.SetSynchronizationContext(oldContext);
			}

			return result;
		}

		private class LightweightSynchronizationContext : SynchronizationContext
		{
			private readonly BlockingCollection<Tuple<SendOrPostCallback, object>> executionCollection = new BlockingCollection<Tuple<SendOrPostCallback, object>>();
			private CancellationTokenSource cts = new CancellationTokenSource();

			public Exception InnerException { get; set; }			

			public override void Send(SendOrPostCallback d, object state)
			{
				throw new NotSupportedException("We cannot send to our same thread");
			}

			public override void Post(SendOrPostCallback d, object state)
			{
				executionCollection.Add(Tuple.Create(d,state),cts.Token);
			}

			public void EndMessageLoop()
			{
				cts.Cancel();
				cts = new CancellationTokenSource();
            }

			private void MessageLoop()
			{
				var cancellationToken = cts.Token;
				do
				{
					cancellationToken.ThrowIfCancellationRequested();

					Tuple<SendOrPostCallback, object> taskTuple;
					if (!executionCollection.TryTake(out taskTuple, 250, cancellationToken))
						continue;

					taskTuple.Item1(taskTuple.Item2);

					if (InnerException != null) // the method threw an exeption
						throw new AggregateException("AsyncHelpers.Run method threw an exception.", InnerException);
				} while (!cts.IsCancellationRequested);
			}

			public void BeginMessageLoop()
			{
				Task.Run(() => MessageLoop(),cts.Token);
			}

			public override SynchronizationContext CreateCopy()
			{
				return this;
			}
		}
	}
}
