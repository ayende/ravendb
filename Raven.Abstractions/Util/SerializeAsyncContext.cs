using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;

namespace Raven.Abstractions.Util
{
	//credit to Nito.AsyncEx library (https://github.com/StephenCleary/AsyncEx)
	//adapted and changed code from AsyncContext
	public class SerializeAsyncContext 
	{
		private readonly LinkedListQueue<Task> executionQueue = new LinkedListQueue<Task>();
		private readonly TaskFactory taskFactory;
		private readonly SerialSynchronizationContext enqueueTasksContext;
		private readonly QueueTaskScheduler scheduler;
		private volatile int pendingOperations;
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private SerializeAsyncContext()
		{
			scheduler = new QueueTaskScheduler(this);
			taskFactory = new TaskFactory(scheduler);
			enqueueTasksContext = new SerialSynchronizationContext(this);
        }

		public static void Run(Func<Task> taskFunc)
		{
			var instance = new SerializeAsyncContext();
			Interlocked.Increment(ref instance.pendingOperations);
			var executionTask = instance.taskFactory.StartNew(taskFunc,TaskCreationOptions.DenyChildAttach).Unwrap()
				.ContinueWith(t =>
				{
					Interlocked.Decrement(ref instance.pendingOperations);
					t.GetAwaiter().GetResult();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Default);

			var originalContext = SynchronizationContext.Current;
			try
			{
				SynchronizationContext.SetSynchronizationContext(instance.enqueueTasksContext);
				while (instance.pendingOperations > 0)
				{
					Task task;
					if (!instance.executionQueue.TryDequeue(out task))
						continue;

					instance.scheduler.ExecuteTask(task);

					if (task.IsFaulted && 
						task.Exception != null) //precaution
					{
						throw task.Exception;
					}
				}
				executionTask.GetAwaiter().GetResult();
			}
			catch (Exception e)
			{
				log.Error("Async task exception. {0}", e);
				throw;
			}
			finally
			{
				SynchronizationContext.SetSynchronizationContext(originalContext);
			}
		}

		public static T Run<T>(Func<Task<T>> taskFunc)
		{
			var instance = new SerializeAsyncContext();
			Interlocked.Increment(ref instance.pendingOperations);
			var executionTask = instance.taskFactory.StartNew(taskFunc,TaskCreationOptions.DenyChildAttach).Unwrap()
				.ContinueWith(t =>
				{
					Interlocked.Decrement(ref instance.pendingOperations);
					return t.GetAwaiter().GetResult();
				},CancellationToken.None,
				  TaskContinuationOptions.ExecuteSynchronously,
				  TaskScheduler.Default);

			var originalContext = SynchronizationContext.Current;
			try
			{
				SynchronizationContext.SetSynchronizationContext(instance.enqueueTasksContext);
				while(instance.pendingOperations > 0)
				{
					Task task;
					if(!instance.executionQueue.TryDequeue(out task))
						continue;

					instance.scheduler.ExecuteTask(task); //after this more operations can be appended to execution queue

					if (task.IsFaulted &&
						task.Exception != null) //precaution
					{
						throw task.Exception;
					}

				}

				return executionTask.GetAwaiter().GetResult();
			}
			catch (Exception e)
			{
				log.Error("Async task exception. {0}", e);
				throw;
			}
			finally
			{
				SynchronizationContext.SetSynchronizationContext(originalContext);
			}

		}

		private class SerialSynchronizationContext : SynchronizationContext
		{
			private readonly SerializeAsyncContext parent;
			public SerialSynchronizationContext(SerializeAsyncContext parent)
			{
				this.parent = parent;
			}

			public override void OperationStarted()
			{
				Interlocked.Increment(ref parent.pendingOperations);
			}

			public override void OperationCompleted()
			{
				Interlocked.Decrement(ref parent.pendingOperations);
			}

			public override void Send(SendOrPostCallback d, object state)
			{
				d(state);
			}

			public override void Post(SendOrPostCallback d, object state)
			{
				Interlocked.Increment(ref parent.pendingOperations);
				var task = parent.taskFactory.StartNew(() =>
					d(state), TaskCreationOptions.DenyChildAttach);
				task.ContinueWith(t => Interlocked.Decrement(ref parent.pendingOperations),
					CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously,
					TaskScheduler.Default);

				parent.executionQueue.Enqueue(task);
			}

			public override SynchronizationContext CreateCopy()
			{
				return this;
			}
		}

		private class QueueTaskScheduler : TaskScheduler
		{
			private readonly SerializeAsyncContext parent;

			public QueueTaskScheduler(SerializeAsyncContext parent)
			{
				this.parent = parent;
			}

			public override int MaximumConcurrencyLevel
			{
				get { return 1; }
			}

			protected override void QueueTask(Task task)
			{
				Interlocked.Increment(ref parent.pendingOperations);
				task.ContinueWith(t => Interlocked.Decrement(ref parent.pendingOperations),
					CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.DenyChildAttach,
					TaskScheduler.Default);

				parent.executionQueue.Enqueue(task);
			}

			protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
			{
				return TryExecuteTask(task);
			}

			public void ExecuteTask(Task task)
			{
				TryExecuteTask(task);
			}

			protected override IEnumerable<Task> GetScheduledTasks()
			{
				return parent.executionQueue.ToArray();
			}
		}

		private class LinkedListQueue<T> : IEnumerable<T>
		{
			private readonly LinkedList<T> queueStorage = new LinkedList<T>();

			public bool TryDequeue(out T value)
			{
				lock (queueStorage)
				{
					if (queueStorage.Count == 0)
					{
						value = default(T);
						return false;
					}

					value = queueStorage.Last.Value;
					queueStorage.RemoveLast();
					return true;
				}
			}

			public void Enqueue(T value)
			{
				lock (queueStorage)
					queueStorage.AddFirst(value);
			}

			public int Count
			{
				get
				{
					lock (queueStorage)
						return queueStorage.Count;
				}
			}

			public IEnumerator<T> GetEnumerator()
			{
				lock (queueStorage) //"snapshot" logic
					return queueStorage.ToList().GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return GetEnumerator();
			}
		}
	}
}
