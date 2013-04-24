﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using System.Threading.Tasks;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public static class InvocationExtensions
	{
		public static Task ContinueOnSuccess<T>(this Task<T> parent, Action<T> action)
		{
			return parent.ContinueWith(task =>
			{
				if(task.IsCanceled)
					return;
				action(task.Result);
			});
		}

		public static Task<bool> ContinueWhenTrue(this Task<bool> parent, Action action)
		{
			return parent.ContinueWith(task =>
			                           	{
			                           		if (task.Result == false)
			                           			return false;
			                           		action();
			                           		return true;
			                           	});
		}

        public static Task<T> ContinueWhenTrue<T>(this Task<bool> parent, Func<T> action)
        {
            return parent.ContinueWith(task =>
            {
                if (task.Result == false)
                    return default(T);
                return action();
            });
        }

		public static Task<bool> ContinueWhenTrueInTheUIThread(this Task<bool> parent, Action action)
		{
		    return parent.ContinueWhenTrue((Action) (() => Execute.OnTheUI(action)));
		}

		public static Task<TResult> ContinueOnSuccess<T, TResult>(this Task<T> parent, Func<T, TResult> action)
		{
			return parent.ContinueWith(task =>
			{
				if (task.IsCanceled)
				{
					var tcs = new TaskCompletionSource<TResult>();
					tcs.SetCanceled();
					return tcs.Task;
				}
				return new CompletedTask<TResult>(action(task.Result));
			}).Unwrap();
		}

		public static Task ContinueOnSuccess(this Task parent, Action action)
		{
			return parent.ContinueWith(task => task.IsFaulted ? task : TaskEx.Run(action)).Unwrap();
		}

        public static Task ContinueOnUIThread(this Task parent, Action<Task> action)
        {
            return parent.ContinueWith(action, Schedulers.UIScheduler);
        }

        public static Task ContinueOnUIThread<T>(this Task<T> parent, Action<Task<T>> action)
        {
            return parent.ContinueWith(action, Schedulers.UIScheduler);
        }

		public static Task ContinueOnSuccessInTheUIThread(this Task parent, Action action)
		{
			return parent.ContinueOnSuccess(() => Execute.OnTheUI(action));
		}

		public static Task ContinueOnSuccessInTheUIThread<T>(this Task<T> parent, Action<T> action)
		{
			return parent.ContinueOnSuccess(result => Execute.OnTheUI(() => action(result)));
		}

		public static Task ContinueOnSuccess(this Task parent, Func<Task> action)
		{
			return parent.ContinueWith(task => task.IsFaulted ? task : action()).Unwrap();
		}

		public static Task Finally(this Task task, Action action)
		{
			return task.ContinueWith(t => action());
		}

		public static Task FinallyInTheUIThread(this Task task, Action action)
		{
			return task.ContinueWith(t => Execute.OnTheUI(action));
		}

		public static Task<TResult> Catch<TResult>(this Task<TResult> parent)
		{
			return parent.Catch(e => { });
		}

		public static Task<TResult> Catch<TResult>(this Task<TResult> parent, Action<AggregateException> action)
		{
			var stackTrace = new StackTrace();
			return parent.ContinueWith(task =>
			{
				if (task.IsFaulted == false)
					return task;

				var ex = task.Exception.ExtractSingleInnerException();
                Execute.OnTheUI(() => ApplicationModel.Current.AddErrorNotification(ex, null, stackTrace))
					.ContinueWith(_ => action(task.Exception));
				return task;
			}).Unwrap();
		}

		public static Task Catch(this Task parent)
		{
			return parent.Catch(e => { });
		}

		public static Task Catch(this Task parent, Action<AggregateException> action)
		{
			return parent.Catch(e =>
			{
				action(e);
				return false;
			});
		}

		public static Task Catch(this Task parent, Func<AggregateException, bool> func)
		{
			var stackTrace = new StackTrace();
			return parent.ContinueWith(task =>
			{
				if (task.IsFaulted == false)
					return;

				var ex = task.Exception.ExtractSingleInnerException();
				Execute.OnTheUI(() =>
				{
					if(func(task.Exception) == false)
						ApplicationModel.Current.AddErrorNotification(ex, null, stackTrace);
				});
			});
		}

		public static Task CatchIgnore<TException>(this Task parent, Action<TException> action) where TException : Exception
		{
			return parent.ContinueWith(task =>
			                           	{
			                           		if (task.IsFaulted == false)
			                           			return task;

											var ex = task.Exception.ExtractSingleInnerException() as TException;
											if (ex == null)
                                                return Execute.EmptyResult<object>();

											Execute.OnTheUI(() => action(ex));
			                           		return Execute.EmptyResult<object>();
			                           	})
				.Unwrap();
		}

		public static Task CatchIgnore<TException>(this Task parent, Action action) where TException : Exception
		{
			return parent.CatchIgnore<TException>(ex => action());
		}

        public static Task CatchIgnore(this Task parent)
        {
            return parent.CatchIgnore<Exception>(ex => { });
        }

		public static Task ProcessTasks(this IEnumerable<Task> tasks)
		{
			var enumerator = tasks.GetEnumerator();
			if (enumerator.MoveNext() == false)
			{
				enumerator.Dispose(); 
				return null;
			}
			return ProcessTasks(enumerator);
		}

		private static Task ProcessTasks(IEnumerator<Task> enumerator)
		{
			return enumerator.Current
				.ContinueWith(task =>
				              {
				              	task.Wait(); // would throw on error
				              	if (enumerator.MoveNext() == false)
				              	{
				              		enumerator.Dispose();
				              		return task;
				              	}
				              	return ProcessTasks(enumerator);
				              }).Unwrap();
		}
	}
}