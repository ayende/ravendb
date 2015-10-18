using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Xunit;

namespace Raven.Tests.Utils
{
	public class SerializedAsyncContextTests
	{
		private async Task VoidAsyncMethod(ManualResetEventSlim mre)
		{
			await Task.Delay(100);
			mre.Set();
		}

		private async Task<int> AsyncMethod(int number)
		{
			await Task.Delay(100);
			return number;
		}

		private async Task<int> AsyncMethodWithException()
		{
			await Task.Delay(100);
			throw new ApplicationException("This is a test exception");
		}

		private async Task AsyncMethodWithInternalException()
		{
			await AsyncMethodWithException();
		}

		[Fact(Timeout = 1000)] 
		public void Run_for_void_task_should_work()
		{
			var mre = new ManualResetEventSlim();
			SerializedAsyncContext.Run(() => VoidAsyncMethod(mre));

			Assert.True(mre.IsSet);
		}

		[Fact(Timeout = 1000)]
		public void Run_for_int_task_should_work()
		{
			Assert.Equal(4321, SerializedAsyncContext.Run(() => AsyncMethod(4321)));
		}

		[Fact(Timeout = 1000)]
		public void Run_for_exception_throwing_task_should_propagate_exception1()
		{
			Assert.Throws<ApplicationException>(() => SerializedAsyncContext.Run(AsyncMethodWithException));
		}

		[Fact(Timeout = 1000)]
		public void Run_for_exception_throwing_task_should_propagate_exception2()
		{
			Assert.Throws<ApplicationException>(() => SerializedAsyncContext.Run(AsyncMethodWithInternalException));
		}
	}
}
