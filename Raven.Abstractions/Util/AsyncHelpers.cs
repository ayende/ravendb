// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
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
			SerializedAsyncContext.Run(task);
		}

		public static T RunSync<T>(Func<Task<T>> task)
		{
			return SerializedAsyncContext.Run(task);
		}		
	}
}