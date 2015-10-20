// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util
{
	public static class AsyncHelpers
	{
		public static void RunSync(Func<Task> task)
		{
			SerializeAsyncContext.Run(task);
		}

		public static T RunSync<T>(Func<Task<T>> task)
		{
			return SerializeAsyncContext.Run(task);
		}	
	}
}
