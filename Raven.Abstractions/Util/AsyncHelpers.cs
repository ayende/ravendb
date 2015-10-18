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
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;

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
