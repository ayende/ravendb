﻿// -----------------------------------------------------------------------
//  <copyright file="CleanupTestIndexesTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Plugins;

namespace Raven.Database.Tasks
{
	public class CleanupTestIndexesTask : IStartupTask, IDisposable
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private Timer checkTimer;

		private DocumentDatabase database;

		public void Execute(DocumentDatabase db)
		{
			database = db;
			checkTimer = new Timer(ExecuteCleanup, null, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(5));
		}

		private void ExecuteCleanup(object state)
		{
			var indexNames = database.IndexDefinitionStorage.IndexNames;
			foreach (var indexName in indexNames)
			{
				try
				{
					if (indexName.StartsWith(Constants.TestIndexPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
						continue;

					var lastQueryTime = database.IndexStorage.GetLastQueryTime(indexName);
					var shouldRemove = (lastQueryTime == null || (SystemTime.UtcNow - lastQueryTime.Value).TotalMinutes > 15);

					if (shouldRemove)
						database.Indexes.DeleteIndex(indexName);
				}
				catch (Exception e)
				{
					log.WarnException(string.Format("Could not delete index '{0}'.", indexName), e);
				}
			}
		}

		public void Dispose()
		{
			if (checkTimer != null)
				checkTimer.Dispose();
		}
	}
}