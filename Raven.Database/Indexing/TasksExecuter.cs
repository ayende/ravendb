﻿//-----------------------------------------------------------------------
// <copyright file="TasksExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using NLog;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class TasksExecuter
	{
		private readonly WorkContext context;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly ITransactionalStorage transactionalStorage;

		public TasksExecuter(ITransactionalStorage transactionalStorage, WorkContext context)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
		}

		int workCounter;
		
		public void Execute()
		{
			while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork = ExecuteTasks();
				}
				catch (Exception e)
				{
					log.ErrorException("Failed to execute indexing", e);
				}
				if (foundWork == false)
				{
					context.WaitForWork(TimeSpan.FromHours(1), ref workCounter);
				}
			}
		}


		private bool ExecuteTasks()
		{
			bool foundWork = false;
			transactionalStorage.Batch(actions =>
			{
				int tasks;
				Task task = actions.Tasks.GetMergedTask(out tasks);
				if (task == null)
					return;

				log.Debug("Executing {0}", task);
				foundWork = true;

				try
				{
					task.Execute(context);
				}
				catch (Exception e)
				{
					log.WarnException(
						string.Format("Task {0} has failed and was deleted without completing any work", task),
						e);
				}
			});
			return foundWork;
		}

	}
}