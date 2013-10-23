//-----------------------------------------------------------------------
// <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class TasksStorageActions : ITasksStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public TasksStorageActions(TableStorage storage, IUuidGenerator generator)
		{
			this.storage = storage;
			this.generator = generator;
		}

		public void AddTask(Task task, DateTime addedAt)
		{
			storage.Tasks.Put(new RavenJObject
			{
				{"index", task.Index},
				{"id", generator.CreateSequentialUuid(UuidType.Tasks).ToByteArray()},
				{"time", addedAt},
				{"type", task.GetType().FullName},
			}, task.AsBytes());
		}

		public bool HasTasks
		{
			get { return ApproximateTaskCount > 0; }
		}

		public long ApproximateTaskCount
		{
			get { return storage.Tasks.Count; }
		}

		public T GetMergedTask<T>() where T : Task
		{
			foreach (var readResult in storage.Tasks)
			{
				var taskType = readResult.Key.Value<string>("type");
                if (taskType != typeof(T).FullName)
					continue;

				Task task;
				try
				{
					task = Task.ToTask(taskType, readResult.Data());
				}
				catch (Exception e)
				{
					logger.ErrorException(
						string.Format("Could not create instance of a task: {0}", readResult.Key),
						e);
					continue;
				}
				MergeSimilarTasks(task, readResult.Key.Value<byte[]>("id"));
				storage.Tasks.Remove(readResult.Key);
				return (T)task;
			}
			return null;
		}

		public IEnumerable<TaskMetadata> GetPendingTasksForDebug()
		{
			return storage.Tasks.Select(readResult => new TaskMetadata
			                                          {
				                                          Id = Etag.Parse(readResult.Key.Value<byte[]>("id")),
				                                          AddedTime = readResult.Key.Value<DateTime>("time"),
				                                          Type = readResult.Key.Value<string>("type"),
				                                          Index = readResult.Key.Value<string>("index")
			                                          });
		}

		private void MergeSimilarTasks(Task task, byte [] taskId)
		{
			var taskType = task.GetType().FullName;

			int totalTaskCount = 0;
            foreach (var keyForTaskToTryMerging in KeyForTaskToTryMergings(task, taskType, new Guid(taskId)))
			{
				var readResult = storage.Tasks.Read(keyForTaskToTryMerging);
                if (readResult == null)
					continue;
				Task existingTask;
				try
				{
					existingTask = Task.ToTask(readResult.Key.Value<string>("type"), readResult.Data());
				}
				catch (Exception e)
				{
					logger.ErrorException(
						string.Format("Could not create instance of a task: {0}", readResult.Key),
						e);
					storage.Tasks.Remove(keyForTaskToTryMerging);
					continue;
				}

				task.Merge(existingTask);

				storage.Tasks.Remove(keyForTaskToTryMerging);
				if (totalTaskCount++ > 1024)
					break;
			}
		}

        private IEnumerable<RavenJToken> KeyForTaskToTryMergings(Task task, string taskType, Guid taskId)
        {
            if (task.SeparateTasksByIndex == false)
            {
                return storage.Tasks["ByIndexAndType"].SkipTo(new RavenJObject
                {
                    {"index", ""}, // the very start
                    {"type", ""},
                })
                      .Where(x => new Guid(x.Value<byte[]>("id")) != taskId)
                      .TakeWhile(x =>
                                                                 StringComparer.OrdinalIgnoreCase.Equals(
                                                                     x.Value<string>("type"), taskType)
                    );

            }

            return storage.Tasks["ByIndexAndType"].SkipTo(new RavenJObject
	        {
	            {"index", task.Index},
	            {"type", taskType},
	        })
				.Where(x => new Guid(x.Value<byte[]>("id")) != taskId)
               	.TakeWhile(x =>
                	StringComparer.OrdinalIgnoreCase.Equals(x.Value<string>("index"), task.Index) && 
					StringComparer.OrdinalIgnoreCase.Equals(x.Value<string>("type"), taskType)
                );
        }
	}
}
