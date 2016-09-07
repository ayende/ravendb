// -----------------------------------------------------------------------
//  <copyright file="RavenThreadPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Database.Config;

namespace Raven.Database.Impl.BackgroundTaskExecuter
{
    public abstract class RavenThreadPool : IDisposable, ICpuUsageHandler
    {
        public const int DefaultPageSize = 1024;
        public abstract string Name { get; }
        public abstract int RunningTasksAmount { get; }
        public abstract int WaitingTasksAmount { get; }
        public abstract void HandleHighCpuUsage();
        public abstract void HandleLowCpuUsage();

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public abstract ThreadsSummary GetThreadPoolStats();
        public abstract ThreadTask[] GetRunningTasks();
        public abstract ThreadTask[] GetAllWaitingTasks();
        public abstract RavenThreadPool Start();
        public abstract void DrainThePendingTasks();

        public abstract void ExecuteBatch<T>(IList<T> src, Action<T> action, string description = null,
            bool allowPartialBatchResumption = false, int completedMultiplier = 2, int freeThreadsMultiplier = 2, int maxWaitMultiplier = 1);

        public abstract void ExecuteBatch<T>(IList<T> src, Action<IEnumerator<T>> action,
            int pageSize = DefaultPageSize, string description = null);

        protected virtual void Dispose(bool disposing)
        {
        }

        public class ThreadsSummary
        {
            public ConcurrentDictionary<ThreadPriority, int> ThreadsPrioritiesCounts { get; set; }
            public int UnstoppableThreadsCount { get; set; }
            public int PartialMaxWait { get; set; }
            public int FreeThreadsAmount { get; set; }
            public int ConcurrentEventsCount { get; set; }
            public int ConcurrentWorkingThreadsAmount { get; set; }
        }

        public class ThreadTask
        {
            public Action Action;
            public BatchStatistics BatchStats;
            public object Description;
            public CountdownEvent DoneEvent;
            public Stopwatch Duration;
            public bool EarlyBreak;
            public DateTime QueuedAt;
        }

        public class BatchStatistics
        {
            public enum BatchType
            {
                Atomic,
                Range
            }

            public int Completed;
            public int Total;
            public BatchType Type;
        }
    }
}