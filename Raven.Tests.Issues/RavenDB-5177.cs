// -----------------------------------------------------------------------
//  <copyright file="RavenDB-5177.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl.BackgroundTaskExecuter;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5177 : RavenTest
    {
        [Fact]
        public void CustomThreadPoolFactoryTypeCreatesThreadPool()
        {
            var threadPoolFactoryTypeValue = typeof (CustomThreadPoolFactory).AssemblyQualifiedName;
            var inMemoryConfiguration = new InMemoryRavenConfiguration();

            inMemoryConfiguration.Settings["Raven/ThreadPoolFactoryType"] = threadPoolFactoryTypeValue;
            inMemoryConfiguration.Initialize();

            var threadPoolFactoryType = inMemoryConfiguration.ThreadPoolFactoryType;

            Assert.Equal(threadPoolFactoryType, threadPoolFactoryTypeValue);

            var threadPool = inMemoryConfiguration.CreateThreadPool(CancellationToken.None, null);

            Assert.Equal(typeof(CustomThreadPoolFactory.CustomThreadPool), threadPool.GetType());
        }

        private class CustomThreadPoolFactory : RavenThreadPoolFactory
        {
            public override RavenThreadPool Create(InMemoryRavenConfiguration configuration, CancellationToken ct, DocumentDatabase database, string name, IReadOnlyList<Action> longRunningActions)
            {
                return new CustomThreadPool();
            }

            public class CustomThreadPool : RavenThreadPool
            {
                public override string Name
                {
                    get { throw new NotImplementedException(); }
                }

                public override int RunningTasksAmount
                {
                    get { throw new NotImplementedException(); }
                }

                public override int WaitingTasksAmount
                {
                    get { throw new NotImplementedException(); }
                }

                public override void HandleHighCpuUsage()
                {
                    throw new NotImplementedException();
                }

                public override void HandleLowCpuUsage()
                {
                    throw new NotImplementedException();
                }

                public override ThreadsSummary GetThreadPoolStats()
                {
                    throw new NotImplementedException();
                }

                public override IEnumerable<object> GetRunningTaskDescriptions()
                {
                    throw new NotImplementedException();
                }

                public override IEnumerable<object> GetAllWaitingTaskDescriptions()
                {
                    throw new NotImplementedException();
                }

                public override RavenThreadPool Start()
                {
                    throw new NotImplementedException();
                }

                public override void DrainThePendingTasks()
                {
                    throw new NotImplementedException();
                }

                public override void ExecuteBatch<T>(IList<T> src, Action<T> action, string description = null, bool allowPartialBatchResumption = false, int completedMultiplier = 2, int freeThreadsMultiplier = 2, int maxWaitMultiplier = 1)
                {
                    throw new NotImplementedException();
                }

                public override void ExecuteBatch<T>(IList<T> src, Action<IEnumerator<T>> action, int pageSize = DefaultPageSize, string description = null)
                {
                    throw new NotImplementedException();
                }
            }
        }
    }
}