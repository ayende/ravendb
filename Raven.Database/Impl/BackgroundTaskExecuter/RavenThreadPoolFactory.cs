// -----------------------------------------------------------------------
//  <copyright file="RavenThreadPoolFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Database.Config;

namespace Raven.Database.Impl.BackgroundTaskExecuter
{
    public abstract class RavenThreadPoolFactory
    {
        public static readonly RavenThreadPoolFactory Default = new DefaultRavenThreadPoolFactory();

        public abstract RavenThreadPool Create(InMemoryRavenConfiguration configuration, CancellationToken ct, DocumentDatabase database, string name, IReadOnlyList<Action> longRunningActions);

        private class DefaultRavenThreadPoolFactory : RavenThreadPoolFactory
        {
            public override RavenThreadPool Create(InMemoryRavenConfiguration configuration, CancellationToken ct, DocumentDatabase database, string name, IReadOnlyList<Action> longRunningActions)
            {
                // The multiplier is from DocumentDatabase which didn't respect the users configuration, but I copied to across to preserve behaviour
                return new DefaultRavenThreadPool(configuration.MaxNumberOfParallelProcessingTasks * 2, ct, database, name, longRunningActions);
            }
        }
    }
}