﻿using System;

using Lucene.Net.Index;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class ErrorLoggingConcurrentMergeScheduler : ConcurrentMergeScheduler
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(ErrorLoggingConcurrentMergeScheduler));

        protected override void HandleMergeException(Exception exc)
        {
            try
            {
                base.HandleMergeException(exc);
            }
            catch (Exception e)
            {
                Log.WarnException("Concurrent merge failed", e);
            }
        }
    }
}