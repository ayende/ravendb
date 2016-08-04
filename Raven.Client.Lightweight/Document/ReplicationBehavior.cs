// -----------------------------------------------------------------------
//  <copyright file="ReplicationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document
{
    using Raven.Abstractions.Connection;

    public class ReplicationBehavior
    {
        private readonly DocumentStore documentStore;

#if !DNXCORE50
        private readonly static ILog log = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog log = LogManager.GetLogger(typeof(ReplicationBehavior));
#endif

        public ReplicationBehavior(DocumentStore documentStore)
        {
            this.documentStore = documentStore;
        }

        /// <summary>
        /// Represents an replication operation to all destination servers of an item specified by ETag
        /// </summary>
        /// <param name="etag">ETag of an replicated item</param>
        /// <param name="timeout">Optional timeout - by default, 30 seconds</param>
        /// <param name="database">The database from which to check, if null, the default database for the document store connection string</param>
        /// <param name="replicas">The min number of replicas that must have the value before we can return (or the number of destinations, if higher)</param>
        /// <returns>Task which will have the number of nodes that the caught up to the specified etag</returns>
        public async Task WaitAsync(Etag etag = null, TimeSpan? timeout = null, string database = null, int replicas = 2)
        {
            etag = etag ?? documentStore.LastEtagHolder.GetLastWrittenEtag();
            if (etag == Etag.Empty || etag == null)
                return; // if the etag is empty, nothing to do

            timeout = timeout ?? TimeSpan.FromSeconds(60);

            var asyncDatabaseCommands = (AsyncServerClient)documentStore.AsyncDatabaseCommands;
            database = database ?? documentStore.DefaultDatabase;
            asyncDatabaseCommands = (AsyncServerClient)asyncDatabaseCommands.ForDatabase(database);

            asyncDatabaseCommands.ForceReadFromMaster();

            await asyncDatabaseCommands.ExecuteWithReplication(HttpMethods.Get,
                (operationMetadata, requestTimeMetric) => asyncDatabaseCommands.WithWriteAssurance(operationMetadata, requestTimeMetric, etag, timeout, replicas)).ConfigureAwait(false);

        }
    }
}
