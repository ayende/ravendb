//-----------------------------------------------------------------------
// <copyright file="IndexCreation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Client.Document;
#if !NET_3_5
using System.ComponentModel.Composition.Hosting;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Extensions;

#endif

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Helper class for creating indexes from implementations of <see cref="AbstractIndexCreationTask"/>.
	/// </summary>
	public static class IndexCreation
	{
#if !NET_3_5

#if !SILVERLIGHT
		/// <summary>
		/// Creates the indexes found in the specified assembly.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			CreateIndexes(catalog, documentStore);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions)
		{
			var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();
			foreach (var task in tasks)
			{
				task.Execute(databaseCommands, conventions);
			}
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to get indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			CreateIndexes(catalogToGetnIndexingTasksFrom, documentStore.DatabaseCommands, documentStore.Conventions);
		}

        /// <summary>
        /// Creates the indexes found in the specified catalog in multiple databases
        /// </summary>
        /// <param name="assemblyToScanForIndexingTasks">The catalog to get indexing tasks from.</param>
        /// <param name="documentStore">The document store.</param>
        /// <param name="databases">Identifiers of the databases on which the indexes should be created on</param>
        /// <param name="shouldIgnoreNotExistingDatabase">Ignores not existing databases when set to true</param>
        public static void CreateIndexesForDatabases(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore, string[] databases, bool shouldIgnoreNotExistingDatabase = false)
        {
            var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
            foreach (var database in databases)
            {
                bool exists = true;
                try
                {
                    documentStore.DatabaseCommands.EnsureDatabaseExists(database);
                }
                catch (Exception)
                {
                    if (!shouldIgnoreNotExistingDatabase)
                        throw new InvalidOperationException(string.Format("Cannot create indexes for database '{0}' because it doesn't exist.", database));
                    exists = false;
                }

                if (exists)
                {
                    CreateIndexes(catalog, documentStore.DatabaseCommands.ForDatabase(database), documentStore.Conventions);
                }
            }
        }
#endif

		/// <summary>
		/// Creates the indexes found in the specified assembly.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static Task CreateIndexesAsync(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
		{
			var catalog = new CompositionContainer(new AssemblyCatalog(assemblyToScanForIndexingTasks));
			return CreateIndexesAsync(catalog, documentStore);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to getn indexing tasks from.</param>
		/// <param name="documentStore">The document store.</param>
		public static Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore)
		{
			return CreateIndexesAsync(catalogToGetnIndexingTasksFrom, documentStore.AsyncDatabaseCommands,
			                          documentStore.Conventions);
		}

		/// <summary>
		/// Creates the indexes found in the specified catalog
		/// </summary>
		/// <param name="catalogToGetnIndexingTasksFrom">The catalog to getn indexing tasks from.</param>
		public static Task CreateIndexesAsync(ExportProvider catalogToGetnIndexingTasksFrom, IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention conventions)
		{
			var tasks = catalogToGetnIndexingTasksFrom.GetExportedValues<AbstractIndexCreationTask>();

			Task[] array = tasks.Select(task => task.ExecuteAsync(asyncDatabaseCommands, conventions)).ToArray();
			var indexesAsync = new Task(() => Task.WaitAll(array));
			indexesAsync.Start();
			return indexesAsync;
		}

#else //NET_3_5

		/// <summary>
		/// Creates the indexes found in the specified assembly.
		/// </summary>
		/// <param name="assemblyToScanForIndexingTasks">The assembly to scan for indexing tasks.</param>
		/// <param name="documentStore">The document store.</param>
		public static void CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore)
		{
			var tasks = assemblyToScanForIndexingTasks.GetTypes()
				.Where(x => typeof (AbstractIndexCreationTask).IsAssignableFrom(x))
				.Where(x => !x.IsAbstract)
				.Select(x => (AbstractIndexCreationTask)System.Activator.CreateInstance(x))
				.ToArray();

			foreach (var task in tasks)
			{
				task.Execute(documentStore.DatabaseCommands, documentStore.Conventions);
			}
		}
#endif
	}
}