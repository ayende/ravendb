﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Task = Raven.Database.Tasks.Task;

namespace Raven.Database.Indexing
{

	public class ReducingExecuter : AbstractIndexingExecuter
	{
		public ReducingExecuter(WorkContext context)
			: base(context)
		{
			autoTuner = new ReduceBatchSizeAutoTuner(context);
		}

		protected void HandleReduceForIndex(IndexToWorkOn indexToWorkOn)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexToWorkOn.IndexName);
			if (viewGenerator == null)
				return;

			TimeSpan reduceDuration = TimeSpan.Zero;
			int totalCount = 0;
			int totalSize = 0;
			bool operationCanceled = false;
			var itemsToDelete = new List<object>();

			IList<ReduceTypePerKey> mappedResultsInfo = null;
			transactionalStorage.Batch(actions =>
			{
				mappedResultsInfo = actions.MapReduce.GetReduceTypesPerKeys(indexToWorkOn.IndexName, 
					context.CurrentNumberOfItemsToReduceInSingleBatch,
					context.NumberOfItemsToExecuteReduceInSingleStep).ToList();
			});

			var singleStepReduceKeys = mappedResultsInfo.Where(x => x.OperationTypeToPerform == ReduceType.SingleStep).Select(x => x.ReduceKey).ToArray();
			var multiStepsReduceKeys = mappedResultsInfo.Where(x => x.OperationTypeToPerform == ReduceType.MultiStep).Select(x => x.ReduceKey).ToArray();

			var sw = Stopwatch.StartNew();

			try
			{
				if (singleStepReduceKeys.Length > 0)
				{
					var reduceCounters = SingleStepReduce(indexToWorkOn, singleStepReduceKeys, viewGenerator, itemsToDelete);
					totalCount += reduceCounters.count;
					totalSize += reduceCounters.size;
				}

				if (multiStepsReduceKeys.Length > 0)
				{
					var reduceCounters = MultiStepReduce(indexToWorkOn, multiStepsReduceKeys, viewGenerator, itemsToDelete);
					totalCount += reduceCounters.count;
					totalSize += reduceCounters.size;
				}

				reduceDuration = sw.Elapsed;
			}
			catch (OperationCanceledException)
			{
				operationCanceled = true;
			}
			finally
			{
				if (operationCanceled == false)
				{
					// whatever we succeeded in indexing or not, we have to update this
					// because otherwise we keep trying to re-index failed mapped results
					transactionalStorage.Batch(actions =>
					{
						var latest = actions.MapReduce.DeleteScheduledReduction(itemsToDelete);

						if(latest == null)
							return;
						actions.Indexing.UpdateLastReduced(indexToWorkOn.IndexName, latest.Etag, latest.Timestamp);
					});
					autoTuner.AutoThrottleBatchSize(totalCount, totalSize, reduceDuration);
				}
			}
		}

		private ReduceResultStats MultiStepReduce(IndexToWorkOn index, string[] keysToReduce, AbstractViewGenerator viewGenerator, List<object> itemsToDelete)
		{
			var result = new ReduceResultStats();
			var needToMoveToMultiStep = new HashSet<string>();
			transactionalStorage.Batch(actions =>
			{
				foreach (var localReduceKey in keysToReduce)
				{
					var lastPerformedReduceType = actions.MapReduce.GetLastPerformedReduceType(index.IndexName, localReduceKey);

					if (lastPerformedReduceType != ReduceType.MultiStep)
						needToMoveToMultiStep.Add(localReduceKey);

					if (lastPerformedReduceType != ReduceType.SingleStep)
						return;
					// we exceeded the limit of items to reduce in single step
					// now we need to scheduce reductions at level 0 for all map results with given reduce key
					var mappedItems = actions.MapReduce.GetMappedBuckets(index.IndexName, localReduceKey).ToList();
					actions.MapReduce.ScheduleReductions(index.IndexName, 0,
					                                     mappedItems.Select(x => new ReduceKeyAndBucket(x, localReduceKey)));
				}
			});

			for (int i = 0; i < 3; i++)
			{
				var level = i;

				transactionalStorage.Batch(actions =>
				{
					context.CancellationToken.ThrowIfCancellationRequested();

					var persistedResults = actions.MapReduce.GetItemsToReduce
						(
							level: level,
							reduceKeys: keysToReduce,
							index: index.IndexName,
							itemsToDelete: itemsToDelete,
							loadData: true
						).ToList();

					var sp = Stopwatch.StartNew();

					result.count += persistedResults.Count;
					result.size += persistedResults.Sum(x => x.Size);

					if (Log.IsDebugEnabled)
					{
						if (persistedResults.Count > 0)
							Log.Debug(() => string.Format("Found {0} results for keys [{1}] for index {2} at level {3} in {4}",
							                              persistedResults.Count,
							                              string.Join(", ", persistedResults.Select(x => x.ReduceKey).Distinct()),
							                              index.IndexName, level, sp.Elapsed));
						else
							Log.Debug("No reduce keys found for {0}", index.IndexName);
					}

					context.CancellationToken.ThrowIfCancellationRequested();

					var requiredReduceNextTime = persistedResults.Select(x => new ReduceKeyAndBucket(x.Bucket, x.ReduceKey))
					                                             .OrderBy(x => x.Bucket)
					                                             .Distinct()
					                                             .ToArray();
					foreach (var mappedResultInfo in requiredReduceNextTime)
					{
						actions.MapReduce.RemoveReduceResults(index.IndexName, level + 1, mappedResultInfo.ReduceKey,
						                                      mappedResultInfo.Bucket);
					}

					if (level != 2)
					{
						var reduceKeysAndBuckets = requiredReduceNextTime
							.Select(x => new ReduceKeyAndBucket(x.Bucket/1024, x.ReduceKey))
							.Distinct()
							.ToArray();
						actions.MapReduce.ScheduleReductions(index.IndexName, level + 1, reduceKeysAndBuckets);
					}

					var results = persistedResults
						.Where(x => x.Data != null)
						.GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data))
						.ToArray();
					var reduceKeys = new HashSet<string>(persistedResults.Select(x => x.ReduceKey),
					                                     StringComparer.InvariantCultureIgnoreCase);
					context.ReducedPerSecIncreaseBy(results.Length);

					context.CancellationToken.ThrowIfCancellationRequested();
					sp = Stopwatch.StartNew();
					context.IndexStorage.Reduce(index.IndexName, viewGenerator, results, level, context, actions, reduceKeys);
					Log.Debug("Indexed {0} reduce keys in {1} with {2} results for index {3} in {4}", reduceKeys.Count, sp.Elapsed,
					          results.Length, index.IndexName, sp.Elapsed);
				});
			}

			foreach (var reduceKey in needToMoveToMultiStep)
			{
				string localReduceKey = reduceKey;
				transactionalStorage.Batch(actions =>
				                           actions.MapReduce.UpdatePerformedReduceType(index.IndexName, localReduceKey,
				                                                                       ReduceType.MultiStep));
			}

			return result;
		}

		private ReduceResultStats SingleStepReduce(IndexToWorkOn index, string[] keysToReduce, AbstractViewGenerator viewGenerator,
		                                        List<object> itemsToDelete)
		{
			var result = new ReduceResultStats();
			var needToMoveToSingleStep = new HashSet<string>();

			Log.Debug(() => string.Format("Executing single step reducing for {0} keys [{1}]", keysToReduce.Length, string.Join(", ", keysToReduce)));
			transactionalStorage.Batch(actions =>
			{
				var scheduledItems = actions.MapReduce.GetItemsToReduce
						(
							level: 0,
							reduceKeys: keysToReduce,
							index: index.IndexName,
							itemsToDelete: itemsToDelete,
							loadData: false
						).ToList();

				// Only look at the scheduled batch for this run, not the entire set of pending reductions.
				//var batchKeys = scheduledItems.Select(x => x.ReduceKey).ToArray();

				foreach (var reduceKey in keysToReduce)
				{
					var lastPerformedReduceType = actions.MapReduce.GetLastPerformedReduceType(index.IndexName, reduceKey);

					if (lastPerformedReduceType != ReduceType.SingleStep)
						needToMoveToSingleStep.Add(reduceKey);

					if (lastPerformedReduceType != ReduceType.MultiStep)
						continue;

					Log.Debug("Key {0} was moved from multi step to single step reduce, removing existing reduce results records",
						reduceKey);

					// now we are in single step but previously multi step reduce was performed for the given key
					var mappedBuckets = actions.MapReduce.GetMappedBuckets(index.IndexName, reduceKey).ToList();

					// add scheduled items too to be sure we will delete reduce results of already deleted documents
					mappedBuckets.AddRange(scheduledItems.Select(x => x.Bucket));

					foreach (var mappedBucket in mappedBuckets.Distinct())
					{
						actions.MapReduce.RemoveReduceResults(index.IndexName, 1, reduceKey, mappedBucket);
						actions.MapReduce.RemoveReduceResults(index.IndexName, 2, reduceKey, mappedBucket / 1024);
					}
				}

				var mappedResults = actions.MapReduce.GetMappedResults(
						index.IndexName,
						keysToReduce, 
						loadData: true
					).ToList();

				result.count += mappedResults.Count;
				result.size += mappedResults.Sum(x => x.Size);

				var reduceKeys = new HashSet<string>(keysToReduce);

				mappedResults.ApplyIfNotNull(x => x.Bucket = 0);

				var results = mappedResults
					.Where(x => x.Data != null)
					.GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data))
					.ToArray();

				context.ReducedPerSecIncreaseBy(results.Length);

				context.IndexStorage.Reduce(index.IndexName, viewGenerator, results, 2, context, actions, reduceKeys);
			});

			foreach (var reduceKey in needToMoveToSingleStep)
			{
				string localReduceKey = reduceKey;
				transactionalStorage.Batch(actions =>
					actions.MapReduce.UpdatePerformedReduceType(index.IndexName, localReduceKey, ReduceType.SingleStep));
			}

			return result;
		}

		private class ReduceResultStats
		{
			public int count;
			public int size;
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions)
		{
			return actions.Staleness.IsReduceStale(indexesStat.Name);
		}

		protected override Task GetApplicableTask(IStorageActionsAccessor actions)
		{
			return null;
		}

		protected override void FlushAllIndexes()
		{
			context.IndexStorage.FlushReduceIndexes();
		}

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexName = indexesStat.Name,
				LastIndexedEtag = Guid.Empty
			};
		}

		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, indexesToWorkOn, 
				HandleReduceForIndex);
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Name);
			return indexDefinition != null && indexDefinition.IsMapReduce;
		}
	}
}
