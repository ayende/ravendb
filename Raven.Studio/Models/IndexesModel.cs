﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;
using IndexStats = Raven.Abstractions.Data.IndexStats;

namespace Raven.Studio.Models
{
	public class IndexesModel : PageViewModel
	{
		private ICommand deleteIndex;
		private ICommand resetIndex;
		private IndexItem itemSelection;
		private Group selectedGroup;
		public ObservableCollection<IndexItem> Indexes { get; private set; }
		public ObservableCollection<Group> GroupedIndexes { get; private set; }

		public IndexesModel()
		{
			ModelUrl = "/indexes";
			ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/indexes";
			Indexes = new ObservableCollection<IndexItem>();
			GroupedIndexes = new ObservableCollection<Group>();
		}

		public override Task TimerTickedAsync()
		{
            // NOTE: I don't know how to Silverlight - Rob
			return DatabaseCommands
				.GetIndexesAsync(0, int.MaxValue)
				.ContinueOnSuccessInTheUIThread((indexes) =>
				{
				    DatabaseCommands
				        .GetStatisticsAsync()
				        .ContinueOnSuccessInTheUIThread((stats) => {
                            UpdateGroupedIndexList(indexes, stats);
				        });
				});
		}

		public IndexItem ItemSelection
		{
			get { return itemSelection; }
			set
			{
				if(value == null)
					return;
				itemSelection = value;
			}
		}

		public ICommand DeleteIndex { get { return deleteIndex ?? (deleteIndex = new DeleteIndexCommand(this)); } }
		public ICommand ResetIndex { get { return resetIndex ?? (resetIndex = new ResetIndexCommand(this)); } }
		public ICommand DeleteIndexes { get { return new DeleteIndexesCommand(this); } }

		public ICommand DeleteGroupIndexes
		{
			get { return new DeleteIndexesCommand(this);}
		}

		public Group SelectedGroup
		{
			get { return selectedGroup; }
			set
			{
				if (value == null)
					return;
				selectedGroup = value;
			}
		}

		public ICommand CollapseAll
		{
			get { return new ActionCommand(() =>
			{
				foreach (var groupedIndex in GroupedIndexes)
				{
					groupedIndex.Collapse.Value = true;
				}
			});}
		}

		public ICommand ExpandAll
		{
			get
			{
				return new ActionCommand(() =>
				{
					foreach (var groupedIndex in GroupedIndexes)
					{
						groupedIndex.Collapse.Value = false;
					}
				});
			}
		}

		private void UpdateGroupedIndexList(IndexDefinition[] indexes, DatabaseStatistics statistics)
		{
			Indexes.Clear();
			Indexes.AddRange(statistics.Indexes.Select(stats => new IndexItem{Name = indexes.First(x=>x.IndexId == stats.Id).Name, GroupName = GetIndexGroup(stats), IndexStats = stats}));
			
			CleanGroupIndexes();
			foreach (var indexItem in Indexes)
			{
				var groupItem = GroupedIndexes.FirstOrDefault(@group => string.Equals(@group.GroupName, indexItem.GroupName, StringComparison.OrdinalIgnoreCase));
				if (groupItem == null)
				{
					groupItem = new Group(indexItem.GroupName);
					GroupedIndexes.Add(groupItem);
				}

				groupItem.Items.Add(indexItem);
			}

			OnPropertyChanged(() => GroupedIndexes);
		}

		private void CleanGroupIndexes()
		{
			foreach (var groupedIndex in GroupedIndexes)
			{
				groupedIndex.Items.Clear();
			}
		}

		private string GetIndexGroup(IndexStats index)
		{
			if (index.ForEntityName.Count == 1)
				return index.ForEntityName.First();
			return "Others";
		}

		public List<IndexItem> IndexesOfPriority(string deleteItems)
		{
			if (deleteItems == "All")
				return Indexes.ToList();
			if (deleteItems == "Idle")
				return
					Indexes.Where(item => item.IndexStats.Priority.HasFlag(IndexingPriority.Idle)).ToList();
			if (deleteItems == "Disabled")
				return Indexes.Where(item => item.IndexStats.Priority.HasFlag(IndexingPriority.Disabled)).ToList();
			if (deleteItems == "Abandoned")
				return Indexes.Where(item => item.IndexStats.Priority.HasFlag(IndexingPriority.Abandoned)).ToList();

			return null;
		}
	}
}