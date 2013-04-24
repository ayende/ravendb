using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteIndexesCommand : Command
	{
		private readonly IndexesModel model;

		public DeleteIndexesCommand(IndexesModel model)
		{
			this.model = model;
		}

		public override void Execute(object parameter)
		{
			var deleteItems = parameter as string;
			AskUser.ConfirmationAsync("Confirm Delete", string.Format("Are you sure that you want to delete all " + deleteItems + " indexes?"))
				.ContinueWhenTrue(() => DeleteIndex(deleteItems));
		}

		private void DeleteIndex(string deleteItems)
		{
			var ravenDocumentsByEntityNameIndexName = new RavenDocumentsByEntityName().IndexName;
			var tasks = (from indexListItem in model.IndexesOfPriority(deleteItems)
			             select indexListItem.Name
			             into indexName
			             where indexName != ravenDocumentsByEntityNameIndexName
			             select new {Task = DatabaseCommands.DeleteIndexAsync(indexName), Name = indexName}).ToArray();
			
			Task.Factory.ContinueWhenAll(tasks.Select(x=>x.Task).ToArray(), taskslist =>
			{
				foreach (var task in taskslist)
				{
					var indexName = tasks.First(x => x.Task == task).Name;
					if (task.IsFaulted)
					{
						ApplicationModel.Current.AddErrorNotification(task.Exception, "index " + indexName + " could not be deleted");
					}
					else
					{
						ApplicationModel.Current.AddInfoNotification("Index " + indexName + " successfully deleted");
						var deletedItem = model.GroupedIndexes.OfType<IndexItem>().FirstOrDefault(item => item.Name == indexName);
						model.GroupedIndexes.Remove(deletedItem);
					}
				}

				UrlUtil.Navigate("/indexes");
			});
		}
	}
}