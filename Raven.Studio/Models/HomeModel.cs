using System.Reactive;
using System.Reactive.Linq;
using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class HomeModel : PageViewModel
	{
		private DocumentsModel recentDocuments;

		public DocumentsModel RecentDocuments
		{
			get
			{
				if (recentDocuments == null)
				{
				    recentDocuments = (new DocumentsModel(new DocumentsCollectionSource())
				                                                      {
				                                                          Header = "Recent Documents",
                                                                          DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index),
                                                                          Context = "AllDocuments",
				                                                      });
                    recentDocuments.SetChangesObservable(d => d.DocumentChanges.Select(s => Unit.Default));
				}

				return recentDocuments;
			}
		}

		public HomeModel()
		{
			ModelUrl = "/home";
			ApplicationModel.Current.Server.Value.RawUrl = null;
		}

	    private bool isGeneratingSampleData;
		public bool IsGeneratingSampleData
		{
			get { return isGeneratingSampleData; }
			set { isGeneratingSampleData = value; OnPropertyChanged(() => IsGeneratingSampleData); }
		}
	}
}