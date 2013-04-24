using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class CsvImportTaskSectionModel : BasicTaskSectionModel
    {
        public CsvImportTaskSectionModel()
        {
            Name = "Csv Import";
            Description = "Import a csv file into a collection. Each column will be treated as a property.";
            IconResource = "Image_ImportCsv_Tiny";
        }

        public override ICommand Action
        {
            get { return new CsvImportCommand(this, line => Execute.OnTheUI(() => Output.Add(line))); }
        }
    }
}