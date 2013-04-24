﻿using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class SampleDataTaskSectionModel : BasicTaskSectionModel
	{
		public SampleDataTaskSectionModel()
		{
			Name = "Create Sample Data";
			IconResource = "Image_EditColumns_Tiny";
			Description = "Create sample data for this database.\nThis will only work if you don't have any documents in the database.";       
		}

		public override ICommand Action
		{
			get { return new CreateSampleDataCommand(this, line => Execute.OnTheUI(() => Output.Add(line))); }
		}
	}
}