﻿using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class StartRestoreTaskSectionModel : BasicTaskSectionModel
	{
		public StartRestoreTaskSectionModel()
		{
			Name = "Restore Database";
			Description = "Restore a database.";
			IconResource = "Image_Restore_Tiny";
			TaskInputs.Add(new TaskInput("Backup Location", @"C:\path-to-your-backup-folder"));
			TaskInputs.Add(new TaskInput("Database Location", ""));
			TaskInputs.Add(new TaskInput("Database Name", ""));
			TaskInputs.Add(new TaskCheckBox("Defrag", false));
		}

		public override ICommand Action
		{
			get { return new RestoreCommand(this); }
		}
	}
}