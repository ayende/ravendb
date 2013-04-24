﻿using System.Collections.ObjectModel;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class ReplicationSettingsSectionModel : SettingsSectionModel
    {
        private ICommand addReplicationCommand;
        private ICommand deleteReplicationCommand;

        public ReplicationSettingsSectionModel()
        {
            ReplicationDestinations = new ObservableCollection<ReplicationDestination>();
            SectionName = "Replication";
        }

        public ICommand DeleteReplication
        {
            get { return deleteReplicationCommand ?? (deleteReplicationCommand = new ActionCommand(HandleDeleteReplication)); }
        }

        public ICommand AddReplication
        {
            get
            {
                return addReplicationCommand ??
                       (addReplicationCommand =
                        new ActionCommand(() => ReplicationDestinations.Add(new ReplicationDestination())));
            }
        }

        public ReplicationDestination SelectedReplication { get; set; }
       
        public ObservableCollection<ReplicationDestination> ReplicationDestinations { get; set; }
        
        public ReplicationDocument ReplicationData { get; set; }

        private void HandleDeleteReplication(object parameter)
        {
            var destination = parameter as ReplicationDestination;
            if (destination == null)
                return;
            ReplicationDestinations.Remove(destination);
            SelectedReplication = null;
        }

        public override void LoadFor(DatabaseDocument _)
        {
            ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name)
                .LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
                .ContinueOnSuccessInTheUIThread(document =>
                {
                    if (document == null)
                        return;
                    ReplicationData = document;
                    ReplicationDestinations.Clear();
                    foreach (var replicationDestination in ReplicationData.Destinations)
                    {
                        ReplicationDestinations.Add(replicationDestination);
                    }
                });
        }
    }
}