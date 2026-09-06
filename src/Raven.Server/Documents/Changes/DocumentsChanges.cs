using System;
using Raven.Client.Documents.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Changes
{
    public sealed class DocumentsChanges : DocumentsChangesBase<ChangesClientConnection, DocumentsOperationContext>
    {
        public event Action<DocumentChange> OnDocumentChange;

        public event Action<CounterChange> OnCounterChange;

        public event Action<TimeSeriesChange> OnTimeSeriesChange;

        public event Action<IndexChange> OnIndexChange;

        public void RaiseNotifications(IndexChange indexChange)
        {
            OnIndexChange?.Invoke(indexChange);

            foreach (var connection in Connections)
                connection.Value.SendIndexChanges(indexChange);
        }

        public void RaiseNotifications(DocumentChange documentChange)
        {
            OnDocumentChange?.Invoke(documentChange);

            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendDocumentChanges(documentChange);
            }
        }

        // Internal listeners (indexing, ETL, replication, subscriptions) only need to know that a
        // collection changed, so we notify them once per collection per transaction instead of once
        // per document - no per-document DocumentChange allocation.
        public void RaiseInternalDocumentChangeNotification(DocumentChange documentChange)
        {
            OnDocumentChange?.Invoke(documentChange);
        }

        // External /changes clients need the per-document detail (id, change vector, type).
        public void SendDocumentChangeToConnections(DocumentChange documentChange)
        {
            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendDocumentChanges(documentChange);
            }
        }

        public void RaiseNotifications(CounterChange counterChange)
        {
            OnCounterChange?.Invoke(counterChange);

            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendCounterChanges(counterChange);
            }
        }

        public void RaiseNotifications(TimeSeriesChange timeSeriesChange)
        {
            OnTimeSeriesChange?.Invoke(timeSeriesChange);

            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendTimeSeriesChanges(timeSeriesChange);
            }
        }
    }
}
