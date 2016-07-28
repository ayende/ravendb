﻿using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Changes;

namespace Raven.Server.Documents
{
    public class DocumentsNotifications
    {
        public readonly ConcurrentDictionary<long, NotificationsClientConnection> Connections = new ConcurrentDictionary<long, NotificationsClientConnection>();

        public event Action<DocumentChangeNotification> OnSystemDocumentChange;

        public event Action<DocumentChangeNotification> OnDocumentChange;

        public event Action<IndexChangeNotification> OnIndexChange;

        public event Action<TransformerChangeNotification> OnTransformerChange;

        public void RaiseNotifications(IndexChangeNotification indexChangeNotification)
        {
            OnIndexChange?.Invoke(indexChangeNotification);
        }

        public void RaiseNotifications(TransformerChangeNotification transformerChangeNotification)
        {
            OnTransformerChange?.Invoke(transformerChangeNotification);
        }

        public void RaiseSystemNotifications(DocumentChangeNotification documentChangeNotification)
        {
            OnSystemDocumentChange?.Invoke(documentChangeNotification);

            foreach (var connection in Connections)
                connection.Value.SendDocumentChanges(documentChangeNotification);
        }

        public void RaiseNotifications(DocumentChangeNotification documentChangeNotification)
        {
            OnDocumentChange?.Invoke(documentChangeNotification);

            foreach (var connection in Connections)
                connection.Value.SendDocumentChanges(documentChangeNotification);
        }

        public void Connect(NotificationsClientConnection connection)
        {
            Connections.TryAdd(connection.Id, connection);
        }

        public void Disconnect(long id)
        {
            NotificationsClientConnection connection;
            if (Connections.TryRemove(id, out connection))
                connection.Dispose();
        }
    }
}