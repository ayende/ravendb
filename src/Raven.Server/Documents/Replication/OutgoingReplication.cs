using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplication : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly ReplicationDestination _destination;
        private Logger _log;
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim(false);
        private readonly CancellationTokenSource _cancellationTokenSource;
        private Thread _sendingThread;

        public EventHandler<Exception> Failed;

        public OutgoingReplication(
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _database = database;
            _destination = destination;
            _log = _database.LoggerSetup.GetLogger<OutgoingReplication>(_database.Name);
            _database.Notifications.OnDocumentChange += HandleDocumentChange;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _sendingThread = new Thread(ReplicateDocuments)
            {
                Name = "Replication from " + _database.Name + " to remote " + _destination.Database + " at " + _destination.Url,
                IsBackground = true
            };
            _sendingThread.Start();
        }

        private void ReplicateDocuments(object o)
        {
            try
            {
                // todo: need to actually handle this properly, have auth in place, etc
                var webRequest = WebRequest.Create(_destination.Url + "/info/tcp");
                var response = webRequest.GetResponseAsync().Result;
                TcpConnectionInfo connection = null; // todo: deserialize above
                using (var tcpClient = new TcpClient())
                {
                    tcpClient.ConnectAsync(new Uri(connection.Url).Host, connection.Port).Wait();

                    using (var networkStream = tcpClient.GetStream())
                    {
                        //todo: TcpConnectionHeaderMessage

                        // todo: get last etag
                        //todo: reply from last etag 

                        while (_cancellationTokenSource.IsCancellationRequested == false)
                        {
                            // send or wait, with heartbeat 
                        }
                    }

                }
            }
            catch (Exception e)
            { 
                Failed?.Invoke(this, e);
            }
        }

        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            _waitForChanges.Set();
        }

        public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= HandleDocumentChange;
            _cancellationTokenSource.Cancel();
            _sendingThread?.Join();
        }
    }
}