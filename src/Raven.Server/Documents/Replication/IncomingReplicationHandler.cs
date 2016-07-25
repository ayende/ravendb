﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Linq;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private NetworkStream _stream;
        private DocumentsOperationContext _context;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        private readonly IDisposable _contextDisposable;

        private long _lastReceivedEtag;

        public long LastReceivedEtag => _lastReceivedEtag;

        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;

        private readonly MetricsScheduler _metricsScheduler = new MetricsScheduler();

        //this probably will become one of the metrics counters, when it will become available 
        public MeterMetric ReceivedDocumentsCount { get; private set; }
        public MeterMetric DocumentSizeInBatch { get; private set; }
        public MeterMetric LargestDocumentSizeInBatch { get; private set; }

        public IncomingReplicationHandler(JsonOperationContext.MultiDocumentParser multiDocumentParser, DocumentDatabase database, TcpClient tcpClient, NetworkStream stream, ReplicationLatestEtagRequest replicatedLastEtag)
        {
            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            _multiDocumentParser = multiDocumentParser;
            _database = database;
            _tcpClient = tcpClient;
            _stream = stream;
            _contextDisposable = _database.DocumentsStorage
                                          .ContextPool
                                          .AllocateOperationContext(out _context);

            _log = _database.LoggerSetup.GetLogger<IncomingReplicationHandler>(_database.Name);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            ReceivedDocumentsCount = new MeterMetric(_metricsScheduler);
            DocumentSizeInBatch = new MeterMetric(_metricsScheduler);
            LargestDocumentSizeInBatch = new MeterMetric(_metricsScheduler);
        }

        public void Start()
        {
            _incomingThread = new Thread(ReceiveReplicatedDocuments)
            {
                IsBackground = true,
                Name = $"Incoming replication {FromToString}"
            };
            _incomingThread.Start();
            if (_log.IsInfoEnabled)
                _log.Info($"Incoming replication thread started ({FromToString})");
        }

        //TODO : do not forget to add logging and code to record stats
        private void ReceiveReplicatedDocuments()
        {
            try
            {
                using (_contextDisposable)
                using (_stream)
                using (var writer = new BlittableJsonTextWriter(_context, _stream))
                using (_multiDocumentParser)
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        using (var message = _multiDocumentParser.ParseToMemory("IncomingReplication/read-message"))
                        {
                            //note: at this point, the valid messages are heartbeat and replication batch.
                            _cts.Token.ThrowIfCancellationRequested();
                            bool _;
                            if (message.TryGet("Heartbeat", out _))
                            {
                                BlittableJsonReaderArray changeVector;
                                if (!message.TryGet("ChangeVector", out changeVector))
                                    throw new InvalidDataException("Failed to get 'ChangeVector' from heartbeat message. This is not supposed to happen and it is likely a bug.");


                                if (_log.IsInfoEnabled)
                                    _log.Info($"Incoming replication thread ({FromToString}) received heartbeat.");
                                continue;
                            }

                            ThrowIfNotReplicationBatch(message);

                            BlittableJsonReaderArray replicatedDocs;
                            if (!message.TryGet("ReplicationBatch", out replicatedDocs))
                                throw new InvalidDataException(
                                    "Expected the message to have a field with replicated document array, named ReplicationBatch. The property wasn\'t found");

                            try
                            {
                                //TODO : consider replacing this with pooled stopwatch objects -> reduce allocations								
                                //note: because of the logic in OutgoingReplicationHandler, 
                                // it is not possible to receive empty batch at this point								
                                var sw = Stopwatch.StartNew();
                                long lastReceivedEtag;
                                using (_context.OpenWriteTransaction())
                                {
                                    lastReceivedEtag = ReceiveDocuments(_context, replicatedDocs);
                                    _context.Transaction.Commit();

                                }
                                sw.Stop();

                                if (_log.IsInfoEnabled)
                                    _log.Info($"Replication connection {FromToString}: received and written {replicatedDocs.Length} documents to database in {sw.ElapsedMilliseconds} ms, with last etag = {lastReceivedEtag}.");

                                _lastReceivedEtag = lastReceivedEtag;
                                //return positive ack
                                _context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = ReplicationBatchReply.ReplyType.Ok.ToString(),
                                    ["LastEtagAccepted"] = lastReceivedEtag,
                                    ["Error"] = null
                                });								
                                OnDocumentsReceived(this);
                            }
                            catch (Exception e)
                            {
                                //if we are disposing, ignore errors
                                if (!_cts.IsCancellationRequested && !(e is ObjectDisposedException))
                                {
                                    //return negative ack
                                    _context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = ReplicationBatchReply.ReplyType.Error.ToString(),
                                        ["LastEtagAccepted"] = -1,
                                        ["Error"] = e.ToString()
                                    });

                                    e.Data.Add("FailedToWrite", true);

                                    if (_log.IsInfoEnabled)
                                        _log.Info($"Replication connection {FromToString}: failed writing documents to database - unhandled exception was thrown.{Environment.NewLine} {e}");
                                    throw;
                                }
                            }
                            finally
                            {
                                try
                                {
                                    writer.Flush();
                                }
                                catch (Exception e)
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info($"Replication connection {FromToString}: failed to send back acknowledgement message for the replication batch. Error thrown : {e}");
                                    // nothing to do at this point
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (!_cts.IsCancellationRequested)
                {
                    //if FailedToWrite is in e.Data, we logged the exception already
                    if (_log.IsInfoEnabled && !e.Data.Contains("FailedToWrite"))
                        _log.Info($"Replication connection {FromToString}: an exception was thrown during receiving incoming document replication batch. {e}");

                    OnFailed(e, this);
                }
            }
            finally
            {
                _context = null;
                _stream = null;
            }
        }

        private static void ThrowIfNotReplicationBatch(BlittableJsonReaderObject message)
        {
            string messageType;
            if (!message.TryGet("Type", out messageType))
                throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

            if (!messageType.Equals("ReplicationBatch", StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Expected the message 'Type = ReplicationBatch' field, but has 'Type={messageType}'. This is likely a bug.");
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        private long ReceiveDocuments(DocumentsOperationContext context, BlittableJsonReaderArray docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
            var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();

            using (new IncomingReplicationScope())
            {
                double sum = 0;
                foreach (BlittableJsonReaderObject doc in docs)
                {
                    var changeVector = doc.EnumerateChangeVector();
                    foreach (var currentEntry in changeVector)
                    {
                        if (currentEntry.DbId != Guid.Empty) //should never happen, but..
                            throw new InvalidOperationException(
                                "change vector database Id is Guid.Empty. This is not supposed to happen and it is likely a bug.");

                        //note: documents in a replication batch are ordered in incremental etag order
                        maxReceivedChangeVectorByDatabase[currentEntry.DbId] = currentEntry.Etag;
                    }

                    //since blittable deals with offsets, if we want to deserialize embedded object properly,
                    //we need to create a new document with proper offsets (that would actually point to embedded object data)
                    using (
                        var detachedDoc = context.ReadObject(doc, "IncomingDocumentReplication -> Detach object from parent array"))
                        WriteReceivedDocument(context, detachedDoc);

                    sum += doc.Size;
                    if(doc.Size > LargestDocumentSizeInBatch.MeanRate)
                        LargestDocumentSizeInBatch.Mark(doc.Size);
                }

            }

            ReceivedDocumentsCount.Mark(docs.Length);

            //if any of [dbId -> etag] is larger than server pair, update it
            var changeVectorUpdated = dbChangeVector.UpdateLargerEtagIfRelevant(maxReceivedChangeVectorByDatabase);
            bool changeVectorResized;
            dbChangeVector = dbChangeVector.InsertNewEtagsIfRelevant(
                maxReceivedChangeVectorByDatabase, out changeVectorResized);

            if (changeVectorUpdated || changeVectorResized)
                _database.DocumentsStorage.SetChangeVector(context, dbChangeVector);

            return dbChangeVector.FirstOrDefault(x => x.DbId == Guid.Parse(ConnectionInfo.SourceDatabaseId)).Etag;
        }

        private void WriteReceivedDocument(DocumentsOperationContext context, BlittableJsonReaderObject doc)
        {

            var id = doc.GetIdFromMetadata();
            if (id == null)
                throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");

            // we need to split this document to an independent blittable document
            // and this time, we'll prepare it for disk.
            doc.PrepareForStorage();			
            _database.DocumentsStorage.Put(context, id, null, doc);
        }

        public void Dispose()
        {
            _cts.Cancel();
            try
            {
                _stream.Dispose();
            }
            catch (Exception)
            {
            }
            try
            {
                _tcpClient.Dispose();
            }
            catch (Exception)
            {
            }

            if (_incomingThread != Thread.CurrentThread)
            {
                _incomingThread?.Join();
            }
            _incomingThread = null;
        }

        protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(instance, exception);
        protected void OnDocumentsReceived(IncomingReplicationHandler instance) => DocumentsReceived?.Invoke(instance);
    }
}
