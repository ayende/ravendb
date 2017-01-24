using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Platform;
using Raven.Json.Linq;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using AsyncHelpers = Raven.Abstractions.Util.AsyncHelpers;

namespace Raven.Client.Document
{
    public class TcpBulkInsertOperation : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(TcpBulkInsertOperation));
        private readonly CancellationTokenSource _cts;
        private readonly Task _getServerResponseTask;
        private JsonOperationContext _jsonOperationContext;
        private readonly BlockingCollection<MemoryStream> _documents = new BlockingCollection<MemoryStream>();

        private readonly BlockingCollection<MemoryStream> _buffers =
            // we use a stack based back end to ensure that we always use the active buffers
            new BlockingCollection<MemoryStream>(new ConcurrentStack<MemoryStream>());

        private readonly Task _writeToServerTask;
        private DateTime _lastHeartbeat;
        private readonly long _sentAccumulator;

        private readonly ManualResetEventSlim _throttlingEvent = new ManualResetEventSlim();
        private bool _isThrottling;
        private readonly long _maxDiffSizeBeforeThrottling = 20L * 1024 * 1024; // each buffer is 4M. We allow the use of 5-6 buffers out of 8 possible
        private TcpClient _tcpClient;


        ~TcpBulkInsertOperation()
        {
            try
            {
                Log.Warn("Web socket bulk insert was not disposed, and is cleaned via finalizer");
                Dispose();
            }
            catch (Exception e)
            {
                Log.WarnException("Failed to dispose web socket bulk operation from finalizer", e);
            }
        }

        public TcpBulkInsertOperation(AsyncServerClient asyncServerClient, CancellationTokenSource cts)
        {
            _throttlingEvent.Set();
            _jsonOperationContext = new JsonOperationContext(1024 * 1024, 16 * 1024);
            _cts = cts ?? new CancellationTokenSource();
            _tcpClient = new TcpClient();

            for (int i = 0; i < 64; i++)
            {
                _buffers.Add(new MemoryStream());
            }
            var connectToServerTask = ConnectToServer(asyncServerClient);

            _sentAccumulator = 0;
            _getServerResponseTask = connectToServerTask.ContinueWith(task =>
            {
                ReadServerResponses(task.Result);
            });

            _writeToServerTask = connectToServerTask.ContinueWith(task =>
            {
                WriteToServer(asyncServerClient.Url, task.Result);
            });

        }

        private async Task<Stream> ConnectToServer(AsyncServerClient asyncServerClient)
        {
            var connectionInfo = await asyncServerClient.GetTcpInfoAsync().ConfigureAwait(false);
            var uri = new Uri(connectionInfo.Url);
            await _tcpClient.ConnectAsync(uri.Host, uri.Port).ConfigureAwait(false);

            _tcpClient.NoDelay = true;
            _tcpClient.SendBufferSize = 32 * 1024;
            _tcpClient.ReceiveBufferSize = 4096;
            var networkStream = _tcpClient.GetStream();

            return networkStream;
        }

        private void WriteToServer(string url, Stream serverStream)
        {
            const string debugTag = "bulk/insert/document";
            var jsonParserState = new JsonParserState();
            //var streamNetworkBuffer = new BufferedStream(serverStream, 32 * 1024);
            var streamNetworkBuffer = serverStream;
            var writeToStreamBuffer = new byte[32 * 1024];
            var header = Encoding.UTF8.GetBytes(RavenJObject.FromObject(new TcpConnectionHeaderMessage
            {
                DatabaseName = MultiDatabase.GetDatabaseName(url),
                Operation = TcpConnectionHeaderMessage.OperationTypes.BulkInsert
            }).ToString());
            streamNetworkBuffer.Write(header, 0, header.Length);
            JsonOperationContext.ManagedPinnedBuffer bytes;
            using (_jsonOperationContext.GetManagedBuffer(out bytes))
            {
                while (_documents.IsCompleted == false)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    MemoryStream jsonBuffer;

                    try
                    {
                        jsonBuffer = _documents.Take();
                    }
                    catch (InvalidOperationException)
                    {
                        break;
                    }

                    var needToThrottle = _throttlingEvent.Wait(0) == false;

                    _jsonOperationContext.ResetAndRenew();
                    using (var jsonParser = new UnmanagedJsonParser(_jsonOperationContext, jsonParserState, debugTag))
                    using (var builder = new BlittableJsonDocumentBuilder(_jsonOperationContext,
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk, debugTag,
                        jsonParser, jsonParserState))
                    {
                        _jsonOperationContext.CachedProperties.NewDocument();
                        builder.ReadObjectDocument();
                        while (true)
                        {
                            var read = jsonBuffer.Read(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                            if (read == 0)
                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                            jsonParser.SetBuffer(bytes, 0, read);
                            if (builder.Read())
                                break;
                        }
                        _buffers.Add(jsonBuffer);
                        builder.FinalizeDocument();
                        WriteVariableSizeInt(streamNetworkBuffer, builder.SizeInBytes);
                        WriteToStream(streamNetworkBuffer, builder, writeToStreamBuffer);
                    }

                    if (needToThrottle)
                    {
                        streamNetworkBuffer.Flush();
                        _throttlingEvent.Wait(500);
                    }
                }
                streamNetworkBuffer.WriteByte(0); //done
                streamNetworkBuffer.Flush();
            }
        }

        private static unsafe void WriteToStream(Stream stream, BlittableJsonDocumentBuilder builder,
            byte[] buffer)
        {
            using (var reader = builder.CreateReader())
            {
                fixed (byte* pBuffer = buffer)
                {
                    var bytes = reader.BasePointer;
                    var remainingSize = reader.Size;
                    while (remainingSize > 0)
                    {
                        var size = Math.Min(remainingSize, buffer.Length);
                        Memory.Copy(pBuffer, bytes + (reader.Size - remainingSize), size);
                        remainingSize -= size;
                        stream.Write(buffer, 0, size);
                    }
                }
            }
        }


        public static void WriteVariableSizeInt(Stream stream, int value)
        {
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                stream.WriteByte((byte)(v | 0x80));
                v >>= 7;
            }
            stream.WriteByte((byte)v);
        }

        public async Task<BlittableJsonReaderObject> TryReadFromWebSocket(
           JsonOperationContext context,
            RavenClientWebSocket webSocket,
           string debugTag,
           CancellationToken cancellationToken)
        {
            var jsonParserState = new JsonParserState();
            JsonOperationContext.ManagedPinnedBuffer bytes;
            using (context.GetManagedBuffer(out bytes))
            using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
            {
                var writer = new BlittableJsonDocumentBuilder(context,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState);

                writer.ReadObjectDocument();

                var result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken).ConfigureAwait(false);

                parser.SetBuffer(bytes, 0, result.Count);
                while (writer.Read() == false)
                {
                    // we got incomplete json response.
                    // This might happen if we close the connection but still server sends something
                    if (result.CloseStatus != null)
                        return null;

                    result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken).ConfigureAwait(false);

                    parser.SetBuffer(bytes, 0, result.Count);
                }
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }

        private void ReadServerResponses(Stream stream)
        {
            bool completed = false;
            using (var context = new JsonOperationContext(4096, 1024))
            {
                do
                {
                    using (var response = context.ReadForMemory(stream, "bulk/insert/message"))
                    {
                        if (response == null)
                        {
                            // we've got disconnection without receiving "Completed" message
                            ReportProgress("Bulk insert aborted because connection with server was disrupted before acknowledging completion");
                            throw new InvalidOperationException("Connection with server was disrupted before acknowledging completion");
                        }

                        string responseType;
                        //TODO: make this strong typed?
                        if (response.TryGet("Type", out responseType))
                        {
                            string msg;
                            switch (responseType)
                            {
                                case "Error":
                                    string exceptionString;
                                    if (response.TryGet("Exception", out exceptionString) == false)
                                        throw new InvalidOperationException("Invalid response from server " +
                                                                            (response.ToString() ?? "null"));
                                    msg =
                                        $"Bulk insert aborted because of server-side exception. Exception information from server : {Environment.NewLine} {exceptionString}";
                                    ReportProgress(msg);
                                    throw new BulkInsertAbortedException(msg);

                                case "Processing":
                                    // do nothing. this is hearbeat while server is really busy
                                    break;

                                case "Waiting":
                                    if (_isThrottling)
                                    {
                                        _isThrottling = false;
                                        _throttlingEvent.Set();
                                    }
                                    break;

                                case "Processed":
                                    {
                                        long processedSize;
                                        if (response.TryGet("Size", out processedSize) == false)
                                            throw new InvalidOperationException("Invalid Processed response from server " +
                                                                                (response.ToString() ?? "null"));

                                        if (_sentAccumulator - processedSize > _maxDiffSizeBeforeThrottling)
                                        {
                                            if (_isThrottling == false)
                                            {
                                                _throttlingEvent.Reset();
                                                _isThrottling = true;
                                            }
                                        }
                                        else
                                        {
                                            if (_isThrottling)
                                            {
                                                _isThrottling = false;
                                                _throttlingEvent.Set();
                                            }
                                        }
                                    }
                                    break;

                                case "Completed":
                                    ReportProgress("Connection closed successfully");
                                    completed = true;
                                    break;
                                default:
                                    {
                                        msg = "Received unexpected message from a server : " + responseType;
                                        ReportProgress(msg);
                                        throw new BulkInsertProtocolViolationException(msg);
                                    }
                            }
                        }
                        _lastHeartbeat = SystemTime.UtcNow;
                    }
                } while (completed == false);
            }
        }

        public async Task WriteAsync(string id, RavenJObject metadata, RavenJObject data)
        {
            _cts.Token.ThrowIfCancellationRequested();

            await AssertValidServerConnection().ConfigureAwait(false);// we should never actually get here, the await will throw

            metadata[Constants.Metadata.Id] = id;
            data[Constants.Metadata.Key] = metadata;

            MemoryStream jsonBuffer;
            while (true)
            {
                if (_buffers.TryTake(out jsonBuffer, 250))
                    break;
                await AssertValidServerConnection().ConfigureAwait(false);
            }
            jsonBuffer.SetLength(0);

            data.WriteTo(jsonBuffer);
            jsonBuffer.Position = 0;
            _documents.Add(jsonBuffer);
        }

        private async Task AssertValidServerConnection()
        {
            if (_getServerResponseTask.IsFaulted || _getServerResponseTask.IsCanceled)
                await _getServerResponseTask.ConfigureAwait(false);

            if (_writeToServerTask.IsFaulted || _writeToServerTask.IsCanceled)
                await _getServerResponseTask.ConfigureAwait(false);

            if (_getServerResponseTask.IsCompleted)
                // we can only get here if we closed the connection
                throw new ObjectDisposedException(nameof(TcpBulkInsertOperation));
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task DisposeAsync()
        {
            try
            {
                try
                {
                    _documents.CompleteAdding();
                    await _writeToServerTask.ConfigureAwait(false);

                    //Make sure that if the server goes down 
                    //in the last moment, we do not get stuck here.
                    //In general, 1 minute should be more than enough for the 
                    //server to finish its stuff and get back to client

                    var timeDelay = Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30);
                    while (true)
                    {
                        var timeoutTask = Task.Delay(timeDelay);
                        var res = await Task.WhenAny(timeoutTask, _getServerResponseTask).ConfigureAwait(false);
                        if (timeoutTask != res)
                            break;
                        if (SystemTime.UtcNow - _lastHeartbeat > timeDelay + TimeSpan.FromSeconds(60))
                        {
                            // Waited to much for close msg from server in bulk insert.. can happen in huge bulk insert ratios.
                            throw new TimeoutException("Wait for bulk-insert closing message from server, but it didn't happen. Maybe the server went down (most likely) and maybe this is due to a bug. In any case,this needs to be investigated.");
                        }
                    }
                }
                finally
                {
                    _tcpClient?.Dispose();
                }
            }
            finally
            {
                _tcpClient = null;
                _jsonOperationContext?.Dispose();
                _jsonOperationContext = null;
                GC.SuppressFinalize(this);
            }
        }

        public event Action<string> Report;

        public void Abort()
        {
            throw new NotSupportedException();
            //TODO: implement this
        }

        protected void ReportProgress(string msg)
        {
            Report?.Invoke(msg);
        }
    }
}