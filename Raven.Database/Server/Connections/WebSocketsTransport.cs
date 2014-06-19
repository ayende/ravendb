﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Counters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Connections
{
    /*
     * This is really ugly way to go about it, but that is the interface that OWIN
     * gives us
     * http://owin.org/extensions/owin-WebSocket-Extension-v0.4.0.htm
     * 
    */
    using WebSocketAccept = Action<IDictionary<string, object>, // options
        Func<IDictionary<string, object>, Task>>; // callback
    using WebSocketCloseAsync =
        Func<int /* closeStatus */,
            string /* closeDescription */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveAsync =
        Func<ArraySegment<byte> /* data */,
            CancellationToken /* cancel */,
            Task<Tuple<int /* messageType */,
                bool /* endOfMessage */,
                int /* count */>>>;
    using WebSocketSendAsync =
        Func<ArraySegment<byte> /* data */,
            int /* messageType */,
            bool /* endOfMessage */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveResult = Tuple<int, // type
        bool, // end of message?
        int>;
    using Raven.Database.Server.RavenFS; // count

    public class WebSocketsTransport : IEventsTransport
    {
        
        private readonly IOwinContext _context;
        private readonly RavenDBOptions _options;


        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

        private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();
        
        public string Id { get; private set; }
        public bool Connected { get; set; }
        public long CoolDownWithDataLossInMilisecods { get; set; }

        private long lastMessageSentTick = 0;
        private object lastMessageEnqueuedAndNotSent = null;

        public WebSocketsTransport(RavenDBOptions options, IOwinContext context)
        {
            _options = options;
            _context = context;
            Connected = true;
            Id = context.Request.Query["id"];
            long waitTimeBetweenMessages = 0;
            long.TryParse(context.Request.Query["coolDownWithDataLoss"], out waitTimeBetweenMessages);
            CoolDownWithDataLossInMilisecods = waitTimeBetweenMessages;
        }

        public void Dispose()
        {
        }

        


        public event Action Disconnected;

        public void SendAsync(object msg)
        {
            msgs.Enqueue(msg);
            manualResetEvent.Set();
        }

        public async Task Run(IDictionary<string, object> websocketContext)
        {
            try
            {
                var sendAsync = (WebSocketSendAsync) websocketContext["websocket.SendAsync"];
                //var receiveAsync = (WebSocketReceiveAsync)websocketContext["websocket.ReceiveAsync"];
                var closeAsync = (WebSocketCloseAsync) websocketContext["websocket.CloseAsync"];
                var callCancelled = (CancellationToken) websocketContext["websocket.CallCancelled"];

                var memoryStream = new MemoryStream();
                var serializer = new JsonSerializer
                {
                    Converters = {new EtagJsonConverter()}
                };
                
                while (callCancelled.IsCancellationRequested == false)
                {
                    bool result = await manualResetEvent.WaitAsync(5000);
                    if (callCancelled.IsCancellationRequested)
                        return;

                    if (result == false)
                    {
                        await SendMessage(memoryStream, serializer,
                            new { Type = "Heartbeat", Time = SystemTime.UtcNow },
                            sendAsync, callCancelled);

                        if (lastMessageEnqueuedAndNotSent != null)
                        {
                            await SendMessage(memoryStream, serializer, lastMessageEnqueuedAndNotSent, sendAsync, callCancelled);
                        }
                        continue;
                    }
                    manualResetEvent.Reset();
                    object message;
                    while (msgs.TryDequeue(out message))
                    {
                        if (CoolDownWithDataLossInMilisecods > 0 && Environment.TickCount - lastMessageSentTick < CoolDownWithDataLossInMilisecods)
                        {
                            lastMessageEnqueuedAndNotSent = message;
                            continue;
                        }
                        
                        if (callCancelled.IsCancellationRequested)
                            break;

                        lastMessageEnqueuedAndNotSent = null;
                        await SendMessage(memoryStream, serializer, message, sendAsync, callCancelled);
                        lastMessageSentTick = Environment.TickCount;
                        
                    }
                }
                try
                {
                    await closeAsync((int) websocketContext["websocket.ClientCloseStatus"], (string) websocketContext["websocket.ClientCloseDescription"], callCancelled);
                }
                catch (Exception)
                {
                }
            }
            finally
            {
                OnDisconnection();
            }
        }

        private static async Task SendMessage(MemoryStream memoryStream, JsonSerializer serializer, object message, WebSocketSendAsync sendAsync, CancellationToken callCancelled)
        {
            memoryStream.Position = 0;
            var jsonTextWriter = new JsonTextWriter(new StreamWriter(memoryStream));
            serializer.Serialize(jsonTextWriter, message);
            jsonTextWriter.Flush();

            var arraySegment = new ArraySegment<byte>(memoryStream.GetBuffer(), 0, (int) memoryStream.Position);
            await sendAsync(arraySegment, 1, true, callCancelled);
        }

        private void OnDisconnection()
        {
            Connected = false;
            Action onDisconnected = Disconnected;
            if (onDisconnected != null)
                onDisconnected();
        }

        public async Task<bool> TrySetupRequest()
        {
            if (string.IsNullOrEmpty(Id))
            {
                _context.Response.StatusCode = 400;
                _context.Response.ReasonPhrase = "BadRequest";
                _context.Response.Write("{ 'Error': 'Id is mandatory' }");
                return false;
            }

            var documentDatabase = await GetDatabase();
			var fileSystem = await GetFileSystem();
	        var counterStorage = await GetCounterStorage();

			if (documentDatabase == null && fileSystem == null && counterStorage == null)
            {
                return false;
            }

			var singleUseToken = _context.Request.Query["singleUseAuthToken"];

            if (string.IsNullOrEmpty(singleUseToken) == false)
            {
                object msg;
                HttpStatusCode code;
                IPrincipal user;
				var resourceName = (fileSystem != null) ? fileSystem.Name : (counterStorage != null) ? counterStorage.Name : documentDatabase.Name;

				if (_options.MixedModeRequestAuthorizer.TryAuthorizeSingleUseAuthToken(singleUseToken, resourceName, out msg, out code, out user) == false)
                {
                    _context.Response.StatusCode = (int) code;
                    _context.Response.ReasonPhrase = code.ToString();
                    _context.Response.Write(RavenJToken.FromObject(msg).ToString(Formatting.Indented));
                    return false;
                }
            }
            else
            {
                switch (_options.SystemDatabase.Configuration.AnonymousUserAccessMode)
                {
                    case AnonymousUserAccessMode.Admin:
                    case AnonymousUserAccessMode.All:
                    case AnonymousUserAccessMode.Get:
                        // this is effectively a GET request, so we'll allow it
                        // under this circumstances
                        break;
                    case AnonymousUserAccessMode.None:
                        _context.Response.StatusCode = 403;
                        _context.Response.ReasonPhrase = "Forbidden";
                        _context.Response.Write("{'Error': 'Single use token is required for authenticated web sockets connections' }");
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(_options.SystemDatabase.Configuration.AnonymousUserAccessMode.ToString());
                }
            }

            if (fileSystem != null)
            {
                fileSystem.TransportState.Register(this);
            }
			else if (counterStorage != null)
			{
				counterStorage.TransportState.Register(this);
			}
            else if (documentDatabase != null)
            {
				documentDatabase.TransportState.Register(this);
            }

            return true;
        }

        private async Task<DocumentDatabase> GetDatabase()
        {
            var dbName = GetDatabaseName();

            if (dbName == null)
                return _options.SystemDatabase;

            DocumentDatabase documentDatabase;
            try
            {
                documentDatabase = await _options.DatabaseLandlord.GetDatabaseInternal(dbName);
            }
            catch (Exception e)
            {
                _context.Response.StatusCode = 500;
                _context.Response.ReasonPhrase = "InternalServerError";
                _context.Response.Write(e.ToString());
                return null;
            }
            return documentDatabase;
        }

		private async Task<RavenFileSystem> GetFileSystem()
		{
			var fsName = GetFileSystemName();

			if (fsName == null)
				return null;

			RavenFileSystem ravenFileSystem;
			try
			{
				ravenFileSystem = await _options.FileSystemLandlord.GetFileSystemInternal(fsName);
			}
			catch (Exception e)
			{
				_context.Response.StatusCode = 500;
				_context.Response.ReasonPhrase = "InternalServerError";
				_context.Response.Write(e.ToString());
				return null;
			}
			return ravenFileSystem;
		}

		private async Task<CounterStorage> GetCounterStorage()
		{
			var csName = GetCounterStorageName();

			if (csName == null)
				return null;

			CounterStorage counterStorage;
			try
			{
				counterStorage = await _options.CountersLandlord.GetCounterInternal(csName);
			}
			catch (Exception e)
			{
				_context.Response.StatusCode = 500;
				_context.Response.ReasonPhrase = "InternalServerError";
				_context.Response.Write(e.ToString());
				return null;
			}
			return counterStorage;
		}

        private string GetDatabaseName()
        {
            var localPath = _context.Request.Uri.LocalPath;
			const string databasesPrefix = "/databases/";

			return GetResourceName(localPath, databasesPrefix);
        }

        private string GetFileSystemName()
        {
            var localPath = _context.Request.Uri.LocalPath;
			const string fileSystemPrefix = "/fs/";

			return GetResourceName(localPath, fileSystemPrefix);
		}

		private string GetCounterStorageName()
		{
			var localPath = _context.Request.Uri.LocalPath;
			const string counterStoragePrefix = "/counters/";

			return GetResourceName(localPath, counterStoragePrefix);
		}

	    private string GetResourceName(string localPath, string prefix)
	    {
			if (localPath.StartsWith(prefix) == false)
				return null;

			var indexOf = localPath.IndexOf('/', prefix.Length + 1);

		    return (indexOf > -1) ? localPath.Substring(prefix.Length, indexOf - prefix.Length) : null;
	    }
    }
}