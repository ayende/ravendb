﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Impl.Clustering;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Server.WebApi
{
	public class RequestManager : IDisposable
	{
		private readonly DatabasesLandlord landlord;
		private int reqNum;
		private Timer serverTimer;
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();
		private readonly TimeSpan maxTimeDatabaseCanBeIdle;
		private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private readonly ConcurrentDictionary<string, ConcurrentQueue<LogHttpRequestStatsParams>> tracedRequests =
			new ConcurrentDictionary<string, ConcurrentQueue<LogHttpRequestStatsParams>>();

		private DateTime lastWriteRequest;
		private bool disposed;
		//private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly AsyncReaderWriterLock disposerLock = new AsyncReaderWriterLock();
		private int physicalRequestsCount;
		private bool initialized;

		public int NumberOfRequests
		{
			get { return Thread.VolatileRead(ref physicalRequestsCount); }
		}

		public event EventHandler<BeforeRequestWebApiEventArgs> BeforeRequest;

		public RequestManager(DatabasesLandlord landlord)
		{
			this.landlord = landlord;
			int val;
			if (int.TryParse(landlord.SystemConfiguration.Settings["Raven/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
				val = 900;
			maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(val);

			if (int.TryParse(landlord.SystemConfiguration.Settings["Raven/Tenants/FrequencyToCheckForIdleDatabases"], out val) == false)
				val = 60;
			frequencyToCheckForIdleDatabases = TimeSpan.FromSeconds(val);

			Init();
		}

		public void Init()
		{
			if (initialized)
				return;
			initialized = true;
			serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, frequencyToCheckForIdleDatabases);
		}

		public void Dispose()
		{
			using(disposerLock.WriteLock())
			{
				disposed = true;
				var exceptionAggregator = new ExceptionAggregator(Logger, "Could not properly dispose of HttpServer");
				
				exceptionAggregator.Execute(() =>
				{
					if (serverTimer != null)
						serverTimer.Dispose();
				});
			}
		}

		public async Task<HttpResponseMessage> HandleActualRequest(RavenDbApiController controller,
		                                                           Func<Task<HttpResponseMessage>> action,
		                                                           Func<HttpException, HttpResponseMessage> onHttpException)
		{
			HttpResponseMessage response = null;

			using (await disposerLock.ReadLockAsync())
			{
				if (disposed)
					return new HttpResponseMessage();

				Stopwatch sw = Stopwatch.StartNew();
				try
				{
					if (IsWriteRequest(controller.InnerRequest))
					{
						lastWriteRequest = SystemTime.UtcNow;
					}

					if (SetupRequestToProperDatabase(controller))
					{
						response = await action();
					}
				}
				catch (HttpException httpException)
				{
					response = onHttpException(httpException);
				}
				finally
				{
					try
					{
						FinalizeRequestProcessing(controller, response, sw, ravenUiRequest: false /*TODO: check*/);
					}
					catch (Exception e)
					{
						Logger.ErrorException("Could not finalize request properly", e);
					}
				}
			}

			return response;
		}

		// Cross-Origin Resource Sharing (CORS) is documented here: http://www.w3.org/TR/cors/
		public void AddAccessControlHeaders(RavenDbApiController controller, HttpResponseMessage msg)
		{
			if (string.IsNullOrEmpty(landlord.SystemConfiguration.AccessControlAllowOrigin))
				return;

			controller.AddHeader("Access-Control-Allow-Credentials", "true", msg);

			bool originAllowed = landlord.SystemConfiguration.AccessControlAllowOrigin == "*" ||
					landlord.SystemConfiguration.AccessControlAllowOrigin.Split(' ')
						.Any(o => o == controller.GetHeader("Origin"));
			if (originAllowed)
			{
				controller.AddHeader("Access-Control-Allow-Origin", controller.GetHeader("Origin"), msg);
			}

			controller.AddHeader("Access-Control-Max-Age", landlord.SystemConfiguration.AccessControlMaxAge, msg);
			controller.AddHeader("Access-Control-Allow-Methods", landlord.SystemConfiguration.AccessControlAllowMethods, msg);
			if (string.IsNullOrEmpty(landlord.SystemConfiguration.AccessControlRequestHeaders))
			{
				// allow whatever headers are being requested
				var hdr = controller.GetHeader("Access-Control-Request-Headers"); // typically: "x-requested-with"
				if (hdr != null) 
					controller.AddHeader("Access-Control-Allow-Headers", hdr, msg);
			}
			else
			{
				controller.AddHeader("Access-Control-Request-Headers", landlord.SystemConfiguration.AccessControlRequestHeaders, msg);
			}
		}

		private bool SetupRequestToProperDatabase(RavenDbApiController controller)
		{
			var onBeforeRequest = BeforeRequest;
			var tenantId = controller.DatabaseName;

			if (string.IsNullOrWhiteSpace(tenantId) || tenantId == "<system>")
			{
				landlord.DatabaseLastRecentlyUsed.AddOrUpdate("System", SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);
				if (onBeforeRequest != null)
				{
					var args = new BeforeRequestWebApiEventArgs
					{
						Controller = controller,
						IgnoreRequest = false,
						TenantId = "System",
						Database = landlord.SystemDatabase
					};
					onBeforeRequest(this, args);
					if (args.IgnoreRequest)
						return false;
				}
				return true;
			}

			Task<DocumentDatabase> resourceStoreTask;
			bool hasDb;
			try
			{
				hasDb = landlord.TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
			}
			catch (Exception e)
			{
				OutputDatabaseOpenFailure(tenantId, e);
				return false;
			}
			if (hasDb)
			{
				try
				{
					if (resourceStoreTask.Wait(TimeSpan.FromSeconds(30)) == false)
					{
						var msg = "The database " + tenantId + " is currently being loaded, but after 30 seconds, this request has been aborted. Please try again later, database loading continues.";
						Logger.Warn(msg);
						throw new HttpException(503, msg);
					}
					if (onBeforeRequest != null)
					{
						var args = new BeforeRequestWebApiEventArgs()
						{
							Controller = controller,
							IgnoreRequest = false,
							TenantId = tenantId,
							Database = resourceStoreTask.Result
						};
						onBeforeRequest(this, args);
						if (args.IgnoreRequest)
							return false;
					}
				}
				catch (Exception e)
				{
					OutputDatabaseOpenFailure(tenantId, e);
					return false;
				}

				landlord.DatabaseLastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

				//TODO: check
				//if (string.IsNullOrEmpty(systemDatabase.Configuration.VirtualDirectory) == false && systemDatabase.Configuration.VirtualDirectory != "/")
				//{
				//	ctx.AdjustUrl(systemDatabase.Configuration.VirtualDirectory + match.Value);
				//}
				//else
				//{
				//	ctx.AdjustUrl(match.Value);
				//}
			}
			else
			{
				var msg = "Could not find a database named: " + tenantId;
				Logger.Warn(msg);
				throw new HttpException(503, msg);
			}
			return true;
		}

		private void ResetThreadLocalState()
		{
			try
			{
				CurrentOperationContext.Headers.Value = new NameValueCollection();
				CurrentOperationContext.User.Value = null;
				LogContext.DatabaseName.Value = null;
				foreach (var disposable in CurrentOperationContext.RequestDisposables.Value)
				{
					disposable.Dispose();
				}
				CurrentOperationContext.RequestDisposables.Value.Clear();
			}
			catch
			{
				// this can happen during system shutdown
			}
		}

		private static void OutputDatabaseOpenFailure(string tenantId, Exception e)
		{
			var msg = "Could open database named: " + tenantId;
			Logger.WarnException(msg, e);
			throw new HttpException(503, msg, e);
		}

		private static bool IsWriteRequest(HttpRequestMessage request)
		{
			return AbstractRequestAuthorizer.IsGetRequest(request.Method.Method, request.RequestUri.AbsoluteUri) == false;
		}

		public void ResetNumberOfRequests()
		{
			//TODO: implement method
			Interlocked.Exchange(ref reqNum, 0);
			Interlocked.Exchange(ref physicalRequestsCount, 0);
			//#if DEBUG
			//			while (recentRequests.Count > 0)
			//			{
			//				string _;
			//				recentRequests.TryDequeue(out _);
			//			}
			//#endif
		}

		public void IncrementRequestCount()
		{
			Interlocked.Increment(ref physicalRequestsCount);
		}

		public void DecrementRequestCount()
		{
			Interlocked.Decrement(ref physicalRequestsCount);
		}

		private void FinalizeRequestProcessing(RavenDbApiController controller, HttpResponseMessage response, Stopwatch sw, bool ravenUiRequest)
		{
			LogHttpRequestStatsParams logHttpRequestStatsParam = null;
			try
			{
				logHttpRequestStatsParam = new LogHttpRequestStatsParams(
					sw,
					GetHeaders(controller.InnerHeaders), //TODO: request.Headers,
					controller.InnerRequest.Method.Method,
					response != null ? (int) response.StatusCode : 500,
					controller.InnerRequest.RequestUri.PathAndQuery,
					controller.CustomRequestTraceInfo != null ? controller.CustomRequestTraceInfo.ToString() : null
					);
			}
			catch (Exception e)
			{
				Logger.WarnException("Could not gather information to log request stats", e);
			}

			if (ravenUiRequest || logHttpRequestStatsParam == null || sw == null)
				return;

			sw.Stop();

			LogHttpRequestStats(logHttpRequestStatsParam, controller.DatabaseName);

			TraceRequest(logHttpRequestStatsParam, controller.DatabaseName);

			//TODO: log
			//OutputSavedLogItems(logger);
		}

		private void TraceRequest(LogHttpRequestStatsParams requestLog, string databaseName)
		{
			if (string.IsNullOrWhiteSpace(databaseName))
				databaseName = Constants.SystemDatabase;

			var traces = tracedRequests.GetOrAdd(databaseName, new ConcurrentQueue<LogHttpRequestStatsParams>());

			LogHttpRequestStatsParams _;
			while (traces.Count > 50 && traces.TryDequeue(out _))
			{
			}

			traces.Enqueue(requestLog);
		}

		public IEnumerable<LogHttpRequestStatsParams> GetRecentRequests(string databaseName)
		{
			if (string.IsNullOrWhiteSpace(databaseName))
				databaseName = Constants.SystemDatabase;

			ConcurrentQueue<LogHttpRequestStatsParams> queue;
			if (tracedRequests.TryGetValue(databaseName, out queue) == false)
				return Enumerable.Empty<LogHttpRequestStatsParams>();

			return queue.ToArray().Reverse();
		}

		private NameValueCollection GetHeaders(HttpHeaders innerHeaders)
		{
			var result = new NameValueCollection();
			foreach (var innerHeader in innerHeaders)
			{
				result.Add(innerHeader.Key, innerHeader.Value.FirstOrDefault());
			}

			return result;
		}

		private void LogHttpRequestStats(LogHttpRequestStatsParams logHttpRequestStatsParams, string databaseName)
		{
			if (Logger.IsDebugEnabled == false)
				return;

			// we filter out requests for the UI because they fill the log with information
			// we probably don't care about them anyway. That said, we do output them if they take too
			// long.
			if (logHttpRequestStatsParams.Headers["Raven-Timer-Request"] == "true" &&
				logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds <= 25)
				return;

			var curReq = Interlocked.Increment(ref reqNum);
			Logger.Debug("Request #{0,4:#,0}: {1,-7} - {2,5:#,0} ms - {5,-10} - {3} - {4}",
							   curReq,
							   logHttpRequestStatsParams.HttpMethod,
							   logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds,
							   logHttpRequestStatsParams.ResponseStatusCode,
							   logHttpRequestStatsParams.RequestUri,
							   databaseName);
		}


		private void IdleOperations(object state)
		{
			if ((SystemTime.UtcNow - lastWriteRequest).TotalMinutes < 1)
				return;// not idle, we just had a write request coming in

			try
			{
				landlord.SystemDatabase.RunIdleOperations();
			}
			catch (Exception e)
			{
				Logger.ErrorException("Error during idle operation run for system database", e);
			}

			foreach (var documentDatabase in landlord.ResourcesStoresCache)
			{
				try
				{
					if (documentDatabase.Value.Status != TaskStatus.RanToCompletion)
						continue;
					documentDatabase.Value.Result.RunIdleOperations();
				}
				catch (Exception e)
				{
					Logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
				}
			}

			var databasesToCleanup = landlord.DatabaseLastRecentlyUsed
				.Where(x => (SystemTime.UtcNow - x.Value) > maxTimeDatabaseCanBeIdle)
				.Select(x => x.Key)
				.ToArray();

			foreach (var db in databasesToCleanup)
			{
				// intentionally inside the loop, so we get better concurrency overall
				// since shutting down a database can take a while
				landlord.CleanupDatabase(db, skipIfActive: true);
			}
		}
	}
}
