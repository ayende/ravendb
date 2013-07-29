#if !NETFX_CORE && !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
#if SILVERLIGHT || NETFX_CORE
using Raven.Client.Silverlight.MissingFromSilverlight;
#else
using System.Collections.Specialized;
#endif
using System.Diagnostics;
using System.IO;
#if !SILVERLIGHT
using System.IO.Compression;
#endif
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	/// <summary>
	/// A representation of an HTTP json request to the RavenDB server
	/// </summary>
	public class HttpJsonRequest
	{
		internal readonly string Url;
		internal readonly string Method;

		internal volatile HttpClient httpClient;
		internal volatile HttpWebRequest webRequest;

		// temporary create a strong reference to the cached data for this request
		// avoid the potential for clearing the cache from a cached item
		internal CachedRequest CachedRequestDetails;
		private readonly HttpJsonRequestFactory factory;
		private readonly IHoldProfilingInformation owner;
		private readonly DocumentConvention conventions;
		private string postedData;
		private Stopwatch sp = Stopwatch.StartNew();
		internal bool ShouldCacheRequest;
		private Stream postedStream;
		private bool writeCalled;
		public static readonly string ClientVersion = typeof(HttpJsonRequest).Assembly.GetName().Version.ToString();
		private bool disabledAuthRetries;
		private string primaryUrl;

		private string operationUrl;

		public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };
		public Action<HttpResponseHeaders, string, string> HandleReplicationStatusChanges2 = delegate { };

		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public NameValueCollection ResponseHeaders { get; set; }

		internal HttpJsonRequest(
			CreateHttpJsonRequestParams requestParams,
			HttpJsonRequestFactory factory)
		{
			Url = requestParams.Url;
			Method = requestParams.Method;

			this.factory = factory;
			owner = requestParams.Owner;
			conventions = requestParams.Convention;

			var handler = new WebRequestHandler
			{
				UseDefaultCredentials = true,
				Credentials = requestParams.Credentials,
			};
			httpClient = new HttpClient(handler);

			webRequest = (HttpWebRequest)WebRequest.Create(requestParams.Url);
			webRequest.UseDefaultCredentials = true;
			webRequest.Credentials = requestParams.Credentials;
			webRequest.Method = requestParams.Method;

			if (factory.DisableRequestCompression == false && requestParams.DisableRequestCompression == false)
			{
				if (requestParams.Method == "POST" || requestParams.Method == "PUT" ||
				    requestParams.Method == "PATCH" || requestParams.Method == "EVAL")
				{
					webRequest.Headers["Content-Encoding"] = "gzip";
					httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Encoding", "gzip");
				}

				webRequest.Headers["Accept-Encoding"] = "gzip";
				httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Accept-Encoding", "gzip");
			}

			webRequest.ContentType = "application/json; charset=utf-8";
			httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "utf-8" });
			webRequest.Headers.Add("Raven-Client-Version", ClientVersion);
			httpClient.DefaultRequestHeaders.Add("Raven-Client-Version", ClientVersion);

			WriteMetadata(requestParams.Metadata);
			requestParams.UpdateHeaders(webRequest);
		}

		public void DisableAuthentication()
		{
			webRequest.Credentials = null;
			webRequest.UseDefaultCredentials = false;
			disabledAuthRetries = true;
		}

		public Task ExecuteRequestAsync()
		{
			return ReadResponseJsonAsync();
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		public async Task<RavenJToken> ReadResponseJsonAsync()
		{
			if (SkipServerCheck)
			{
				var result = factory.GetCachedResponse(this);
				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)ResponseStatusCode,
					Status = RequestStatus.AggressivelyCached,
					Result = result.ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});
				return result;
			}

			int retries = 0;
			while (true)
			{
				ErrorResponseException webException;
				try
				{
					if (writeCalled == false)
					{
						try
						{
							Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url));

							ResponseHeaders = new NameValueCollection();
							foreach (var header in Response.Headers)
							{
								foreach (var val in header.Value)
								{
									ResponseHeaders.Add(header.Key, val);
								}
							}

							ResponseStatusCode = Response.StatusCode;
						}
						finally
						{
							sp.Stop();
						}
						await this.CheckForErrorsAsync();
					}
					return await ReadJsonInternalAsync();
				}
				catch (ErrorResponseException e)
				{
					if (++retries >= 3 || disabledAuthRetries)
						throw;

					if (e.StatusCode != HttpStatusCode.Unauthorized &&
						e.StatusCode != HttpStatusCode.Forbidden &&
						e.StatusCode != HttpStatusCode.PreconditionFailed)
						throw;

					webException = e;
				}

				if (Response.StatusCode == HttpStatusCode.Forbidden)
				{
					await HandleForbiddenResponseAsync(Response);
					await new CompletedTask(webException).Task; // Throws, preserving original stack
				}

				if (await HandleUnauthorizedResponseAsync(Response) == false)
					await new CompletedTask(webException).Task; // Throws, preserving original stack
			}
		}

		private async Task CheckForErrorsAsync()
		{
			if (Response.IsSuccessStatusCode == false)
			{
				if (Response.StatusCode == HttpStatusCode.Unauthorized ||
					Response.StatusCode == HttpStatusCode.NotFound ||
					Response.StatusCode == HttpStatusCode.Conflict)
				{
					factory.InvokeLogRequest(owner, () => new RequestResultArgs
					{
						DurationMilliseconds = CalculateDuration(),
						Method = webRequest.Method,
						HttpResult = (int)Response.StatusCode,
						Status = RequestStatus.ErrorOnServer,
						Result = Response.StatusCode.ToString(),
						Url = webRequest.RequestUri.PathAndQuery,
						PostedData = postedData
					});

					throw new ErrorResponseException(Response);
				}

				if (Response.StatusCode == HttpStatusCode.NotModified
					&& CachedRequestDetails != null)
				{
					factory.UpdateCacheTime(this);
					var result = factory.GetCachedResponse(this, Response.Headers);

					HandleReplicationStatusChanges2(Response.Headers, primaryUrl, operationUrl);

					factory.InvokeLogRequest(owner, () => new RequestResultArgs
					{
						DurationMilliseconds = CalculateDuration(),
						Method = webRequest.Method,
						HttpResult = (int)Response.StatusCode,
						Status = RequestStatus.Cached,
						Result = result.ToString(),
						Url = webRequest.RequestUri.PathAndQuery,
						PostedData = postedData
					});

					// return result;
				}


				using (var sr = new StreamReader(await Response.GetResponseStreamWithHttpDecompression()))
				{
					var readToEnd = sr.ReadToEnd();

					factory.InvokeLogRequest(owner, () => new RequestResultArgs
					{
						DurationMilliseconds = CalculateDuration(),
						Method = webRequest.Method,
						HttpResult = (int)Response.StatusCode,
						Status = RequestStatus.Cached,
						Result = readToEnd,
						Url = webRequest.RequestUri.PathAndQuery,
						PostedData = postedData
					});

					if (string.IsNullOrWhiteSpace(readToEnd))
						throw new ErrorResponseException(Response);

					RavenJObject ravenJObject;
					try
					{
						ravenJObject = RavenJObject.Parse(readToEnd);
					}
					catch (Exception e)
					{
						throw new InvalidOperationException(readToEnd, e);
					}
					if (ravenJObject.ContainsKey("IndexDefinitionProperty"))
					{
						throw new IndexCompilationException(ravenJObject.Value<string>("Message"))
						{
							IndexDefinitionProperty = ravenJObject.Value<string>("IndexDefinitionProperty"),
							ProblematicText = ravenJObject.Value<string>("ProblematicText")
						};
					}
					if (Response.StatusCode == HttpStatusCode.BadRequest && ravenJObject.ContainsKey("Message"))
					{
						throw new BadRequestException(ravenJObject.Value<string>("Message"), new ErrorResponseException(Response));
					}
					if (ravenJObject.ContainsKey("Error"))
					{
						var sb = new StringBuilder();
						foreach (var prop in ravenJObject)
						{
							if (prop.Key == "Error")
								continue;

							sb.Append(prop.Key).Append(": ").AppendLine(prop.Value.ToString(Formatting.Indented));
						}

						if (sb.Length > 0)
							sb.AppendLine();
						sb.Append(ravenJObject.Value<string>("Error"));

						throw new InvalidOperationException(sb.ToString(), new ErrorResponseException(Response));
					}
					throw new InvalidOperationException(readToEnd, new ErrorResponseException(Response));
				}
			}

		}

		public async Task<byte[]> ReadResponseBytesAsync()
		{
			if (writeCalled == false)
				webRequest.ContentLength = 0;
			using (var webResponse = await webRequest.GetResponseAsync())
			using (var stream = webResponse.GetResponseStreamWithHttpDecompression())
			{
				ResponseHeaders = new NameValueCollection(webResponse.Headers);
				return await stream.ReadDataAsync();
			}
		}

		public void ExecuteRequest()
		{
			ReadResponseJson();
		}

		public byte[] ReadResponseBytes()
		{
			if (writeCalled == false)
				webRequest.ContentLength = 0;
			using (var webResponse = webRequest.GetResponse())
			using (var stream = webResponse.GetResponseStreamWithHttpDecompression())
			{
				ResponseHeaders = new NameValueCollection(webResponse.Headers);
				return stream.ReadData();
			}
		}

		/// <summary>
		/// Reads the response string.
		/// </summary>
		/// <returns></returns>
		public RavenJToken ReadResponseJson()
		{
			if (SkipServerCheck)
			{
				var result = factory.GetCachedResponse(this);
				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)ResponseStatusCode,
					Status = RequestStatus.AggressivelyCached,
					Result = result.ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});
				return result;
			}

			int retries = 0;
			while (true)
			{
				try
				{
					if (writeCalled == false)
						webRequest.ContentLength = 0;
					return ReadJsonInternal(webRequest.GetResponse);
				}
				catch (WebException e)
				{
					if (++retries >= 3 || disabledAuthRetries)
						throw;

					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse == null ||
						(httpWebResponse.StatusCode != HttpStatusCode.Unauthorized &&
						 httpWebResponse.StatusCode != HttpStatusCode.Forbidden &&
						 httpWebResponse.StatusCode != HttpStatusCode.PreconditionFailed))
						throw;

					if (httpWebResponse.StatusCode == HttpStatusCode.Forbidden)
					{
						HandleForbiddenResponse(httpWebResponse);
						throw;
					}

					if (HandleUnauthorizedResponse(httpWebResponse) == false)
						throw;
				}
			}
		}

		public bool HandleUnauthorizedResponse(HttpWebResponse unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponse == null)
				return false;

			var handleUnauthorizedResponse = conventions.HandleUnauthorizedResponse(unauthorizedResponse);
			if (handleUnauthorizedResponse == null)
				return false;

			RecreateWebRequest(handleUnauthorizedResponse);
			return true;
		}

		private void HandleForbiddenResponse(HttpWebResponse forbiddenResponse)
		{
			if (conventions.HandleForbiddenResponse == null)
				return;

			conventions.HandleForbiddenResponse(forbiddenResponse);
		}

		public async Task<bool> HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return false;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);
			if (unauthorizedResponseAsync == null)
				return false;

			RecreateWebRequest(await unauthorizedResponseAsync);
			return true;
		}

		private async Task HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
		{
			if (conventions.HandleForbiddenResponseAsync == null)
				return;

			var forbiddenResponseAsync = conventions.HandleForbiddenResponseAsync(forbiddenResponse);
			if (forbiddenResponseAsync == null)
				return;

			await forbiddenResponseAsync;
		}

		private void RecreateWebRequest(Action<HttpWebRequest> action)
		{
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything

			var newWebRequest = (HttpWebRequest)WebRequest.Create(Url);
			newWebRequest.Method = webRequest.Method;
			HttpRequestHelper.CopyHeaders(webRequest, newWebRequest);
			newWebRequest.UseDefaultCredentials = webRequest.UseDefaultCredentials;
			newWebRequest.Credentials = webRequest.Credentials;
			action(newWebRequest);

			if (postedData != null)
			{
				HttpRequestHelper.WriteDataToRequest(newWebRequest, postedData, factory.DisableRequestCompression);
			}
			if (postedStream != null)
			{
				postedStream.Position = 0;
				using (var stream = newWebRequest.GetRequestStream())
				using (var commpressedData = new GZipStream(stream, CompressionMode.Compress))
				{
					if (factory.DisableRequestCompression == false)
						postedStream.CopyTo(commpressedData);
					else
						postedStream.CopyTo(stream);

					commpressedData.Flush();
					stream.Flush();
				}
			}
			webRequest = newWebRequest;
		}

		private RavenJToken ReadJsonInternal(Func<WebResponse> getResponse)
		{
			WebResponse response;
			try
			{
				response = getResponse();
				sp.Stop();
			}
			catch (WebException e)
			{
				sp.Stop();
				var result = HandleErrors(e);
				if (result == null)
					throw;
				return result;
			}

			ResponseHeaders = new NameValueCollection(response.Headers);
			ResponseStatusCode = ((HttpWebResponse)response).StatusCode;

			HandleReplicationStatusChanges(ResponseHeaders, primaryUrl, operationUrl);

			using (response)
			using (var responseStream = response.GetResponseStreamWithHttpDecompression())
			{
				var data = RavenJToken.TryLoad(responseStream);

				if (Method == "GET" && ShouldCacheRequest)
				{
					factory.CacheResponse(Url, data, ResponseHeaders);
				}

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)ResponseStatusCode,
					Status = RequestStatus.SentToServer,
					Result = (data ?? "").ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return data;
			}
		}

		private async Task<RavenJToken> ReadJsonInternalAsync()
		{
			HandleReplicationStatusChanges(ResponseHeaders, primaryUrl, operationUrl);

			using (var responseStream = await Response.GetResponseStreamWithHttpDecompression())
			{
				var data = await RavenJToken.TryLoadAsync(responseStream);

				if (Method == "GET" && ShouldCacheRequest)
				{
					factory.CacheResponse(Url, data, Response.Headers);
				}

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int) ResponseStatusCode,
					Status = RequestStatus.SentToServer,
					Result = (data ?? "").ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return data;
			}
		}

		private RavenJToken HandleErrors(WebException e)
		{
			var httpWebResponse = e.Response as HttpWebResponse;
			if (httpWebResponse == null ||
				httpWebResponse.StatusCode == HttpStatusCode.Unauthorized ||
				httpWebResponse.StatusCode == HttpStatusCode.NotFound ||
				httpWebResponse.StatusCode == HttpStatusCode.Conflict)
			{
				int httpResult = -1;
				if (httpWebResponse != null)
					httpResult = (int)httpWebResponse.StatusCode;

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = httpResult,
					Status = RequestStatus.ErrorOnServer,
					Result = e.Message,
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return null;//throws
			}

			if (httpWebResponse.StatusCode == HttpStatusCode.NotModified
				&& CachedRequestDetails != null)
			{
				factory.UpdateCacheTime(this);
				var result = factory.GetCachedResponse(this, httpWebResponse.Headers);

				HandleReplicationStatusChanges(httpWebResponse.Headers, primaryUrl, operationUrl);

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)httpWebResponse.StatusCode,
					Status = RequestStatus.Cached,
					Result = result.ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return result;
			}


			using (var sr = new StreamReader(e.Response.GetResponseStreamWithHttpDecompression()))
			{
				var readToEnd = sr.ReadToEnd();

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)httpWebResponse.StatusCode,
					Status = RequestStatus.Cached,
					Result = readToEnd,
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				if (string.IsNullOrWhiteSpace(readToEnd))
					return null;// throws

				RavenJObject ravenJObject;
				try
				{
					ravenJObject = RavenJObject.Parse(readToEnd);
				}
				catch (Exception)
				{
					throw new InvalidOperationException(readToEnd, e);
				}
				if (ravenJObject.ContainsKey("IndexDefinitionProperty"))
				{
					throw new IndexCompilationException(ravenJObject.Value<string>("Message"))
					{
						IndexDefinitionProperty = ravenJObject.Value<string>("IndexDefinitionProperty"),
						ProblematicText = ravenJObject.Value<string>("ProblematicText")
					};
				}
                if (httpWebResponse.StatusCode == HttpStatusCode.BadRequest && ravenJObject.ContainsKey("Message"))
			    {
                    throw new BadRequestException(ravenJObject.Value<string>("Message"), e);
			    }
				if (ravenJObject.ContainsKey("Error"))
				{
					var sb = new StringBuilder();
					foreach (var prop in ravenJObject)
					{
						if (prop.Key == "Error")
							continue;

						sb.Append(prop.Key).Append(": ").AppendLine(prop.Value.ToString(Formatting.Indented));
					}

					if (sb.Length > 0)
						sb.AppendLine();
					sb.Append(ravenJObject.Value<string>("Error"));

					throw new InvalidOperationException(sb.ToString(), e);
				}
				throw new InvalidOperationException(readToEnd, e);
			}
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public HttpJsonRequest AddOperationHeaders(IDictionary<string, string> operationsHeaders)
		{
			foreach (var header in operationsHeaders)
			{
				webRequest.Headers[header.Key] = header.Value;
			}
			return this;
		}

		/// <summary>
		/// Adds the operation header.
		/// </summary>
		public HttpJsonRequest AddOperationHeader(string key, string value)
		{
			webRequest.Headers[key] = value;
			return this;
		}

		public HttpJsonRequest AddReplicationStatusHeaders(string thePrimaryUrl, string currentUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior, Action<NameValueCollection, string, string> handleReplicationStatusChanges)
		{
			if (thePrimaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
				return this;
			if (replicationInformer.GetFailureCount(thePrimaryUrl) <= 0)
				return this; // not because of failover, no need to do this.

			var lastPrimaryCheck = replicationInformer.GetFailureLastCheck(thePrimaryUrl);
			webRequest.Headers.Add(Constants.RavenClientPrimaryServerUrl, ToRemoteUrl(thePrimaryUrl));
			webRequest.Headers.Add(Constants.RavenClientPrimaryServerLastCheck, lastPrimaryCheck.ToString("s"));

			primaryUrl = thePrimaryUrl;
			operationUrl = currentUrl;

			HandleReplicationStatusChanges = handleReplicationStatusChanges;

			return this;
		}

		private static string ToRemoteUrl(string primaryUrl)
		{
			var uriBuilder = new UriBuilder(primaryUrl);
			if (uriBuilder.Host == "localhost" || uriBuilder.Host == "127.0.0.1")
				uriBuilder.Host = Environment.MachineName;
			return uriBuilder.Uri.ToString();
		}

		/// <summary>
		/// The request duration
		/// </summary>
		public double CalculateDuration()
		{
			return sp.ElapsedMilliseconds;
		}

		/// <summary>
		/// Gets or sets the response status code.
		/// </summary>
		/// <value>The response status code.</value>
		public HttpStatusCode ResponseStatusCode { get; set; }

		///<summary>
		/// Whatever we can skip the server check and directly return the cached result
		///</summary>
		public bool SkipServerCheck { get; set; }

		/// <summary>
		/// The underlying request content type
		/// </summary>
		public string ContentType
		{
			get { return webRequest.ContentType; }
			set { webRequest.ContentType = value; }
		}

		public TimeSpan Timeout
		{
			set { webRequest.Timeout = (int) value.TotalMilliseconds; }
		}

		public HttpResponseMessage Response { get; private set; }

		private void WriteMetadata(RavenJObject metadata)
		{
			if (metadata == null || metadata.Count == 0)
			{
				return;
			}

			foreach (var prop in metadata)
			{
				if (prop.Value == null)
					continue;

				if (prop.Value.Type == JTokenType.Object ||
					prop.Value.Type == JTokenType.Array)
					continue;

				var headerName = prop.Key;
				if (headerName == "ETag")
					headerName = "If-None-Match";
				var value = prop.Value.Value<object>().ToString();

				bool isRestricted;
				try
				{
					isRestricted = WebHeaderCollection.IsRestricted(headerName);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Could not figure out how to treat header: " + headerName, e);
				}
				// Restricted headers require their own special treatment, otherwise an exception will
				// be thrown.
				// See http://msdn.microsoft.com/en-us/library/78h415ay.aspx
				if (isRestricted)
				{
					switch (headerName)
					{
						/*case "Date":
						case "Referer":
						case "Content-Length":
						case "Expect":
						case "Range":
						case "Transfer-Encoding":
						case "User-Agent":
						case "Proxy-Connection":
						case "Host": // Host property is not supported by 3.5
							break;*/
						case "Content-Type":
							webRequest.ContentType = value;
							break;
						case "If-Modified-Since":
							DateTime tmp;
							DateTime.TryParse(value, out tmp);
							webRequest.IfModifiedSince = tmp;
							break;
						case "Accept":
							webRequest.Accept = value;
							break;
						case "Connection":
							webRequest.Connection = value;
							break;
					}
				}
				else
				{
					webRequest.Headers[headerName] = value;
				}
			}
		}

		/// <summary>
		/// Writes the specified data.
		/// </summary>
		/// <param name="data">The data.</param>
		public void Write(string data)
		{
			writeCalled = true;
			postedData = data;

			HttpRequestHelper.WriteDataToRequest(webRequest, data, factory.DisableRequestCompression);
		}

		public void Write(Stream streamToWrite)
		{
			writeCalled = true;
			postedStream = streamToWrite;
			webRequest.SendChunked = true;
			using (var stream = webRequest.GetRequestStream())
			using (var commpressedData = new GZipStream(stream, CompressionMode.Compress))
			{
				streamToWrite.CopyTo(commpressedData);

				commpressedData.Flush();
				stream.Flush();
			}
		}

		public async Task<IObservable<string>> ServerPullAsync()
		{
			int retries = 0;
			while (true)
			{
				ErrorResponseException webException;

				try
				{
					Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url), HttpCompletionOption.ResponseHeadersRead);
					await CheckForErrorsAsync();

					var stream = await Response.GetResponseStreamWithHttpDecompression();
					var observableLineStream = new ObservableLineStream(stream, () => Response.Dispose());
					observableLineStream.Start();
					return (IObservable<string>) observableLineStream;
				}
				catch (ErrorResponseException e)
				{
					if (++retries >= 3 || disabledAuthRetries)
						throw;

					if (e.StatusCode != HttpStatusCode.Unauthorized &&
					    e.StatusCode != HttpStatusCode.Forbidden &&
					    e.StatusCode != HttpStatusCode.PreconditionFailed)
						throw;

					webException = e;
				}

				if (webException.StatusCode == HttpStatusCode.Forbidden)
				{
					await HandleForbiddenResponseAsync(webException.Response);
					await new CompletedTask(webException).Task; // Throws, preserving original stack
				}

				if (await HandleUnauthorizedResponseAsync(webException.Response) == false)
					await new CompletedTask(webException).Task; // Throws, preserving original stack
			}
		}

		public async Task ExecuteWriteAsync(string data)
		{
			await WriteAsync(data);
			await ExecuteRequestAsync();
		}

		public async Task ExecuteWriteAsync(byte[] data)
		{
			await WriteAsync(new MemoryStream(data));
			await ExecuteRequestAsync();
		}

		private async Task WriteAsync(Stream streamToWrite)
		{
			postedStream = streamToWrite;

			using (postedStream)
			using (var dataStream = new GZipStream(postedStream, CompressionMode.Compress))
			{
				Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
				{
					Content = new StreamContent(dataStream)
				});

				if (Response.IsSuccessStatusCode == false)
					throw new ErrorResponseException(Response);
			}
		}

		public async Task WriteAsync(string data)
		{
			writeCalled = true;
			Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
			{
				Content = new CompressedStringContent(data, factory.DisableRequestCompression),
			});

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);
		}

		public Task<Stream> GetRawRequestStream()
		{
			webRequest.SendChunked = true;
			return Task.Factory.FromAsync<Stream>(webRequest.BeginGetRequestStream, webRequest.EndGetRequestStream, null);
		}

		public WebResponse RawExecuteRequest()
		{
			try
			{
				return webRequest.GetResponse();
			}
			catch (WebException we)
			{
				var httpWebResponse = we.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				var sb = new StringBuilder()
					.Append(httpWebResponse.StatusCode)
					.Append(" ")
					.Append(httpWebResponse.StatusDescription)
					.AppendLine();

				using (var reader = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression()))
				{
					string line;
					while ((line = reader.ReadLine()) != null)
					{
						sb.AppendLine(line);
					}
				}
				throw new InvalidOperationException(sb.ToString(), we);
			}
		}

		public async Task<WebResponse> RawExecuteRequestAsync()
		{
			try
			{
				return await webRequest.GetResponseAsync();
			}
			catch (WebException we)
			{
				var httpWebResponse = we.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				var sb = new StringBuilder()
					.Append(httpWebResponse.StatusCode)
					.Append(" ")
					.Append(httpWebResponse.StatusDescription)
					.AppendLine();

				using (var reader = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression()))
				{
					string line;
					while ((line = reader.ReadLine()) != null)
					{
						sb.AppendLine(line);
					}
				}
				throw new InvalidOperationException(sb.ToString(), we);
			}
		}

		public void PrepareForLongRequest()
		{
			Timeout = TimeSpan.FromHours(6);
			webRequest.AllowWriteStreamBuffering = false;
		}
	}
}
#endif