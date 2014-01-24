﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Controllers;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public abstract class RavenBaseApiController : ApiController
	{
		protected static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private HttpRequestMessage request;

		public HttpRequestMessage InnerRequest
		{
			get
			{
				return Request ?? request;
			}
		}

		public HttpHeaders InnerHeaders
		{
			get
			{
				var headers = new Headers();
				foreach (var header in InnerRequest.Headers)
				{
					if (header.Value.Count() == 1)
						headers.Add(header.Key, header.Value.First());
					else
						headers.Add(header.Key, header.Value.ToList());
				}

				if (InnerRequest.Content == null)
					return headers;

				foreach (var header in InnerRequest.Content.Headers)
				{
					if (header.Value.Count() == 1)
						headers.Add(header.Key, header.Value.First());
					else
						headers.Add(header.Key, header.Value.ToList());
				}

				return headers;
			}
		}

		public new IPrincipal User
		{
			get;
			set;
		}

		protected virtual void InnerInitialization(HttpControllerContext controllerContext)
		{
			request = controllerContext.Request;
			User = controllerContext.RequestContext.Principal;
		}

		public async Task<T> ReadJsonObjectAsync<T>()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			//using(var gzipStream = new GZipStream(stream, CompressionMode.Decompress))
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			{
				using (var jsonReader = new JsonTextReader(streamReader))
				{
					var result = JsonExtensions.CreateDefaultJsonSerializer();

					return (T)result.Deserialize(jsonReader, typeof(T));
				}
			}
		}

		public async Task<RavenJObject> ReadJsonAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJObject.Load(jsonReader);
		}

		public async Task<RavenJArray> ReadJsonArrayAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
			using (var jsonReader = new RavenJsonTextReader(streamReader))
				return RavenJArray.Load(jsonReader);
		}

		public async Task<string> ReadStringAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
				return streamReader.ReadToEnd();
		}

		public async Task<RavenJArray> ReadBsonArrayAsync()
		{
			using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
			using (var jsonReader = new BsonReader(stream))
			{
				var jObject = RavenJObject.Load(jsonReader);
				return new RavenJArray(jObject.Values<RavenJToken>());
			}
		}

		private Encoding GetRequestEncoding()
		{
			if (InnerRequest.Content.Headers.ContentType == null || string.IsNullOrWhiteSpace(InnerRequest.Content.Headers.ContentType.CharSet))
				return Encoding.GetEncoding(Constants.DefaultRequestEncoding);
			return Encoding.GetEncoding(InnerRequest.Content.Headers.ContentType.CharSet);
		}

		public int GetStart()
		{
			int start;
			int.TryParse(GetQueryStringValue("start"), out start);
			return Math.Max(0, start);
		}

		public int GetPageSize(int maxPageSize)
		{
			int pageSize;
			if (int.TryParse(GetQueryStringValue("pageSize"), out pageSize) == false || pageSize < 0)
				pageSize = 25;
			if (pageSize > maxPageSize)
				pageSize = maxPageSize;
			return pageSize;
		}

		public bool MatchEtag(Etag etag)
		{
			return EtagHeaderToEtag() == etag;
		}

		internal Etag EtagHeaderToEtag()
		{
			var responseHeader = GetHeader("If-None-Match");
			if (string.IsNullOrEmpty(responseHeader))
				return Etag.InvalidEtag;

			if (responseHeader[0] == '\"')
				return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

			return Etag.Parse(responseHeader);
		}

		public string GetQueryStringValue(string key)
		{
			return GetQueryStringValue(InnerRequest, key);
		}

		public static string GetQueryStringValue(HttpRequestMessage req, string key)
		{
			return req.GetQueryNameValuePairs().Where(pair => pair.Key == key).Select(pair => pair.Value).FirstOrDefault();
		}

		public string[] GetQueryStringValues(string key)
		{
			var items = InnerRequest.GetQueryNameValuePairs().Where(pair => pair.Key == key);
			return items.Select(pair => pair.Value).ToArray();
		}

		public Etag GetEtagFromQueryString()
		{
			var etagAsString = GetQueryStringValue("etag");
			return etagAsString != null ? Etag.Parse(etagAsString) : null;
		}

		public void WriteETag(Etag etag, HttpResponseMessage msg)
		{
			if (etag == null)
				return;
			WriteETag(etag.ToString(), msg);
		}

		public void WriteETag(string etag, HttpResponseMessage msg)
		{
			if (string.IsNullOrWhiteSpace(etag))
				return;
			//string clientVersion = GetHeader("Raven-Client-Version");
			//if (string.IsNullOrEmpty(clientVersion))
			//{
			//	msg.Headers.ETag = new EntityTagHeaderValue(etag);
			//	return;
			//}

			msg.Headers.ETag = new EntityTagHeaderValue("\"" + etag + "\"");
		}

		public void WriteHeaders(RavenJObject headers, Etag etag, HttpResponseMessage msg)
		{
			foreach (var header in headers)
			{
				if (header.Key.StartsWith("@"))
					continue;

				switch (header.Key)
				{
					case "Content-Type":
						var headerValue = header.Value.Value<string>();
						string charset = null;
						if (headerValue.Contains("charset="))
						{
							var splits = headerValue.Split(';');
							headerValue = splits[0];

							charset = splits[1].Split('=')[1];
						}

						msg.Content.Headers.ContentType = new MediaTypeHeaderValue(headerValue) { CharSet = charset };

						break;
					default:
						if (header.Value.Type == JTokenType.Date)
						{
							var rfc1123 = GetDateString(header.Value, "r");
							var iso8601 = GetDateString(header.Value, "o");
							msg.Content.Headers.Add(header.Key, rfc1123);
							if (header.Key.StartsWith("Raven-") == false)
								msg.Content.Headers.Add("Raven-" + header.Key, iso8601);
						}
						else
						{
							var value = UnescapeStringIfNeeded(header.Value.ToString(Formatting.None));
							msg.Content.Headers.Add(header.Key, value);
						}
						break;
				}
			}
			if (headers["@Http-Status-Code"] != null)
			{
				msg.StatusCode = (HttpStatusCode)headers.Value<int>("@Http-Status-Code");
				msg.Content.Headers.Add("Temp-Status-Description", headers.Value<string>("@Http-Status-Description"));
			}

			WriteETag(etag, msg);
		}

		public void AddHeader(string key, string value, HttpResponseMessage msg)
		{
			if (msg.Content == null)
				msg.Content = new JsonContent();

            // Ensure we haven't already appended these values.
            IEnumerable<string> existingValues;
            var hasExistingHeaderAppended = msg.Content.Headers.TryGetValues(key, out existingValues) && existingValues.Any(v => v == value);
            if (!hasExistingHeaderAppended)
            {
                msg.Content.Headers.Add(key, value);
            }
		}

		private string GetDateString(RavenJToken token, string format)
		{
			var value = token as RavenJValue;
			if (value == null)
				return token.ToString();

			var obj = value.Value;

			if (obj is DateTime)
				return ((DateTime)obj).ToString(format);

			if (obj is DateTimeOffset)
				return ((DateTimeOffset)obj).ToString(format);

			return obj.ToString();
		}

		private static string UnescapeStringIfNeeded(string str)
		{
			if (str.StartsWith("\"") && str.EndsWith("\""))
				str = Regex.Unescape(str.Substring(1, str.Length - 2));
			if (str.Any(ch => ch > 127))
			{
				// contains non ASCII chars, needs encoding
				return Uri.EscapeDataString(str);
			}
			return str;
		}

		public virtual HttpResponseMessage GetMessageWithObject(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var token = item as RavenJToken;
			if (token == null && item != null)
			{
				token = RavenJToken.FromObject(item);
			}

            bool metadataOnly;
            if (bool.TryParse(GetQueryStringValue("metadata-only"), out metadataOnly) && metadataOnly)
                token = HttpExtensions.MinimizeToken(token);
            
			var msg = new HttpResponseMessage(code)
			{
				Content = new JsonContent(token),
			};

			WriteETag(etag, msg);

			return msg;
		}

		public virtual HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var resMsg = new HttpResponseMessage(code)
			{
				Content = new JsonContent(msg)
			};

			WriteETag(etag, resMsg);

			return resMsg;
		}

		public virtual HttpResponseMessage GetEmptyMessage(HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var resMsg = new HttpResponseMessage(code)
			{
				Content = new JsonContent()
			};
			WriteETag(etag, resMsg);
			return resMsg;
		}

		public HttpResponseMessage WriteData(RavenJObject data, RavenJObject headers, Etag etag, HttpStatusCode status = HttpStatusCode.OK, HttpResponseMessage msg = null)
		{
			if (msg == null)
				msg = GetEmptyMessage(status);

			var jsonContent = ((JsonContent)msg.Content);

			WriteHeaders(headers, etag, msg);

			var jsonp = GetQueryStringValue("jsonp");
			if (string.IsNullOrEmpty(jsonp) == false)
				jsonContent.Jsonp = jsonp;

			jsonContent.Data = data;

			return msg;
		}

		public Etag GetEtag()
		{
			var etagAsString = GetHeader("If-None-Match") ?? GetHeader("If-Match");
			if (etagAsString != null)
			{
				// etags are usually quoted
				if (etagAsString.StartsWith("\"") && etagAsString.EndsWith("\""))
					etagAsString = etagAsString.Substring(1, etagAsString.Length - 2);

				Etag result;
				if (Etag.TryParse(etagAsString, out result))
					return result;

				throw new BadRequestException("Could not parse If-None-Match or If-Match header as Guid");
			}

			return null;
		}

		public string GetHeader(string key)
		{
			if (InnerHeaders.Contains(key) == false)
				return null;
			return InnerHeaders.GetValues(key).FirstOrDefault();
		}

		public List<string> GetHeaders(string key)
		{
			if (InnerHeaders.Contains(key) == false)
				return null;
			return InnerHeaders.GetValues(key).ToList();
		}

		public bool HasCookie(string key)
		{
			return InnerRequest.Headers.GetCookies(key).Count != 0;
		}

		public string GetCookie(string key)
		{
			var cookieHeaderValue = InnerRequest.Headers.GetCookies(key).FirstOrDefault();
			if (cookieHeaderValue != null)
			{
				var coockie = cookieHeaderValue.Cookies.FirstOrDefault();
				if (coockie != null)
					return coockie.Value;
			}

			return null;
		}

		public HttpResponseMessage WriteEmbeddedFile(string ravenPath, string docPath)
		{
			var filePath = Path.Combine(ravenPath, docPath);
			var type = GetContentType(docPath);
			if (File.Exists(filePath))
				return WriteFile(filePath);
			return WriteEmbeddedFileOfType(docPath, type);
		}

		public HttpResponseMessage WriteFile(string filePath)
		{
			var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-None-Match");
			var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
			if (etagValue == fileEtag)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var msg = new HttpResponseMessage
			{
				Content = new StreamContent(new FileStream(filePath, FileMode.Open))
			};

			WriteETag(fileEtag, msg);

			return msg;
		}

		private HttpResponseMessage WriteEmbeddedFileOfType(string docPath, string type)
		{
			var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-Match");
			var currentFileEtag = EmbeddedLastChangedDate + docPath;
			if (etagValue == currentFileEtag)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			byte[] bytes;
			var resourceName = "Raven.Database.Server.WebUI." + docPath.Replace("/", ".");

			using (var resource = typeof(IHttpContext).Assembly.GetManifestResourceStream(resourceName))
			{
				if (resource == null)
					return GetEmptyMessage(HttpStatusCode.NotFound);

				bytes = resource.ReadData();
			}
			var msg = new HttpResponseMessage
			{
				Content = new ByteArrayContent(bytes),
			};

			msg.Content.Headers.ContentType = new MediaTypeHeaderValue(type);
			WriteETag(etagValue, msg);

			return msg;
		}

		private static readonly string EmbeddedLastChangedDate =
            File.GetLastWriteTime(AssemblyHelper.GetAssemblyLocationFor(typeof(HttpExtensions))).Ticks.ToString("G");

		private static string GetContentType(string docPath)
		{
			switch (Path.GetExtension(docPath))
			{
				case ".html":
				case ".htm":
					return "text/html";
				case ".css":
					return "text/css";
				case ".js":
					return "text/javascript";
				case ".ico":
					return "image/vnd.microsoft.icon";
				case ".jpg":
					return "image/jpeg";
				case ".gif":
					return "image/gif";
				case ".png":
					return "image/png";
				case ".xap":
					return "application/x-silverlight-2";
				default:
					return "text/plain";
			}
		}

		public class Headers : HttpHeaders
		{

		}
	}
}
