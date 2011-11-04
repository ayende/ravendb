using System;
using System.Net;
using System.Text;

namespace Raven.Client.Connection
{
	public class HttpJsonRequestHelper
	{
		public static void WriteDataToRequest(HttpWebRequest req, string data)
		{
			var byteArray = Encoding.UTF8.GetBytes(data);

			req.ContentLength = byteArray.Length;

			using (var dataStream = req.GetRequestStream())
			{
				dataStream.Write(byteArray, 0, byteArray.Length);
				dataStream.Flush();
			}
		}

		public static void CopyHeaders(HttpWebRequest src, HttpWebRequest dest)
		{
			foreach (string header in src.Headers)
			{
				var values = src.Headers.GetValues(header);
				if (values == null)
					continue;
				if (WebHeaderCollection.IsRestricted(header))
				{
					switch (header)
					{
						case "Accept":
							dest.Accept = src.Accept;
							break;
						case "Connection":
							// explicitly ignoring this
							break;
						case "Content-Length":
							dest.ContentLength = src.ContentLength;
							break;
						case "Content-Type":
							dest.ContentType = src.ContentType;
							break;
						case "Date":
							break;
						case "Expect":
							// explicitly ignoring this
							break;
#if !NET_3_5
						case "Host":
							dest.Host = src.Host;
							break;
#endif
						case "If-Modified-Since":
							dest.IfModifiedSince = src.IfModifiedSince;
							break;
						case "Range":
							throw new NotSupportedException("Range copying isn't supported at this stage, we don't support range queries anyway, so it shouldn't matter");
						case "Referer":
							dest.Referer = src.Referer;
							break;
						case "Transfer-Encoding":
							dest.TransferEncoding = src.TransferEncoding;
							break;
						case "User-Agent":
							dest.UserAgent = src.UserAgent;
							break;
						case "Proxy-Connection":
							dest.Proxy = src.Proxy;
							break;
						default:
							throw new ArgumentException(string.Format("No idea how to handle restricted header: '{0}'", header));
					}
				}
				else
				{
					foreach (var value in values)
					{
						dest.Headers.Add(header, value);
					}
				}
			}
		}

	}
}