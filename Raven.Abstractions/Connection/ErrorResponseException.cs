using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Connection
{
    [Serializable]
    public class ErrorResponseException : Exception
    {
	    public HttpResponseMessage Response { get; private set; }

        public HttpStatusCode StatusCode
        {
            get { return Response.StatusCode; }
        }

	    public ErrorResponseException(ErrorResponseException e, string message)
            :base(message)
	    {
	        Response = e.Response;
	        ResponseString = e.ResponseString;
	    }

        public ErrorResponseException(HttpResponseMessage response, string msg, Exception exception)
            : base(msg, exception)
        {
            Response = response;
        }

        public ErrorResponseException(HttpResponseMessage response, string msg, string responseString= null)
            : base(msg)
        {
            Response = response;
            ResponseString = responseString;
        }

		private static readonly ConcurrentDictionary<Tuple<HttpStatusCode, string>, ErrorResponseException> exceptionsWithErrorStringCache =
			new ConcurrentDictionary<Tuple<HttpStatusCode, string>, ErrorResponseException>();

		private static readonly ConcurrentDictionary<HttpStatusCode, ErrorResponseException> exceptionsCache =
			new ConcurrentDictionary<HttpStatusCode, ErrorResponseException>();

		private const string statusCodeWithResponseString = "Status code: {0}{1}{2}";
		private const string statusCode = "Status code: {0}";

		public static ErrorResponseException FromResponseMessage(HttpResponseMessage response, bool readErrorString = true)
		{
			if (readErrorString && response.Content != null)
			{
                var readAsStringAsync = response.GetResponseStreamWithHttpDecompression();
			    if (readAsStringAsync.IsCompleted)
			    {
			        using (var streamReader = new StreamReader(readAsStringAsync.Result))
			        {
				        var responseString = streamReader.ReadToEnd();
				        return exceptionsWithErrorStringCache.GetOrAdd(Tuple.Create(response.StatusCode, responseString), key =>
				        {
					        var responseRef = response;
					        return new ErrorResponseException(responseRef, String.Format(statusCodeWithResponseString,key.Item1,Environment.NewLine,key.Item2))
							{
								ResponseString = key.Item2
							};
						});
			        }
			    }
			}

			return exceptionsCache.GetOrAdd(response.StatusCode, key => new ErrorResponseException(response, string.Format(statusCode, response.StatusCode)));
		}

	    public string ResponseString { get; private set; }

	    public Etag Etag
	    {
	        get
	        {
	            if (Response.Headers.ETag == null)
	                return null;
                var responseHeader = Response.Headers.ETag.Tag;

	            if (responseHeader[0] == '\"')
                    return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

                return Etag.Parse(responseHeader);
	        }
	    }

        protected ErrorResponseException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }
    }
}