using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Client.Connection;
using Xunit;

namespace Raven.Tests.Faceted
{
    public static class ConditionalGetHelper
    {
        private static HttpWebResponse GetHttpResponseHandle304(WebRequest request)
        {
            try
            {
                return (HttpWebResponse)request.GetResponse();
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if (response != null && response.StatusCode == HttpStatusCode.NotModified)
                {
                    return response;
                }

                throw;
            }
        }

        public static Result PerformGet(string url, Guid? requestEtag)
        {
            var getRequest = WebRequest.Create(url);

            if(requestEtag != null)
                getRequest.Headers.Add("If-None-Match", requestEtag.ToString());

            using (var response = GetHttpResponseHandle304(getRequest))
            {
                Guid? responseEtag;
                try
                {
                    responseEtag = response.GetEtagHeader();
                }
                catch (Exception)
                {
                    responseEtag = null;
                }

                return new Result()
                {
                    StatusCode = response.StatusCode,
                    ReponseEtag = responseEtag
                };
            }
        }

        public static Result PerformPost(string url, string payload, Guid? requestEtag)
        {
            var request = WebRequest.Create(url);
            
            if (requestEtag != null)
                request.Headers.Add("If-None-Match", requestEtag.ToString());

            request.Method = "POST";

            byte[] buffer = Encoding.UTF8.GetBytes(payload);

            request.GetRequestStream().Write(buffer, 0, buffer.Length);

            using (var response = GetHttpResponseHandle304(request))
            {
                Guid? responseEtag;
                try
                {
                    responseEtag = response.GetEtagHeader();
                }
                catch (Exception)
                {
                    responseEtag = null;
                }

                return new Result()
                {
                    StatusCode = response.StatusCode,
                    ReponseEtag = responseEtag
                };
            }
        }
    }

    public class Result
    {
        public HttpStatusCode StatusCode { get; set; }
        public Guid? ReponseEtag { get; set; }
    }
}
