using System;
using System.Net;

namespace Raven.Server.Documents.AI.AiGen
{
    /// <summary>Base for all Gen‑AI service errors.</summary>
    public class GenAiException(string message) : Exception(message)
    {
        public required string RequestId { get; set; }
    }

    public sealed class GenAiRefusedToAnswerException(string message) : GenAiException(message)
    {
        public string Refusal;
        public string FinishReason;
    }

    public sealed class GenAiUnexpectedResponseException(string message) : GenAiException(message)
    {
    }

    public class GenUnsuccessfulRequestException : GenAiException
    {
        public HttpStatusCode StatusCode { get; }

        public GenUnsuccessfulRequestException(string message, HttpStatusCode statusCode) : base(message)
        {
            StatusCode = statusCode;
        }
    }

    public class GenAiTooManyRequestsException : GenUnsuccessfulRequestException
    {

        public GenAiTooManyRequestsException(string message) : base(message, HttpStatusCode.TooManyRequests)
        {
        }
    }

    /// <summary>HTTP 429 – too many requests.</summary>
    public sealed class GenAiRateLimitException(string message) : GenAiTooManyRequestsException(message)
    {
        public TimeSpan RetryAfter { get; set; }
    }


    public class GenAiInsufficientQuotaException(string message) : GenAiTooManyRequestsException(message)
    {
    }

    public sealed class GenAiTooManyTokensException(string message) : GenAiTooManyRequestsException(message)
    {
    }


}
