using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public class SubscriptionConnectionServerMessage
    {
        public enum MessageType
        {
            None,
            CoonectionStatus,
            EndOfBatch,
            Data,
            Confirm,
            Terminated
        }

        public enum ConnectionStatus
        {
            None,
            Accepted,
            InUse,
            Closed,
            NotFound
        }

        public MessageType Type { get; set; }
        public ConnectionStatus Status { get; set; }
        public RavenJObject Data { get; set; }
    }
}