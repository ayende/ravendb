namespace Raven.Abstractions.Data
{
    public class SubscriptionConnectionClientMessage
    {
        public enum MessageType
        {
            None,
            Acknowledge
        }

        public MessageType Type { get; set; }
        public long Etag { get; set; }
    }
}