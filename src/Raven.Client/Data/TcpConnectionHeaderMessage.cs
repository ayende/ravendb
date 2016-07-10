namespace Raven.Abstractions.Data
{
    public class TcpConnectionHeaderMessage
    {
        public enum OperationTypes
        {
            None,
            BulkInsert,
            Subscription,
            Replication
        }

        public string Database { get; set; }
        public OperationTypes Operation { get; set; }
    }
}