using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Server.Utils.Metrics;
using Sparrow;

namespace Raven.Server.Documents
{
    public class SubscriptionConnectionState:IDisposable
    {
        private readonly AsyncManualResetEvent _connectionInUse = new AsyncManualResetEvent();

        public SubscriptionConnectionState(SubscriptionConnectionOptions currentConnection, MetricsScheduler metricsScheduler)
        {
            _currentConnection = currentConnection;
            DocsRate = new MeterMetric(metricsScheduler);
            _connectionInUse.Set();
        }

        private SubscriptionConnectionOptions _currentConnection;
        internal readonly MeterMetric DocsRate;


        public SubscriptionConnectionOptions Connection => _currentConnection;

        

        

        // we should have two locks: one lock for a connection and one lock for operations
        // remember to catch ArgumentOutOfRangeException for timeout problems
        public async Task<IDisposable> RegisterSubscriptionConnection(
            SubscriptionConnectionOptions incomingConnection,
            int timeToWait)
        {
            if (await _connectionInUse.WaitAsync(timeToWait) == false)
            {
                switch (incomingConnection.Strategy)
                {
                    // we try to connect, if the resource is occupied, we will throw an exception
                    // this piece of code could have been upper, but we choose to have it here, for better readability
                    case SubscriptionOpeningStrategy.WaitForFree:
                        throw new TimeoutException();
                    case SubscriptionOpeningStrategy.OpenIfFree:
                        throw new SubscriptionInUseException(
                            $"Subscription {incomingConnection.SubscriptionId} is occupied, connection cannot be opened");
                    case SubscriptionOpeningStrategy.TakeOver:
                        if (_currentConnection?.Strategy == SubscriptionOpeningStrategy.ForceAndKeep)
                            throw new SubscriptionInUseException(
                                $"Subscription {incomingConnection.SubscriptionId} is occupied by a ForceAndKeep connection, connectionId cannot be opened");
                        _currentConnection.ConnectionException = new SubscriptionClosedException("Closed by Takeover");
                        _currentConnection?.CancellationTokenSource.Cancel();
                        
                        throw new TimeoutException();
                    case SubscriptionOpeningStrategy.ForceAndKeep:
                        _currentConnection.ConnectionException = new SubscriptionClosedException("Closed by ForceAndKeep");
                        _currentConnection?.CancellationTokenSource.Cancel();
                        
                        throw new TimeoutException();
                    default:
                        throw new InvalidOperationException("Unknown subscription open strategy: " +
                                                            incomingConnection.Strategy);
                }
            }

            _connectionInUse.Reset();
            _currentConnection = incomingConnection;
            return new DisposableAction(() => {
                _connectionInUse.SetByAsyncCompletion();
                _currentConnection = null;
            });
        }

        public void EndConnection()
        {
            _currentConnection?.CancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            EndConnection();
            DocsRate?.Dispose();
        }
    }
}