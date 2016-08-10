using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using metrics.Core;
using Sparrow.Collections;

namespace Raven.Database.Util
{
    public sealed class MetricsTicker: IDisposable
    {
        private static MetricsTicker _instance;
        private static int hasValue = 0;
        public static bool HasValue
        {
            get { return hasValue != 0; }
        }
        public static MetricsTicker Instance
        {
            get
            {
                if (Interlocked.CompareExchange(ref hasValue, 1, 0) == 0)
                {
                    _instance = new MetricsTicker();
                }
                while (_instance == null)
                {
                }

                return _instance;
            }
        }

        static MetricsTicker() {}

        private MetricsTicker()
        {
            perSecondCounterMetricsTask = CreateTask(1000, perSecondCounterMetrics);
            meterMetricsTask = CreateTask(5000, meterMetrics);
        }

        private Task perSecondCounterMetricsTask;
        private Task meterMetricsTask;
        private readonly ConcurrentSet<ICounterMetric> perSecondCounterMetrics =
            new ConcurrentSet<ICounterMetric>();
        private readonly ConcurrentSet<ICounterMetric> meterMetrics = new ConcurrentSet<ICounterMetric>();
        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        public void AddPerSecondCounterMetric(PerSecondCounterMetric newPerSecondCounterMetric)
        {
            perSecondCounterMetrics.Add(newPerSecondCounterMetric);
        }

        public void RemovePerSecondCounterMetric(PerSecondCounterMetric perSecondCounterMetricToRemove)
        {
            perSecondCounterMetrics.TryRemove(perSecondCounterMetricToRemove);
        }

        public void AddMeterMetric(MeterMetric newMeterMetric)
        {
            meterMetrics.Add(newMeterMetric);
        }

        public void RemoveMeterMetric(MeterMetric meterMetricToRemove)
        {
            meterMetrics.TryRemove(meterMetricToRemove);
        }

        private Task CreateTask(int baseInterval, ConcurrentSet<ICounterMetric> concurrentSet)
        {
            return Task.Run(async () =>
            {
                long elapsed = 0;

                while (true)
                {
                    try
                    {
                        if (cts.IsCancellationRequested)
                            return;

                        var milliseconds = baseInterval - elapsed;
                        if (milliseconds > 0)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(milliseconds), cts.Token).ConfigureAwait(false);
                        }

                        var sp = Stopwatch.StartNew();
                        foreach (var ticker in concurrentSet)
                        {
                            if (cts.IsCancellationRequested)
                                return;

                            ticker.Tick();
                        }
                        elapsed = sp.ElapsedMilliseconds;
                    }
                    catch (TaskCanceledException)
                    {
                        return;
                    }
                }
            }, cts.Token);
        }

        public void Dispose()
        {
            cts.Cancel();
        }
    }
}