﻿using System;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow
{
    public class IoMeterBuffer
    {
        public class MeterItem
        {
            public long Size;
            public long FileSize;
            public DateTime Start;
            public DateTime End;
            public IoMetrics.MeterType Type;
            public TimeSpan Duration => End - Start;
        }

        public class SummerizedItem
        {
            public long TotalSize;
            public TimeSpan TotalTime;
            public TimeSpan MinTime;
            public TimeSpan MaxTime; 
            public DateTime TotalTimeStart;
            public DateTime TotalTimeEnd;
            public long Count;
            public IoMetrics.MeterType Type;
            public long TotalFileSize;
        }

        public IoMeterBuffer(int metricsBufferSize, int summaryBufferSize)
        {
            _buffer = new MeterItem[metricsBufferSize];
            _summerizedBuffer = new SummerizedItem[summaryBufferSize];
        }

        private readonly MeterItem[] _buffer;
        private int _bufferPos = -1;

        private readonly SummerizedItem[] _summerizedBuffer;
        private int _summerizedPos = -1;

        public IEnumerable<SummerizedItem> GetSummerizedItems()
        {
            for (int pos = 0; pos < _summerizedBuffer.Length; pos++)
            {
                var summerizedItem = _summerizedBuffer[pos];
                if (summerizedItem == null)
                    continue;
                yield return summerizedItem;
            }
        }

        public IEnumerable<MeterItem> GetCurrentItems()
        {
            for (int pos = 0; pos < _buffer.Length; pos++)
            {
                var item = _buffer[pos];
                if (item  == null)
                    continue;
                yield return item;
            }
        }

        public struct DurationMeasurement : IDisposable
        {
            public readonly IoMeterBuffer Parent;
            private readonly IoMetrics.MeterType _type;
            public long Size;
            private readonly DateTime _start;
            private long _fileSize;
            public Action<MeterItem> _onFileChange;

            public DurationMeasurement(IoMeterBuffer parent, IoMetrics.MeterType type, long size, long filesize, Action<MeterItem> onFileChange)
            {
                Parent = parent;
                _type = type;
                Size = size;
                _fileSize = filesize;
                _start = DateTime.UtcNow;
                _onFileChange = onFileChange;
            }

            public void IncrementSize(long size)
            {
                Size += size;
            }

            public void Dispose()
            {
                Parent.Mark(Size, _start, DateTime.UtcNow, _type, _fileSize, _onFileChange);
            }

            public void IncrementFileSize(long fileSize)
            {
                _fileSize += fileSize;
            }

            public void SetFileSize(long fileSize)
            {
                _fileSize = fileSize;
            }
        }

        internal void Mark(long size, DateTime start, DateTime end, IoMetrics.MeterType type, long filesize, Action<MeterItem> onFileChange = null)
        {
            var meterItem = new MeterItem
            {
                Start = start,
                Size = size,            
                FileSize  = filesize,
                Type = type,
                End = end
            };

            onFileChange?.Invoke(meterItem);

            var pos = Interlocked.Increment(ref _bufferPos);
            var adjustedTail = pos%_buffer.Length;

            if (Interlocked.CompareExchange(ref _buffer[adjustedTail], meterItem, null) == null)
                return;

            var newSummary = new SummerizedItem
            {
                TotalTimeStart = meterItem.Start,
                TotalTimeEnd = meterItem.End,
                Count = 1,
                MaxTime = meterItem.Duration,
                MinTime = meterItem.Duration,
                TotalTime = meterItem.Duration,
                TotalSize = meterItem.Size,
                TotalFileSize = meterItem.FileSize,
                Type = meterItem.Type
            };

            for (int i = 0; i < _buffer.Length; i++)
            {
                var oldVal = Interlocked.Exchange(ref _buffer[(adjustedTail + i) % _buffer.Length], null);
                if (oldVal != null)
                {
                    newSummary.TotalTimeStart = newSummary.TotalTimeStart > oldVal.Start ? oldVal.Start : newSummary.TotalTimeStart;
                    newSummary.TotalTimeEnd = newSummary.TotalTimeEnd > oldVal.End ? newSummary.TotalTimeEnd : oldVal.End;
                    newSummary.Count++;
                    newSummary.MaxTime = newSummary.MaxTime > oldVal.Duration ? newSummary.MaxTime : oldVal.Duration;
                    newSummary.MinTime = newSummary.MinTime > oldVal.Duration ? oldVal.Duration : newSummary.MinTime;
                    newSummary.TotalSize += oldVal.Size;
                    newSummary.TotalFileSize = oldVal.FileSize; // take last size to history
                    newSummary.TotalTime += oldVal.Duration;
                }
            }
            var increment = Interlocked.Increment(ref _summerizedPos);
            _summerizedBuffer[increment%_summerizedBuffer.Length] = newSummary;
        }
    }
}