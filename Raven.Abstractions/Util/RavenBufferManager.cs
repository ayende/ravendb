using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Collections.ObjectModel;
using System.ServiceModel.Channels;
using System.Collections.Concurrent;

namespace Raven.Abstractions
{
	public class RavenBufferManager : BufferManager
	{
		private ConcurrentQueue<byte[]>[] _bufferBuckets = new ConcurrentQueue<byte[]>[0];

		private ConcurrentQueue<byte[]> GetBucket (int bufferSize)
		{
			int bf = log2 ((int)NearestPowerOfTwo(bufferSize));
			if (_bufferBuckets.Length <= bf) {
				lock (this) {
					if (_bufferBuckets.Length <= bf) {
						var old = _bufferBuckets.Length;
						Array.Resize (ref _bufferBuckets, bf + 1);
						for (int i = old; i < _bufferBuckets.Length; i++) {
							_bufferBuckets [i] = new ConcurrentQueue<byte[]> ();
						}
					}
				}
			}
			return _bufferBuckets [bf];
		}

		public override void ReturnBuffer (byte[] buffer)
		{
			GetBucket (buffer.Length).Enqueue (buffer);
		}

		private static int log2 (int n)
		{
			int pos = 0;
			if (n >= 1<<16) {
				n >>= 16;
				pos += 16;
			}
			if (n >= 1<< 8) {
				n >>= 8;
				pos += 8;
			}
			if (n >= 1<< 4) {
				n >>= 4;
				pos += 4;
			}
			if (n >= 1<< 2) {
				n >>= 2;
				pos += 2;
			}
			if (n >= 1<< 1)
				pos += 1;
			return ((n == 0) ? (-1) : pos);
		}

		public override byte[] TakeBuffer (int bufferSize)
		{
			var bucket = GetBucket (bufferSize);
			byte[] buffer;
			if (bucket.TryDequeue (out buffer))
				return buffer;

			var newBufferSize = NearestPowerOfTwo (bufferSize);
			return new byte[newBufferSize];
		}

		public override void Clear ()
		{
			lock (this) {
				_bufferBuckets = new ConcurrentQueue<byte[]>[0];
			}
		}

		private static long NearestPowerOfTwo(long v)
		{
			v--;
			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			v++;
			return v;

		}
	}
}