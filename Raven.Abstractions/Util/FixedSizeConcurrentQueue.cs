using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.Util
{
    public class FixedSizeConcurrentQueue<T> : IEnumerable<T>
    {
        private ConcurrentQueue<T> queue;
        private int dequeueInProgress;
        private int size;
        private const int Forever = 500;
        public FixedSizeConcurrentQueue(int size)
        {
            this.queue = new ConcurrentQueue<T>();
            this.size = size;
        }

        public FixedSizeConcurrentQueue(int size, IEnumerable<T> other)
        {
            this.queue = new ConcurrentQueue<T>(other.Take(size));
            this.size = size;
        } 

        /// <summary>
        /// Returns the 'size of the queue' oldest elements
        /// the queue may have new elements added to it meanwhile.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            return queue.Take(size)?.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Although this is thread safe the Count may change after reading it.
        /// </summary>
        public int Count => queue.Count;

        /// <summary>
        ///  Although this is thread safe the IsEmpty state may change after reading it.
        /// </summary>
        public bool IsEmpty => queue.IsEmpty;

        public T[] ToArray()
        {
            return queue.Take(size).ToArray();
        }

        public void Enqueue(T item)
        {
            queue.Enqueue(item);
            var otherThreadDequeue = Interlocked.CompareExchange(ref dequeueInProgress, 1, 0);
            //somebody else is cleaning the queue...
            if (1 == otherThreadDequeue) 
                return;
            var now = DateTime.UtcNow;
            // i can relay on Count because i only allow a single dequeue thread!
            var queueSize = queue.Count;
            while (queueSize > size)
            {
                //this is taking forever for some reason i'll leave it for some other thread to clean...
                if ((DateTime.UtcNow - now).TotalMilliseconds > Forever)
                {
                    Interlocked.Exchange(ref dequeueInProgress, 0);
                    return;
                }
                T dontCare;
                if (queue.TryDequeue(out dontCare))
                    --queueSize;
            }
            //done dequeu size should be at most the fixed size of the queue.
            Interlocked.Exchange(ref dequeueInProgress, 0);
        }

        public bool TryDequeue(out T result)
        {
            result = default(T);
            var otherThreadDequeue = Interlocked.CompareExchange(ref dequeueInProgress, 1, 0);
            //somebody else is doing dequeue so this thread doing a dequeue is probably seen an old count value
            //to be safe we will fail the dequeue.
            if (1 == otherThreadDequeue)
                return false;
            var now = DateTime.UtcNow;
            while (true)
            {
                //this is taking forever for some reason i'll fail now
                if ((DateTime.UtcNow - now).TotalMilliseconds > Forever)
                {
                    Interlocked.Exchange(ref dequeueInProgress, 0);
                    return false;
                }
                if (queue.TryDequeue(out result))
                {
                    Interlocked.Exchange(ref dequeueInProgress, 0);
                    return true;
                }
            }
        }

        /// <summary>
        /// The behavior of this method is problematic, i dom't want to lock the queue or mark it as doing dequeue
        /// because that may lead other threads thinking somebody is cleaning the queue and nobody is...
        /// so this peek may yield a value that will be removed from the queue by some other thread!
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool TryPeek(out T result)
        {
            return queue.TryPeek(out result);
        }
    }
}
