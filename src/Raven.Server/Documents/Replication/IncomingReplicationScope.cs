using System;

namespace Raven.Server
{
    public class IncomingReplicationScope : IDisposable
    {
        [ThreadStatic]
        public static bool IsActive;

        public IncomingReplicationScope()
        {
            IsActive = true;
        }

        public void Dispose()
        {
            IsActive = false;
        }
    }
}
