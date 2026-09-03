using System;
using System.IO;
using System.Linq;
using Voron.Global;

namespace Voron.Impl.Journal
{
    // Temporary probe for RavenDB-27377: traces every flush attempt so we can see how often the
    // flusher actually runs, what each attempt covered, and whether an old active transaction is
    // starving it of coverage. Enabled only when RAVEN_FLUSHTRACE is set to a directory path.
    internal static class FlushProbe
    {
        private static readonly string Dir = Environment.GetEnvironmentVariable("RAVEN_FLUSHTRACE");
        public static readonly bool Enabled = string.IsNullOrEmpty(Dir) == false;
        private static readonly object Lock = new object();

        public static void Log(StorageEnvironment env, string line)
        {
            if (Enabled == false)
                return;

            try
            {
                var name = Path.GetFileName(env.Options.BasePath.FullPath.TrimEnd('/', '\\'));
                var msg = $"{DateTime.UtcNow:HH:mm:ss.fff}|{name}|{line}{Environment.NewLine}";
                lock (Lock)
                {
                    Directory.CreateDirectory(Dir);
                    File.AppendAllText(Path.Combine(Dir, "flushtrace.log"), msg);
                }
            }
            catch
            {
                // never let the probe take down a flush
            }
        }

        public static string Snapshot(StorageEnvironment env, long uptoTxIdExclusive)
        {
            long newest = env.CurrentStateRecord.TransactionId;
            var active = env.ActiveTransactions.AllTransactions;
            active.Sort((a, b) => a.Id.CompareTo(b.Id));
            string oldest3 = string.Join(",", active.Take(3).Select(t =>
                $"{t.Id}:{(t.Flags == TransactionFlags.ReadWrite ? "W" : "R")}{(t.AsyncCommit ? "a" : "")}:{t.CallerName ?? "?"}"));
            (long files, long inUsePages, long capacityPages) = env.ScratchBufferPool.GetFlushProbeTotals();
            return $"upto={uptoTxIdExclusive}|newest={newest}|atx={active.Count}|oldest3=[{oldest3}]|" +
                   $"scrFiles={files}|scrInUseMB={inUsePages * Constants.Storage.PageSize / 1024 / 1024}|scrCapMB={capacityPages * Constants.Storage.PageSize / 1024 / 1024}";
        }
    }
}
