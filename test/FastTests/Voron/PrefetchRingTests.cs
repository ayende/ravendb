using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Server.Utils;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Posix;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public unsafe class PrefetchRingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const int PosixFadviseDontNeed = 4;

    [RavenFact(RavenTestCategory.Voron)]
    public void DataFilesReleaseTheirRingRegistrationWhenClosed()
    {
        const int maxFiles = 64;
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles, queueSize: 64);
        if (ring.Handle == null)
        {
            Output.WriteLine("io_uring is not available here (kernel or seccomp policy): no ring to register with");
            return;
        }

        var path = RavenTestHelper.NewDataPath(nameof(DataFilesReleaseTheirRingRegistrationWhenClosed), 0, forceCreateDir: true);
        var open = new List<(Pager Pager, Pager.State State)>();
        try
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                (Pager, Pager.State) Open(int i) => Pager.Create(options, Path.Combine(path, $"{i}.voron"), 64 * 1024, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                void Close((Pager Pager, Pager.State State) p)
                {
                    p.State.Dispose();
                    p.Pager.Dispose();
                }

                for (var i = 0; i < maxFiles; i++)
                    open.Add(Open(i));

                var full = Assert.ThrowsAny<Exception>(() => Open(maxFiles));
                Assert.Contains(nameof(PalFlags.FailCodes.FailPrefetchFilesExhausted), full.Message);
                Assert.ThrowsAny<Exception>(ring.Dispose); // files are still registered

                Close(open[^1]);
                open.RemoveAt(open.Count - 1);
                open.Add(Open(maxFiles)); // the released slot is reused

                foreach (var p in open)
                    Close(p);
                open.Clear();

                // more open / close cycles than the ring has slots
                for (var i = 0; i < 100; i++)
                    Close(Open(i % 3));
            }
        }
        finally
        {
            foreach (var p in open)
            {
                p.State.Dispose();
                p.Pager.Dispose();
            }
            IOExtensions.DeleteDirectory(path);
        }

        ring.Dispose(); // no file is registered any more
        Assert.True(ring.Handle == null);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PrefetchedPagesReachThePageCache()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int pages = 64;
        var path = RavenTestHelper.NewDataPath(nameof(PrefetchedPagesReachThePageCache), 0, forceCreateDir: true);
        try
        {
            PrefetchAndCheck(path, pages);
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    private void PrefetchAndCheck(string path, int pages)
    {
        var file = Path.Combine(path, Constants.DatabaseFilename);
        var content = new byte[pages * Constants.Storage.PageSize];
        Random.Shared.NextBytes(content);
        File.WriteAllBytes(file, content);

        using (var options = StorageEnvironmentOptions.ForPathForTests(path))
        {
            var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
            using (pager)
            using (state)
            {
                // without io_uring (old kernel, seccomp policy) the PAL falls back to posix_fadvise on this thread
                var ringAvailable = PrefetchRing.Default.Handle != null;
                Output.WriteLine($"prefetch ring available: {ringAvailable}");

                var expected = content.Length / Environment.SystemPageSize;
                DropFromPageCache(file);
                var before = ResidentPages(state.ReadAddress, content.Length);
                Output.WriteLine($"resident before the prefetch: {before} of {expected}");
                if (before == expected)
                {
                    Output.WriteLine("this file system keeps the pages in memory (tmpfs?): nothing to prefetch");
                    return;
                }

                Assert.True(Pager.PrefetchPages(state, 0, pages));

                var sw = Stopwatch.StartNew();
                int resident;
                while ((resident = ResidentPages(state.ReadAddress, content.Length)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
                    Thread.Sleep(10);
                Assert.Equal(expected, resident);

                if (ringAvailable)
                {
                    Assert.True(PrefetchRing.Default.TryGetStats(out var submitted, out _, out _, out var failed));
                    Assert.True(submitted > 0);
                    Assert.Equal(0, failed);
                }
            }
        }
    }

    private static void DropFromPageCache(string file)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.ReadWrite);
        stream.Flush(flushToDisk: true); // DONTNEED keeps dirty pages
        var fd = stream.SafeFileHandle.DangerousGetHandle().ToInt32();
        Assert.Equal(0, Syscall.posix_fadvise(fd, 0, 0, PosixFadviseDontNeed));
    }

    private static int ResidentPages(byte* address, long length)
    {
        var count = (int)(length / Environment.SystemPageSize);
        var vec = stackalloc char[count];
        Assert.Equal(0, Syscall.mincore(address, new IntPtr(length), vec));
        var resident = 0;
        var bytes = (byte*)vec;
        for (var i = 0; i < count; i++)
            resident += bytes[i] & 1;
        return resident;
    }
}
