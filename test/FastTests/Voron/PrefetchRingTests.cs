using System;
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
    public void ClosedDataFilesReleaseTheirPrefetchRegistration()
    {
        // more open / close cycles than the ring has file slots (4096 by default): a slot that a close did not release
        // would make the next open fail
        var path = RavenTestHelper.NewDataPath(nameof(ClosedDataFilesReleaseTheirPrefetchRegistration), 0);
        try
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                for (var i = 0; i < 5_000; i++)
                {
                    var (pager, state) = options.InitializeDataPager();
                    state.Dispose();
                    pager.Dispose();
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
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
                if (Pal.rvn_prefetch_stats(out _, out _, out _, out _) != PalFlags.FailCodes.Success)
                {
                    Output.WriteLine("io_uring is not available here (kernel or seccomp policy): no prefetch ring to test");
                    return;
                }

                DropFromPageCache(file);
                Output.WriteLine($"resident before the prefetch: {ResidentPages(state.ReadAddress, content.Length)} of {content.Length / Environment.SystemPageSize}");

                Assert.True(Pager.PrefetchPages(state, 0, pages));

                var expected = content.Length / Environment.SystemPageSize;
                var sw = Stopwatch.StartNew();
                int resident;
                while ((resident = ResidentPages(state.ReadAddress, content.Length)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
                    Thread.Sleep(10);
                Assert.Equal(expected, resident);

                Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_prefetch_stats(out var submitted, out _, out _, out var failed));
                Assert.True(submitted > 0);
                Assert.Equal(0, failed);
            }
        }
    }

    private static void DropFromPageCache(string file)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read);
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
