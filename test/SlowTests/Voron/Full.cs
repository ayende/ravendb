﻿using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using Voron;
using Voron.Impl.Backup;

namespace SlowTests.Voron
{
    public class Full : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * options.PageSize;
            options.ManualFlushing = true;
        }

        [Fact]
        public void CanBackupAndRestore()
        {
            RequireFileBasedPager();
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 5000; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            Assert.True(Env.Journal.Files.Count > 1);

            Env.FlushLogToDataFile(); // force writing data to the data file

            // add more data to journal files
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 500; i < 1000; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile(); // force writing data to the data file - this won't sync data to disk because there was another sync withing last minute

            BackupMethods.Full.ToFile(Env, Path.Combine(DataDir, "voron-test.backup"));

            BackupMethods.Full.Restore(Path.Combine(DataDir, "voron-test.backup"), Path.Combine(DataDir, "backup-test.data"));

            var options = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "backup-test.data"));
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 1000; i++)
                    {
                        var readResult = tree.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }
    }
}