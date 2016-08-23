﻿using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Smuggler;
using Xunit;

namespace SlowTests.Smuggler
{
    public class LegacySmugglerTests : RavenTestBase
    {
        [Theory(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        [InlineData("Smuggler/Northwind_3.5.35168.ravendbdump")]
        public async Task CanImportNorthwind(string file)
        {
            var fileInfo = new FileInfo(file);
            Assert.True(fileInfo.Exists);

            using (var store = await GetDocumentStore())
            {
                await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), fileInfo.FullName);

                var stats = store.DatabaseCommands.GetStatistics();

                Assert.Equal(1059, stats.CountOfDocuments);
                Assert.Equal(4, stats.CountOfIndexes);
                Assert.Equal(1, stats.CountOfTransformers);
            }
        }
    }
}