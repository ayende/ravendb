using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace SlowTests.Issues;

public class RavenDB_23409(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Counters)]
    public async Task CanProperlyImportActionsFile()
    {
        await using var stream = typeof(RavenDB_23409).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB-23409.ravendbdump");
        Assert.NotNull(stream);

        using var store = GetDocumentStore();

        var command = new GetNextOperationIdCommand();
        await store.Commands().ExecuteAsync(command);
        var r = store.Maintenance.Send(new ReplayTransactionsRecordingOperation(stream, command.Result));
        Assert.NotNull(r.Message);
    }
}
