using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15690 : RavenTestBase
    {
        public RavenDB_15690(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetHasChangesOnDelete()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new {Name = "Ayende"}, "users/ayende");
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<object>("users/ayende");
                session.Delete(item);
                Assert.True(session.Advanced.HasChanges);
            }
        }
    }
}
