using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.MailingList.PhilJones
{
    public class Projections : RavenTestBase
    {
        [Fact]
        public async Task WorkWithRealTypes()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Offer>()
                        .Where(x => x.TripId == "trips/1234")
                        .OrderBy(x => x.Name)
                        .Select(x => new SelectListItem
                        {
                            Text = x.Name,
                            Value = x.Id
                        })
                        .ToList();
                }
            }
        }

        private class SelectListItem
        {
            public string Text { get; set; }
            public string Value { get; set; }
        }

        private class Offer
        {
            public string Id { get; set; }
            public string TripId { get; set; }
            public string Name { get; set; }
        }
    }

}
