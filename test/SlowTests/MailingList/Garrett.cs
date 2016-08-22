// -----------------------------------------------------------------------
//  <copyright file="Garrett.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class Garrett : RavenTestBase
    {
        private class StrategyIndividual
        {
            public string OtherProp { get; set; }
            public Dictionary<int, double> Statistcs { get; set; }
        }

        [Fact(Skip = "TODO arek - figure out why we are translating the field in auto-index to Statistcs[4] instead of Statistcs.4")]
        public async Task CanOrderByDictionaryValue()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new StrategyIndividual
                    {
                        OtherProp = "Test",
                        Statistcs = new Dictionary<int, double>
                        {
                            { 4, 5.0 }
                        }
                    });

                    session.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Query<StrategyIndividual>()
                     .Where(x => x.Statistcs[4] == 0)
                     .OrderBy(x => x.Statistcs[4])
                     .ToList();
                }
            }
        }
    }
}
