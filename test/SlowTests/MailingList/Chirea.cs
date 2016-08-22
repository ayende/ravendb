// -----------------------------------------------------------------------
//  <copyright file="Chirea.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Chirea : RavenTestBase
    {
        private class Item
        {
            public string Name { get; set; }
        }

        private class Container
        {
            public Item First { get; set; }
            public Item Second { get; set; }
        }

        private class ContainsIndex : AbstractIndexCreationTask<Container>
        {
            public ContainsIndex()
            {
                Map = containers =>
                      from container in containers
                      from item in new[] { container.First, container.Second }
                      select new
                      {
                          item.Name
                      };
            }
        }

        [Fact]
        public async Task CanCreateIndexWithArrayOfNestedObjects()
        {
            using (var store = await GetDocumentStore())
            {
                new ContainsIndex().Execute(store);
            }
        }
    }
}
