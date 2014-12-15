﻿using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.SlowTests.Issues;
using Raven.Tests.Common;
using Raven.Tests.Core;
using Raven.Tests.Core.Querying;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main()
		{
			/*for (int i = 0; i < 100; i++)
			{
				using (var s = new RavenDB_1359())
				{
					Console.WriteLine(i);
					s.IndexThatLoadAttachmentsShouldIndexAllDocuments();
				}
			
			}*/

            new OrdinaryQueryTest2().Execute();

		}
	}

    public class OrdinaryQueryTest2 : RavenTest
    {
        public class Ob
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }
    
        public void Execute()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.MaxLengthOfQueryUsingGetUrl = 10;
                new ObsIndex().Execute(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Ob
                    {
                        Name = "A",
                        Age=1
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Queryable.Where(session.Query<Ob>().Customize(x => x.WaitForNonStaleResults()), x => x.Name == "A" && x.Name != "B" && x.Name != "C" && x.Name != "D" && x.Name != "E" && x.Name != "F" && x.Name != "G" && x.Name != "H" && x.Name != "I" && x.Name != "J" && x.Name != "K" && x.Name != "L" && x.Name != "M" && x.Name != "N" ).ToListAsync().GetAwaiter().GetResult();
                    Queryable.Where(session.Query<Ob>().Customize(x => x.WaitForNonStaleResults()), x => x.Name == "A" && x.Name != "B" && x.Name != "C" && x.Name != "D" && x.Name != "E" && x.Name != "F" && x.Name != "G" && x.Name != "H" && x.Name != "I" && x.Name != "J" && x.Name != "K" && x.Name != "L" && x.Name != "M" && x.Name != "N").ToListAsync().GetAwaiter().GetResult();
                    /*Queryable.Where(session.Query<Ob>().Customize(x => x.WaitForNonStaleResults()), x => x.Name == "A").ToListAsync().GetAwaiter().GetResult();*/
                    Queryable.Where(session.Query<Ob,ObsIndex>().Customize(x => x.WaitForNonStaleResults()), x => x.Name == "A").ToFacetsAsync(Enumerable.Repeat(1,1).Select(x=> new Facet() { Name = "Age" }).ToArray()).GetAwaiter().GetResult();
                    Queryable.Where(session.Query<Ob, ObsIndex>().Customize(x => x.WaitForNonStaleResults()), x => x.Name == "A").ToFacetsAsync(Enumerable.Repeat(1, 1).Select(x => new Facet() { Name = "Age" }).ToArray()).GetAwaiter().GetResult();
                    //Queryable.Where(session.Query<Ob,ObsIndex>().Customize(x => x.WaitForNonStaleResults()), x => x.Name == "A").ToFacetsAsync(new[] { new Facet() { Name = "Age" } }).GetAwaiter().GetResult();
                }

            }
        }

        public class ObsIndex : AbstractIndexCreationTask<Ob>
        {
            public ObsIndex()
            {
                Map = results => from result in results
                    select new Ob
                    {
                        Name = result.Name
                    };
            }
        }
    }


	
}