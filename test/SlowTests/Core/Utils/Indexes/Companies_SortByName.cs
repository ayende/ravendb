// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Indexes
{
    public class Companies_SortByName : AbstractIndexCreationTask<Company>
    {

        public Companies_SortByName()
        {
            Map = companies => from company in companies
                               select new { company.Name };

            Sort(c => c.Name, SortOptions.String);

            throw new NotImplementedException();

            //Analyzers.Add(c => c.Name, typeof(Raven.Database.Indexing.Collation.Cultures.PlCollationAnalyzer).ToString());
        }
    }
}
