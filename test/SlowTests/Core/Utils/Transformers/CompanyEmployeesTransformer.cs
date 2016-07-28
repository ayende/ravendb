// -----------------------------------------------------------------------
//  <copyright file="UsersWithCustomDataAndInclude.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Collections.Generic;
using Company = SlowTests.Core.Utils.Entities.Company;
using Employee = SlowTests.Core.Utils.Entities.Employee;

namespace SlowTests.Core.Utils.Transformers
{
    public class CompanyEmployeesTransformer : AbstractTransformerCreationTask<Company>
    {
        public class Result
        {
            public string Name { get; set; }

            public List<string> Employees { get; set; }
        }

        public CompanyEmployeesTransformer()
        {
            TransformResults = companies => from company in companies
                                                      select new
                                                      {
                                                          Name = company.Name,
                                                          Employees = company.EmployeesIds.Select(x => LoadDocument<Employee>(x).LastName)
                                                      };
        }
    }
}
