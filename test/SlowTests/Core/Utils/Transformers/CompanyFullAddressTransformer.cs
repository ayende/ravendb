// -----------------------------------------------------------------------
//  <copyright file="CompanyFullAddressTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Transformers
{
    public class CompanyFullAddressTransformer : AbstractTransformerCreationTask<Company>
    {
        public class Result
        {
            public string FullAddress { get; set; }
        }

        public CompanyFullAddressTransformer()
        {
            TransformResults = companies => companies.Select(x => new
            {
                FullAddress = (x.Address1 ?? string.Empty) +
                              (x.Address2 != null ? (x.Address1 != null ? ", " : string.Empty) + x.Address2 : string.Empty) +
                              (x.Address3 != null ? (x.Address1 != null || x.Address2 != null ? ", " : string.Empty) + x.Address3 : string.Empty)
            });
        }
    }
}
