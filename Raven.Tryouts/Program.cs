using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.FileSystem.Connection;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
#if !DNXCORE50
using System.Security.Policy;
using Raven.Tests.Core;
using Raven.Tests.Core.Commands;
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{
    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }
        public Contact Contact { get; set; }
        public Address Address { get; set; }
        public string Phone { get; set; }
        public string Fax { get; set; }
    }

    public class Address
    {
        public string Line1 { get; set; }
        public string Line2 { get; set; }
        public string City { get; set; }
        public string Region { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }
    }

    public class Contact
    {
        public string Name { get; set; }
        public string Title { get; set; }
    }

    public class Category
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    public class Order
    {
        public string Id { get; set; }
        public string Company { get; set; }
        public string Employee { get; set; }
        public DateTime OrderedAt { get; set; }
        public DateTime RequireAt { get; set; }
        public DateTime? ShippedAt { get; set; }
        public Address ShipTo { get; set; }
        public string ShipVia { get; set; }
        public decimal Freight { get; set; }
        public List<OrderLine> Lines { get; set; }
    }

    public class OrderLine
    {
        public string Product { get; set; }
        public string ProductName { get; set; }
        public decimal PricePerUnit { get; set; }
        public int Quantity { get; set; }
        public decimal Discount { get; set; }
    }

    public class ProductAlfa
    {
        public string Id { get; set; }
        public string Supplier { get; set; }
        public string Category { get; set; }
        public string QuantityPerUnit { get; set; }
        public int UnitsInStock { get; set; }
        public int UnitsOnOrder { get; set; }
    }

    public class Product : ProductAlfa
    {
        public string Name { get; set; }
    }

    public class MaCrap
    {
        public class ProductA
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Supplier { get; set; }
            public string Category { get; set; }
            public string QuantityPerUnit { get; set; }
        }
    }

    public class ProductB
    {
        public decimal PricePerUnit { get; set; }
        public int UnitsInStock { get; set; }
        public int UnitsOnOrder { get; set; }
        public bool Discontinued { get; set; }
        public int ReorderLevel { get; set; }
    }

    public class Supplier
    {
        public string Id { get; set; }
        public Contact Contact { get; set; }
        public string Name { get; set; }
        public Address Address { get; set; }
        public string Phone { get; set; }
        public string Fax { get; set; }
        public string HomePage { get; set; }
    }

    public class Employee
    {
        public string Id { get; set; }
        public string LastName { get; set; }
        public string FirstName { get; set; }
        public string Title { get; set; }
        public Address Address { get; set; }
        public DateTime HiredAt { get; set; }
        public DateTime Birthday { get; set; }
        public string HomePhone { get; set; }
        public string Extension { get; set; }
        public string ReportsTo { get; set; }
        public List<string> Notes { get; set; }

        public List<string> Territories { get; set; }
    }

    public class Region
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<Territory> Territories { get; set; }
    }

    public class Territory
    {
        public string Code { get; set; }
        public string Name { get; set; }
    }

    public class Shipper
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Phone { get; set; }
    }

    public class Foo
    {
        public string Name { get; set; }
        public string Supplier { get; set; }
    }

    internal class Employees_ByFirstName : AbstractIndexCreationTask<Employee>
    {
        public Employees_ByFirstName()
        {
            Map = employee => from e in employee select new { FirstName = e.FirstName };
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
#if! DNXCORE50
            var store = new DocumentStore
            {
                Url = "http://localhost.fiddler:8080/",
                DefaultDatabase = "IdanDB"

            };
            store.Initialize();
            store.Initialize();
            //            MultiLoadResult semo = store.DatabaseCommands.Get(new[] { "employees/2", "employees/3" }, null, metadataOnly: true);
            //            store.DatabaseCommands.Get("").DataAsJson();
            //            var s = store.DatabaseCommands.Query("Employees/byFirstName", new IndexQuery
            //            {
            //                Query = "FirstName:\"Robert ops\""
            //            });
            using (var session = store.OpenSession())
            {
                //                ProductAlfa a = session.Load<ProductAlfa>("products/66");
                IDocumentQuery<Employee> documentQuery = session
                    .Advanced
                    .DocumentQuery<Employee>().UsingDefaultOperator(QueryOperator.And)
                    .WhereIn(x => x.LastName, new[] {"Fuller","Davolio s"});

                var results = documentQuery.ToList();
//                results = session
//                    .Advanced
//                    .DocumentQuery<Employee>("Employees/byFirstName")
//                    .WhereEquals("FirstName", 0.1).ToList();
                foreach (var item in results)
                {
                    Console.WriteLine(item);
                }

                session.SaveChanges();
            }
            Console.ReadKey();
        }
#endif
    }
}
