using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.MailingList.RobStats;
using Xunit;

namespace Raven.Tests.Issues
{
    public class ValuesMismatch : RavenTest
    {
        [Fact]
        public void IndexingFieldsWithDoubleTypeShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                new ProductSalesByTag().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Name = "Product1", Price = 15.4,Discount = 0.1, Quantity = 3, Tag = "Tag3"});
                    session.Store(new Product() { Name = "Product2", Price = 5.4, Discount = 0.11, Quantity = 7, Tag = "Tag1" });
                    session.Store(new Product() { Name = "Product3", Price = 1.4, Discount = 0.15, Quantity = 2, Tag = "Tag2" });
                    session.Store(new Product() { Name = "Product4", Price = 3.6, Discount = 0.14, Quantity = 8, Tag = "Tag1" });
                    session.Store(new Product() { Name = "Product5", Price = 2.4, Discount = 0.17, Quantity = 1, Tag = "Tag2" });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var res = session.Query<ProductSalesByTag.Result>("ProductSalesByTag").ToList();
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(3,res.Count);
                }                
            }
        }
       
        public class Product
        {
            public string Name { get; set; }
            public string Tag { get; set; }
            public double Price { get; set; }
            public double Discount { get; set; }
            public int Quantity { get; set; }
        }
        public class ProductSalesByTag : AbstractIndexCreationTask<Product, ProductSalesByTag.Result> 
        {
             public ProductSalesByTag()
             {
                 Map = products => from product in products 
                                   select new {product.Tag, Sales = (product.Price*product.Quantity)*(1 - product.Discount)};
                 Reduce = results => from res in results
                     group res by res.Tag
                     into g
                     select new Result
                     {
                         Tag = g.Key,
                         Sales = g.Sum(x=>x.Sales)
                     };
             }
             public class Result
            {
                 public string Tag { get; set; }
                 public double Sales { get; set; }
            }
        }
    }
}
