using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public class ArrayIndexedPropertyTests : IndexedPropertiesTests
	{
		public ArrayIndexedPropertyTests()
			: base(new CustomerOrderIds(),
				   new IndexedPropertiesSetupDoc()
				   {
					   DocumentKey = "CustomerId",
					   FieldNameMappings = { { "OrderIds", "OrderIds" } }
				   })
		{
		}

		public override void WaitForIndex(IDocumentSession session)
		{
			session.Query<object, CustomerOrderIds>().Customize(custom => custom.WaitForNonStaleResults()).Any();
		}

		public class CustomerOrderIds : AbstractIndexCreationTask<Order, CustomerOrderIds.Result>
		{
			public class Result
			{
				public string CustomerId { get; set; }
				public string[] OrderIds { get; set; }
			}

			public CustomerOrderIds()
			{
				Map = orders => from order in orders
								select new { order.CustomerId, OrderIds = new[] { order.Id } };
				Reduce = results => from result in results
									group result by result.CustomerId
										into g
										select new Result()
										{
											CustomerId = g.Key,
											OrderIds = g.SelectMany(r => r.OrderIds).ToArray()
										};
			}
		}

		[Fact]
		public void ThenTheOrderIdIsAdded()
		{
			WhenAnOrderIsAdded();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(1, actual.OrderIds.Count);
				Assert.Contains(Order1.Id, actual.OrderIds);
			}
		}

		[Fact]
		public void ThenTheOrderIdsAreAdded()
		{
			WhenTwoOrdersAreAdded();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(2, actual.OrderIds.Count);
				Assert.Contains(Order1.Id, actual.OrderIds);
				Assert.Contains(Order2.Id, actual.OrderIds);
			}
		}

		[Fact]
		public void ThenTheOrderIdIsRemoved()
		{
			WhenOrder2IsRemoved();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(1, actual.OrderIds.Count);
				Assert.Contains(Order1.Id, actual.OrderIds);
				Assert.DoesNotContain(Order2.Id, actual.OrderIds);
			}
		}

		[Fact]
		public void ThenOrderIdsIsRemoved()
		{
			WhenBothOrdersAreRemoved();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Null(actual.OrderIds);
			}
		}



	}
}
