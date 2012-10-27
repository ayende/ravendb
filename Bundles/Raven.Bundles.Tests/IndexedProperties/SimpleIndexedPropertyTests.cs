using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public class SimpleIndexedPropertyTests : IndexedPropertiesTests
	{
		public SimpleIndexedPropertyTests()
			: base(new CustomerOrdersTotal(),
				   new IndexedPropertiesSetupDoc()
				   {
					   DocumentKey = "CustomerId",
					   FieldNameMappings = { { "OrdersTotal", "OrdersTotal" } }
				   })
		{
		}

		public override void WaitForIndex(IDocumentSession session)
		{
			session.Query<object, CustomerOrdersTotal>().Customize(custom => custom.WaitForNonStaleResults()).Any();
		}

		public class CustomerOrdersTotal : AbstractIndexCreationTask<Order, CustomerOrdersTotal.Result>
		{
			public class Result
			{
				public string CustomerId { get; set; }
				public int OrdersTotal { get; set; }
			}

			public CustomerOrdersTotal()
			{
				Map = orders => from order in orders
								select new { order.CustomerId, OrdersTotal = order.Total };

				Reduce = results => from result in results
									group result by result.CustomerId
										into g
										select new Result()
										{
											CustomerId = g.Key,
											OrdersTotal = g.Sum(r => r.OrdersTotal)
										};
			}
		}

		[Fact]
		public void ThenOrdersTotalIsSet()
		{
			WhenAnOrderIsAdded();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(Order1.Total, actual.OrdersTotal);
			}
		}

		[Fact]
		public void ThenOrdersTotalIsIncremented()
		{
			WhenTwoOrdersAreAdded();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(Order1.Total + Order2.Total, actual.OrdersTotal);
			}
		}

		[Fact]
		public void ThenOrdersTotalIsDecremented()
		{
			WhenOrder2IsRemoved();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(Order1.Total, actual.OrdersTotal);
			}
		}

		[Fact]
		public void ThenOrdersTotalIsRemoved()
		{
			WhenBothOrdersAreRemoved();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(default(int), actual.OrdersTotal);
			}
		}

	}
}
