using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public class ObjectArrayIndexedPropertyTests : IndexedPropertiesTests
	{

		public ObjectArrayIndexedPropertyTests()
			: base(new CustomerOrders(),
				   new IndexedPropertiesSetupDoc()
				   {
					   DocumentKey = "CustomerId",
					   FieldNameMappings = {{"Orders", "Orders"}}
				   })
		{
		}

		public override void WaitForIndex(IDocumentSession session)
		{
			session.Query<object, CustomerOrders>().Customize(custom => custom.WaitForNonStaleResults()).Any();
		}


		public class CustomerOrders : AbstractIndexCreationTask<Order, CustomerOrders.Result>
		{
			public class Result
			{
				public string CustomerId { get; set; }
				public OrderReference[] Orders { get; set; }
			}

			public CustomerOrders()
			{
				Map = orders => from order in orders
								select new
								{
									order.CustomerId,
									Orders = new[]
									{
										new {order.Id, order.PlacedDate, order.Total}
									}
								};
				Reduce = results => from result in results
									group result by result.CustomerId
										into g
										select new Result()
										{
											CustomerId = g.Key,
											Orders = g.SelectMany(r => r.Orders).ToArray()
										};
			}
		}

		[Fact]
		public void ThenTheOrderIsAdded()
		{
			WhenAnOrderIsAdded();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(1, actual.Orders.Count);
				Assert.Contains(Order1, actual.Orders);
			}
		}

		[Fact]
		public void ThenTheOrdersAreAdded()
		{
			WhenTwoOrdersAreAdded();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(2, actual.Orders.Count);
				Assert.Contains(Order1, actual.Orders);
				Assert.Contains(Order2, actual.Orders);
			}
		}

		[Fact]
		public void ThenTheOrderIsRemoved()
		{
			WhenOrder2IsRemoved();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Equal(1, actual.Orders.Count);
				Assert.Contains(Order1, actual.Orders);
				Assert.DoesNotContain(Order2, actual.Orders);
			}
		}

		[Fact]
		public void ThenOrdersIsRemoved()
		{
			WhenBothOrdersAreRemoved();

			using (var session = DocumentStore.OpenSession())
			{
				var actual = session.Load<Customer>(CustomerId);
				Assert.Null(actual.Orders);
			}
		}



	}
}