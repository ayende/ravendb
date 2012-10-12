using System;
using Raven.Abstractions.Data;
using Raven.Bundles.ScriptedTriggers;
using Raven.Bundles.ScriptedTriggers.Data;
using Xunit;

namespace Raven.Bundles.Tests.ScriptedTriggers
{
	public class WhenAConfigurationExists : ScriptedTriggerTestBase
	{

		public class Customer
		{
			public string Id { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public string FullName { get; set; }
			public string ClrType { get; set; }
			public string CustomerId { get; set; }
		}

		public class Order
		{
			public string Id { get; set; }
			public string CustomerId { get; set; }
			public string CustomerName { get; set; }
		}

		public WhenAConfigurationExists()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var customerEntityName = DocumentStore.Conventions.GetTypeTagName(typeof (Customer));
				var customerTrigger = new PutScriptConfiguration()
							  {
								  Id = ScriptedPutTrigger.ConfigurationPrefix + customerEntityName,
								  Script = "this.FullName = this.FirstName + ' ' + this.LastName;" +
										   "this.ClrType = metadata['" + Constants.RavenClrType + "'];" +
										   "this.CustomerId = key;"
							  };
				session.Store(customerTrigger);

				var orderEntityName = DocumentStore.Conventions.GetTypeTagName(typeof (Order));
				var orderTrigger = new PutScriptConfiguration()
									   {
										   Id = ScriptedPutTrigger.ConfigurationPrefix + orderEntityName,
										   Script = "var customer = LoadDocument(this.CustomerId);" +
													"if (customer) {" +
													"   this.CustomerName = customer.FullName;" +
													"};"
									   };
				session.Store(orderTrigger);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var customer = new Customer()
								   {
									   Id = "customers/1",
									   FirstName = "Arthur",
									   LastName = "Dent",
								   };
				var order = new Order()
								{
									Id = "orders/1",
									CustomerId = customer.Id,
									CustomerName = "wrong name"
								};

				session.Store(customer);
				session.Store(order);
				session.SaveChanges();
			}

		}

		[Fact]
		public void ThenTheScriptIsExecuted()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var customer = session.Load<Customer>(1);
				Assert.Equal("Arthur Dent", customer.FullName);
			}
		}

		[Fact]
		public void ThenTheMetadataCanBeUsed()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var customer = session.Load<Customer>(1);
				var expected = DocumentStore.Conventions.GetClrTypeName(typeof (Customer));
				Assert.Equal(expected, customer.ClrType);
			}
		}

		[Fact]
		public void ThenTheKeyCanBeUsed()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var customer = session.Load<Customer>(1);
				var expected = session.Advanced.GetDocumentId(customer);
				Assert.Equal(expected, customer.CustomerId);
			}
		}

		[Fact]
		public void ThenOtherDocumentsCanBeLoaded()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var order = session.Load<Order>(1);
				var customer = session.Load<Customer>(1);
				Assert.Equal(customer.FullName, order.CustomerName);
			}
		}
	}
}
