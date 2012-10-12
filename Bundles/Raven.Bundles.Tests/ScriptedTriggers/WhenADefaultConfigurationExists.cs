using System;
using Raven.Abstractions.Data;
using Raven.Bundles.ScriptedTriggers;
using Raven.Bundles.ScriptedTriggers.Data;
using Xunit;

namespace Raven.Bundles.Tests.ScriptedTriggers
{
	public class WhenADefaultConfigurationExists : ScriptedTriggerTestBase
	{

		public class Customer
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public string FullName { get; set; }
		}

		public WhenADefaultConfigurationExists()
		{
			using (var session = DocumentStore.OpenSession())
			{
				
				var doc = new PutScriptConfiguration()
							  {
								  Id = ScriptedPutTrigger.DefaultConfigurationId,
								  Script = "this.FullName = this.FirstName + ' ' + this.LastName;"
							  };
				session.Store(doc);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var customer = new Customer() {FirstName = "Arthur", LastName = "Dent"};
				session.Store(customer, "customers/1");
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
	}
}
