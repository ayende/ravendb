using System;
using Raven.Abstractions.Data;
using Raven.Bundles.ScriptedTriggers;
using Raven.Bundles.ScriptedTriggers.Data;
using Xunit;

namespace Raven.Bundles.Tests.ScriptedTriggers
{
	public class WhenBothConfigurationsExists : ScriptedTriggerTestBase
	{

		public class Customer
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public string FullName { get; set; }
		}

		public WhenBothConfigurationsExists()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var entityName = DocumentStore.Conventions.GetTypeTagName(typeof (WhenAConfigurationExists.Customer));
				var cfg = new PutScriptConfiguration()
							  {
								  Id = ScriptedPutTrigger.ConfigurationPrefix + entityName,
								  Script = "this.FullName = this.FirstName + ' ' + this.LastName;"
							  };
				session.Store(cfg);

				var defaultCfg = new PutScriptConfiguration()
									 {
										 Id = ScriptedPutTrigger.DefaultConfigurationId,
										 Script = "this.FullName = null;"
									 };
				session.Store(defaultCfg);
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
		public void ThenTheEntitySpecificScriptIsExecuted()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var customer = session.Load<Customer>(1);
				Assert.Equal("Arthur Dent", customer.FullName);
			}
		}

	}
}
