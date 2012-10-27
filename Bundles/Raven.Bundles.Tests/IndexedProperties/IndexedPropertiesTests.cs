using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Bundles.Tests.IndexedProperties
{
	public abstract class IndexedPropertiesTests
	{

		public readonly EmbeddableDocumentStore DocumentStore;

		public const string CustomerId = "customers/1";
		public readonly Customer Customer1 = new Customer() { Id = CustomerId };
		public readonly Order Order1 = new Order() { Id = "orders/1", CustomerId = CustomerId, PlacedDate = DateTime.Today.AddDays(-1), Total = 23 };
		public readonly Order Order2 = new Order() { Id = "orders/2", CustomerId = CustomerId, PlacedDate = DateTime.Today, Total = 12 };

		protected IndexedPropertiesTests(AbstractIndexCreationTask indexCreationTask, IndexedPropertiesSetupDoc setupDoc)
		{
			DocumentStore = new EmbeddableDocumentStore()
								{
									RunInMemory = true
								};

			DocumentStore.Configuration.Settings["Raven/ActiveBundles"] = "IndexedProperties";

			DocumentStore.Initialize();

			Assert.Empty(DocumentStore.DatabaseCommands.GetIndexNames(0, 250));
			Assert.Empty(DocumentStore.DatabaseCommands.StartsWith(IndexedPropertiesSetupDoc.IdPrefix, "", 0, 250));

			indexCreationTask.Conventions = DocumentStore.Conventions;

			DocumentStore.DatabaseCommands.PutIndex(indexCreationTask.IndexName, indexCreationTask.CreateIndexDefinition());

			var documentId = GenerateSetupDocumentId(indexCreationTask);

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(setupDoc, documentId);
				session.SaveChanges();
			}
		}

		public void Dispose()
		{
			DocumentStore.Dispose();
		}

		public string GenerateSetupDocumentId(AbstractIndexCreationTask indexCreationTask)
		{
			return IndexedPropertiesSetupDoc.IdPrefix + indexCreationTask.IndexName;
		}

		public abstract void WaitForIndex(IDocumentSession session);

		public void WhenAnOrderIsAdded()
		{
			using (var session = DocumentStore.OpenSession())
			{
				session.Store(Customer1);
				session.Store(Order1);
				session.SaveChanges();
				WaitForIndex(session);
			}
		}

		public void WhenTwoOrdersAreAdded()
		{
			using (var session = DocumentStore.OpenSession())
			{
				session.Store(Customer1);
				session.Store(Order1);
				session.Store(Order2);
				session.SaveChanges();
				WaitForIndex(session);
			}
		}

		public void WhenOrder2IsRemoved()
		{
			using (var session = DocumentStore.OpenSession())
			{
				session.Store(Customer1);
				session.Store(Order1);
				session.Store(Order2);
				session.SaveChanges();
				WaitForIndex(session);
			}

			using (var session = DocumentStore.OpenSession())
			{
				session.Delete(session.Load<Order>(Order2.Id));
				session.SaveChanges();
				WaitForIndex(session);
			}
		}

		public void WhenBothOrdersAreRemoved()
		{
			using (var session = DocumentStore.OpenSession())
			{
				session.Store(Customer1);
				session.Store(Order1);
				session.Store(Order2);
				session.SaveChanges();
				WaitForIndex(session);
			}

			using (var session = DocumentStore.OpenSession())
			{
				session.Delete(session.Load<Order>(Order1.Id));
				session.Delete(session.Load<Order>(Order2.Id));
				session.SaveChanges();
				WaitForIndex(session);
				Thread.Sleep(TimeSpan.FromSeconds(3)); // Don't know how to wait until batcher is disposed
			}
		}


		#region " Model "

		public class Customer
		{
			public string Id { get; set; }
			public int OrdersTotal { get; set; }
			public HashSet<string> OrderIds { get; set; }
			public HashSet<OrderReference> Orders { get; set; }
		}

		public class Order
		{
			public string Id { get; set; }
			public int Total { get; set; }
			public DateTime PlacedDate { get; set; }
			public string CustomerId { get; set; }
		}

		public class OrderReference : IEquatable<OrderReference>, IEquatable<Order>
		{
			public string Id { get; set; }
			public int Total { get; set; }
			public DateTime PlacedDate { get; set; }

			public static implicit operator OrderReference(Order order)
			{
				if (order == null) return null;
				return new OrderReference()
				{
					Id = order.Id,
					PlacedDate = order.PlacedDate,
					Total = order.Total
				};
			}

			public bool Equals(OrderReference other)
			{
				if (ReferenceEquals(other, null))
					return false;
				return Id == other.Id;
			}

			public bool Equals(Order other)
			{
				return Equals((OrderReference)other);
			}

			public override bool Equals(object obj)
			{
				if (ReferenceEquals(obj, null))
					return false;
				if (obj is OrderReference)
					return Equals((OrderReference)obj);
				if (obj is Order)
					return Equals((Order)obj);
				return false;
			}

			public override int GetHashCode()
			{
				return Id.GetHashCode();
			}
		}

		#endregion
	}
}