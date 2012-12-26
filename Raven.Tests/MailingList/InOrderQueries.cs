using Xunit;
using System.Linq;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
	public class InOrderQueries : RavenTest
	{
		public class User
		{
			public string Country { get; set; }
		}

		[Fact]
		public void WhenQueryAndIndexInAlphaOrder()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Country = "England" });
					session.Store(new User { Country = "Germany" });
					session.Store(new User { Country = "Israel" });
					session.Store(new User { Country = "Japan" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var values = new[] { "England", "Germany" };
					var collection = session.Query<User>().Where(x => x.Country.InOrder(values)).ToList();

					Assert.NotEmpty(collection);
					Assert.Equal(values.Length, collection.Count);
					for (var i = 0; i < collection.Count; i++)
					{
						Assert.Equal(values[i], collection[i].Country);
					}
				}
			}
		}

		[Fact]
		public void WhenQueryNotInAlphaOrder()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Country = "England" });
					session.Store(new User { Country = "Germany" });
					session.Store(new User { Country = "Israel" });
					session.Store(new User { Country = "Japan" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var values = new[] { "Germany", "England" };
					var collection = session.Query<User>().Where(x => x.Country.InOrder(values)).ToList();

					Assert.NotEmpty(collection);
					Assert.Equal(values.Length, collection.Count);
					for (var i = 0; i < collection.Count; i++)
					{
						Assert.Equal(values[i], collection[i].Country);
					}
				}
			}
		}

		[Fact]
		public void WhenIndexNotInAlphaOrder()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Country = "Japan" });
					session.Store(new User { Country = "Israel" });
					session.Store(new User { Country = "Germany" });
					session.Store(new User { Country = "England" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var values = new[] { "England", "Germany" };
					var collection = session.Query<User>().Where(x => x.Country.InOrder(values)).ToList();

					Assert.NotEmpty(collection);
					Assert.Equal(values.Length, collection.Count);
					for (var i = 0; i < collection.Count; i++)
					{
						Assert.Equal(values[i], collection[i].Country);
					}
				}
			}
		}

		[Fact]
		public void WhenIndexAndQueryNotInAlphaOrder()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Country = "Japan" });
					session.Store(new User { Country = "Israel" });
					session.Store(new User { Country = "Germany" });
					session.Store(new User { Country = "England" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var values = new[] { "Germany", "England" };
					var collection = session.Query<User>().Where(x => x.Country.InOrder(values)).ToList();

					Assert.NotEmpty(collection);
					Assert.Equal(values.Length, collection.Count);
					for (var i = 0; i < collection.Count; i++)
					{
						Assert.Equal(values[i], collection[i].Country);
					}
				}
			}
		}
	}
}