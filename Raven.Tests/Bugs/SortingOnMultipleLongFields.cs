using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SortingOnMultipleLongFields : RavenTest
	{
		void UsingDatabaseOfFoos(Action<IDocumentSession> action)
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Foo
					{
						Value = 100,
						OtherValue = 0
					});

					session.Store(new Foo
					{
						Value = 100,
						OtherValue = 5
					});

					session.Store(new Foo
					{
						Value = 25,
						OtherValue = 3
					});

					session.Store(new Foo
					{
						Value = 25,
						OtherValue = 1
					});

					session.Store(new Foo
					{
						Value = 88,
						OtherValue = 100
					});

					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("long",
					new IndexDefinition
					{
						Map = "from doc in docs select new { doc.Value }",
						SortOptions = { { "Value", SortOptions.Long }, {"OtherValue", SortOptions.Long} },
					});

				using (var session = store.OpenSession())
				{
					action(session);
				}
			}
		}

		[Fact]
		public void CanSortDescendingOnTwoLongs()
		{
			UsingDatabaseOfFoos(delegate(IDocumentSession session)
			{
				var foos1 = session.Query<Foo>("long")
					.Customize(x => x.WaitForNonStaleResults())
					.OrderByDescending(x => x.Value)
					.ThenByDescending(x => x.OtherValue)
					.ToList();

				Assert.Equal(5, foos1.Count);

				Assert.Equal(100, foos1[0].Value); Assert.Equal(5, foos1[0].OtherValue);
				Assert.Equal(100, foos1[1].Value); Assert.Equal(0, foos1[1].OtherValue);
				Assert.Equal(88, foos1[2].Value); Assert.Equal(100, foos1[2].OtherValue);
				Assert.Equal(25, foos1[3].Value); Assert.Equal(3, foos1[3].OtherValue);
				Assert.Equal(25, foos1[4].Value); Assert.Equal(1, foos1[4].OtherValue);
			});
		}

		[Fact]
		public void CanSortAscendingOnTwoLongs()
		{
			UsingDatabaseOfFoos(delegate(IDocumentSession session)
			{
				var foos1 = session.Query<Foo>("long")
					.Customize(x => x.WaitForNonStaleResults())
					.OrderBy(x => x.Value)
					.ThenBy(x => x.OtherValue)
					.ToList();

				Assert.Equal(5, foos1.Count);

				Assert.Equal(25, foos1[0].Value); Assert.Equal(1, foos1[0].OtherValue);
				Assert.Equal(25, foos1[1].Value); Assert.Equal(3, foos1[1].OtherValue);
				Assert.Equal(88, foos1[2].Value); Assert.Equal(100, foos1[2].OtherValue);
				Assert.Equal(100, foos1[3].Value); Assert.Equal(0, foos1[3].OtherValue);
				Assert.Equal(100, foos1[4].Value); Assert.Equal(5, foos1[4].OtherValue);
			});
		}
		
		public class Foo
		{
			public string Id { get; set; }
			public long Value { get; set; }
			public long OtherValue { get; set; }

			public override string ToString()
			{
				return string.Format("Id: {0}, Value: {1}, OtherValue: {1}", Id, Value, OtherValue);
			}
		}
	}
}