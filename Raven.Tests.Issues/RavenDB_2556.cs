// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2556.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2556 : RavenTest
	{
		[Fact]
		public void FailoverBehaviorShouldBeReadFromServer()
		{
			using (var store = NewRemoteDocumentStore())
			{
				Assert.Equal(FailoverBehavior.AllowReadsFromSecondaries, store.Conventions.FailoverBehavior);

				store
					.DatabaseCommands
					.Put(Constants.RavenClientConfiguration, null, RavenJObject.FromObject(new DocumentStoreConfiguration { FailoverBehavior = null }), new RavenJObject());

				using (var internalStore = new DocumentStore { Url = store.Url, DefaultDatabase = store.DefaultDatabase }.Initialize())
				{
					Assert.Equal(FailoverBehavior.AllowReadsFromSecondaries, internalStore.Conventions.FailoverBehavior);
				}

				store
					.DatabaseCommands
					.Put(Constants.RavenClientConfiguration, null, RavenJObject.FromObject(new DocumentStoreConfiguration { FailoverBehavior = FailoverBehavior.FailImmediately }), new RavenJObject());

				using (var internalStore = new DocumentStore { Url = store.Url, DefaultDatabase = store.DefaultDatabase }.Initialize())
				{
					Assert.Equal(FailoverBehavior.FailImmediately, internalStore.Conventions.FailoverBehavior);
				}
			}
		}
	}
}