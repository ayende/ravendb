//-----------------------------------------------------------------------
// <copyright file="Deleting.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System;

using Raven.Client.Document;

using Xunit;

namespace Raven.Tests.Bundles.Authorization
{
	public class Deleting : AuthorizationTest
	{
		[Fact]
		public void WillAbortDeleteIfUserDoesNotHavePermissions()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization());// deny everyone

				s.SaveChanges();
			}

			using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Rename");

				Assert.Throws<InvalidOperationException>(() => ((DocumentSession)s).DatabaseCommands.Delete(company.Id, null));
			}
		}

		[Fact]
		public void WillDeleteIfUserHavePermissions()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions =
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission
							{
								Allow = true,
								User = UserId,
								Operation = "Company/Rename"
							}
						}
				});// deny everyone

				s.SaveChanges();
			}

			using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Rename");
				company.Name = "Stampeding Rhinos";
				s.Store(company);

				Assert.DoesNotThrow(() => store.DatabaseCommands.Delete(company.Id, null));
			}
		}
	}
}
