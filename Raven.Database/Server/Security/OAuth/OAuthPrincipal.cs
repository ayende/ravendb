using System;
using System.Security.Principal;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthPrincipal : IPrincipal, IIdentity
	{
		private readonly AccessTokenBody tokenBody;
		private readonly string tenantId;

		public OAuthPrincipal(AccessTokenBody tokenBody, string tenantId)
		{
			this.tokenBody = tokenBody;
			this.tenantId = tenantId;
		}

		public bool IsInRole(string role)
		{
			if ("Administrators".Equals(role, StringComparison.InvariantCultureIgnoreCase) == false)
				return false;

			var databaseAccess = tokenBody.AuthorizedDatabases.FirstOrDefault(x=>string.Equals(x.TenantId, tenantId, StringComparison.InvariantCultureIgnoreCase) || x.TenantId == "*");

			if (databaseAccess == null)
				return false;

			return databaseAccess.Admin;
		}

		public IIdentity Identity
		{
			get { return this; }
		}

		public string Name
		{
			get { return tokenBody.UserId; }
		}

		public string AuthenticationType
		{
			get { return "OAuth"; }
		}

		public bool IsAuthenticated
		{
			get { return true; }
		}
	}
}