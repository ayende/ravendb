// -----------------------------------------------------------------------
//  <copyright file="MixedModeAuthorizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security.OAuth;

namespace Raven.Database.Server.Security
{
	public class MixedModeAuthorizer : AbstractRequestAuthorizer
	{
		private readonly List<string> requiredGroups = new List<string>();
		private readonly List<string> requiredUsers = new List<string>();

		protected override void Initialize()
		{
			var requiredGroupsString = server.Configuration.Settings["Raven/Authorization/Windows/RequiredGroups"];
			if (requiredGroupsString != null)
			{
				var groups = requiredGroupsString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				requiredGroups.AddRange(groups);
			}

			var requiredUsersString = server.Configuration.Settings["Raven/Authorization/Windows/RequiredUsers"];
			if (requiredUsersString != null)
			{
				var users = requiredUsersString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				requiredUsers.AddRange(users);
			}
		}

		public override bool Authorize(IHttpContext ctx)
		{
			var requestUrl = ctx.GetRequestUrl();
			if (NeverSecret.Urls.Contains(requestUrl, StringComparer.InvariantCultureIgnoreCase))
				return true;

			var getRequest = IsGetRequest(ctx.Request.HttpMethod, requestUrl);
			Action onRejectingRequest;
			var isInvalidUser = IsInvalidWindowsUser(ctx, out onRejectingRequest) ||
								IsInvalidOAuthUser(ctx, getRequest == false, out onRejectingRequest);

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None &&
				isInvalidUser)
			{
				onRejectingRequest();
				return false;
			}

			var httpRequest = ctx.Request;

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				isInvalidUser &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
				onRejectingRequest();
				return false;
			}

			if (isInvalidUser == false)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = ctx.User.Identity.Name;
				CurrentOperationContext.User.Value = ctx.User;
			}
			return true;
		}

		private bool IsInvalidOAuthUser(IHttpContext ctx, bool writeAccess,  out Action onRejectingRequest)
		{
			var token = GetToken(ctx);

			if (token == null)
			{
				onRejectingRequest = () => 
					WriteAuthorizationChallenge(ctx, 401, "invalid_request", "The access token is required");
				return true;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(Settings.OAuthTokenCertificate, token, out tokenBody))
			{
				onRejectingRequest = () =>
					WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is invalid");

				return true;
			}

			if (tokenBody.IsExpired())
			{
				onRejectingRequest = () =>
					WriteAuthorizationChallenge(ctx, 401, "invalid_token", "The access token is expired");

				return true;
			}

			if (!tokenBody.IsAuthorized(TenantId, writeAccess))
			{
				onRejectingRequest = () => WriteAuthorizationChallenge(ctx, 403, "insufficient_scope",
									writeAccess ?
									"Not authorized for read/write access for tenant " + TenantId :
									"Not authorized for tenant " + TenantId);

				return true;
			}

			ctx.User = new OAuthPrincipal(tokenBody, TenantId);
			onRejectingRequest = null;
			return false;
		}

		private bool IsInvalidWindowsUser(IHttpContext ctx, out Action onRejectingRequest)
		{
			onRejectingRequest = ctx.SetStatusToUnauthorized;
			
			if (ctx.User == null || ctx.User.Identity.IsAuthenticated == false)
			{
				return true;
			}

			if (ctx.User is WindowsPrincipal == false)
				return true;

			if (requiredGroups.Count > 0 || requiredUsers.Count > 0)
			{

				if (requiredGroups.Any(requiredGroup => ctx.User.IsInRole(requiredGroup)) ||
					requiredUsers.Any(requiredUser => string.Equals(ctx.User.Identity.Name, requiredUser, StringComparison.InvariantCultureIgnoreCase)))
					return false;

				return true;
			}

			return false;
		}

		static string GetToken(IHttpContext ctx)
		{
			const string bearerPrefix = "Bearer ";

			var auth = ctx.Request.Headers["Authorization"];

			if (auth == null || auth.Length <= bearerPrefix.Length || !auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
				return null;

			var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);

			return token;
		}

		void WriteAuthorizationChallenge(IHttpContext ctx, int statusCode, string error, string errorDescription)
		{
			if (string.IsNullOrEmpty(Settings.OAuthTokenServer) == false)
			{
				ctx.Response.AddHeader("OAuth-Source", Settings.OAuthTokenServer);
			}
			ctx.Response.StatusCode = statusCode;
			ctx.Response.AddHeader("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription));
		}
	}
}