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
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security.OAuth;

namespace Raven.Database.Server.Security
{
	public class MixedModeAuthorizer
	{
		private readonly List<string> requiredGroups = new List<string>();
		private readonly List<string> requiredUsers = new List<string>();

		private Func<InMemoryRavenConfiguration> settings;
		private Func<DocumentDatabase> database;
		protected HttpServer server;

		public DocumentDatabase ResourceStore { get { return database(); } }
		public InMemoryRavenConfiguration Settings { get { return settings(); } }

		public void Initialize(Func<DocumentDatabase> databaseGetter, Func<InMemoryRavenConfiguration> settingsGetter, HttpServer theServer)
		{
			server = theServer;
			database = databaseGetter;
			settings = settingsGetter;

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

		public static bool IsGetRequest(string httpMethod, string requestPath)
		{
			return (httpMethod == "GET" || httpMethod == "HEAD") ||
				   httpMethod == "POST" && (requestPath == "/multi_get/" || requestPath == "/multi_get");
		}

		public bool Authorize(IAuthenticationContext ctx)
		{
			var requestUrl = ctx.RequestUrl;
			if (NeverSecret.Urls.Contains(requestUrl))
				return true;

			var getRequest = IsGetRequest(ctx.Request.HttpMethod, requestUrl);
			int statusCode = 401;
			var isInvalidUser = IsInvalidWindowsUser(ctx) &&
								IsInvalidOAuthUser(ctx, getRequest == false, ref statusCode);

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None &&
				isInvalidUser)
			{
				SendUnauthorizedResponse(ctx, statusCode);
				return false;
			}

			var httpRequest = ctx.Request;

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				isInvalidUser &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
				SendUnauthorizedResponse(ctx, statusCode);
				return false;
			}

			if (isInvalidUser == false)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = ctx.User.Identity.Name;
				CurrentOperationContext.User.Value = ctx.User;
			}
			return true;
		}

		private static void SendUnauthorizedResponse(IAuthenticationContext ctx, int statusCode)
		{
			ctx.RegisterResponse(response =>
			{
				response.StatusCode = statusCode;
				response.AddHeader("WWW-Authenticate", "Negotiate");
				response.AddHeader("WWW-Authenticate", "NTLM");
				response.AddHeader("WWW-Authenticate", "Bearer realm=\"Raven\", error=\"invalid_token\",error_description=\"The access token is invalid\"");
			});
		}

		private bool IsInvalidOAuthUser(IAuthenticationContext ctx, bool writeAccess, ref int statusCode)
		{
			var token = GetToken(ctx);

			if (token == null)
			{
				WriteAuthorizationChallenge(ctx, "invalid_request", "The access token is required");
				return true;
			}

			AccessTokenBody tokenBody;
			if (!AccessToken.TryParseBody(Settings.OAuthTokenCertificate, token, out tokenBody))
			{
				WriteAuthorizationChallenge(ctx, "invalid_token", "The access token is invalid");

				return true;
			}

			if (tokenBody.IsExpired())
			{
				WriteAuthorizationChallenge(ctx, "invalid_token", "The access token is expired");

				return true;
			}

			string tenantId;
			HttpServer.TryGetTenantId(ctx.Request.RawUrl, out tenantId);
			
			ctx.User = new OAuthPrincipal(tokenBody, tenantId);

			if (!tokenBody.IsAuthorized(tenantId, writeAccess))
			{
				statusCode = 403;
				WriteAuthorizationChallenge(ctx, "insufficient_scope",
									writeAccess ?
									"Not authorized for read/write access for tenant " + tenantId :
									"Not authorized for tenant " + tenantId);

				return true;
			}

			return false;
		}

		private bool IsInvalidWindowsUser(IAuthenticationContext ctx)
		{
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

		static string GetToken(IAuthenticationContext ctx)
		{
			const string bearerPrefix = "Bearer ";

			var auth = ctx.Request.Headers["Authorization"];

			if (auth == null || auth.Length <= bearerPrefix.Length || !auth.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
				return null;

			var token = auth.Substring(bearerPrefix.Length, auth.Length - bearerPrefix.Length);

			return token;
		}

		void WriteAuthorizationChallenge(IAuthenticationContext ctx, string error, string errorDescription)
		{
			ctx.RegisterResponse(response =>
			{
				if (string.IsNullOrEmpty(Settings.OAuthTokenServer) == false)
				{
					response.AddHeader("OAuth-Source", Settings.OAuthTokenServer);
				}
				response.AddHeader("WWW-Authenticate", string.Format("Bearer realm=\"Raven\", error=\"{0}\",error_description=\"{1}\"", error, errorDescription));
			});	
		}
	}

	public interface IAuthenticationContext
	{
		string RequestUrl { get;  }
		IHttpRequest Request { get; }
		IPrincipal User { get; set; }

		void RegisterResponse(Action<IHttpResponse> action);
	}
}