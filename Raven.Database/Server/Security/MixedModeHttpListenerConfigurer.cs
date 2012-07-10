// -----------------------------------------------------------------------
//  <copyright file="MixedModeHttpListenerConfigurer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Security.Principal;
using System.Text.RegularExpressions;
using Raven.Database.Config;
using System.Linq;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security
{
	public class MixedModeHttpListenerConfigurer : IConfigureHttpListener
	{
		public static Regex IsAdminRequest = new Regex(@"(^/admin)|(^/databases/[\w.-_\d]+/admin)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
		private MixedModeAuthorizer requestAuthorizer;

		public void Initialize(MixedModeAuthorizer authorizer)
		{
			requestAuthorizer = authorizer;
		}

		public void Configure(HttpListener listener, InMemoryRavenConfiguration config)
		{
			listener.AuthenticationSchemes = AuthenticationSchemes.Anonymous |
											 AuthenticationSchemes.Basic |
											 AuthenticationSchemes.IntegratedWindowsAuthentication;
			listener.AuthenticationSchemeSelectorDelegate = AuthenticationSchemeSelectorDelegate;
		}

		private class HttpListenerRequestAuthorizationContext : IAuthenticationContext
		{
			private HttpListenerRequest request;
			private HttpListenerRequestAdapter wrappedRequest;

			public HttpListenerRequestAuthorizationContext(HttpListenerRequest request)
			{
				this.request = request;
				this.wrappedRequest = new HttpListenerRequestAdapter(request);
			}

			public string RequestUrl
			{
				get { return request.RawUrl; }
			}

			public IHttpRequest Request
			{
				get { return wrappedRequest;}
			}

			public IPrincipal User
			{
				get { return null; }
				set { }
			}

			public void RegisterResponse(Action<IHttpResponse> action)
			{
				// we won't be sending a response here
			}
		}

		private AuthenticationSchemes AuthenticationSchemeSelectorDelegate(HttpListenerRequest request)
		{
			var authorize = requestAuthorizer.Authorize(new HttpListenerRequestAuthorizationContext(request));

			var authHeader = request.Headers["Authorization"];
			if (authorize && authHeader != null)
			{
				if (authHeader.StartsWith("NTLM") || authHeader.StartsWith("Negotiate"))
					return AuthenticationSchemes.IntegratedWindowsAuthentication;
				return AuthenticationSchemes.Anonymous;
			}

			if (NeverSecret.Urls.Contains(request.Url.AbsolutePath))
				return AuthenticationSchemes.Anonymous;

			if (request.RawUrl.StartsWith("/OAuth/AccessToken", StringComparison.InvariantCultureIgnoreCase))
			{
				return AuthenticationSchemes.Anonymous;
			}

			if (IsAdminRequest.IsMatch(request.RawUrl))
				return AuthenticationSchemes.IntegratedWindowsAuthentication;

			return authorize  ? 
				AuthenticationSchemes.Anonymous : 
				AuthenticationSchemes.IntegratedWindowsAuthentication;
		}
	}
}