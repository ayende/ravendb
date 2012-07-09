// -----------------------------------------------------------------------
//  <copyright file="MixedModeHttpListenerConfigurer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Database.Config;
using System.Linq;

namespace Raven.Database.Server.Security
{
	public class MixedModeHttpListenerConfigurer : IConfigureHttpListener
	{
		public void Configure(HttpListener listener, InMemoryRavenConfiguration config)
		{
			listener.AuthenticationSchemes = AuthenticationSchemes.Anonymous | AuthenticationSchemes.Basic |
			                                 AuthenticationSchemes.IntegratedWindowsAuthentication;
			listener.AuthenticationSchemeSelectorDelegate = AuthenticationSchemeSelectorDelegate;
		}

		private AuthenticationSchemes AuthenticationSchemeSelectorDelegate(HttpListenerRequest request)
		{
			if (NeverSecret.Urls.Contains(request.Url.AbsolutePath, StringComparer.InvariantCultureIgnoreCase))
				return AuthenticationSchemes.Anonymous; 
			
			if (request.RawUrl.StartsWith("/OAuth/AccessToken", StringComparison.InvariantCultureIgnoreCase))
			{
				// only here we support basic auth
				return AuthenticationSchemes.Basic | AuthenticationSchemes.Anonymous;
			}

			return AuthenticationSchemes.Anonymous | AuthenticationSchemes.IntegratedWindowsAuthentication;
		}
	}
}