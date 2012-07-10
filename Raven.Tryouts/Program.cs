using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var listener = new HttpListener
			              	{
			              		Prefixes = {"http://+:8080/"},
								AuthenticationSchemes = 
									AuthenticationSchemes.IntegratedWindowsAuthentication | 
									AuthenticationSchemes.Anonymous,
								AuthenticationSchemeSelectorDelegate = request =>
								                                       	{
								                                       		var authHeader = request.Headers["Authorization"];
																			if(string.IsNullOrEmpty(authHeader))
																				return AuthenticationSchemes.Anonymous;
																			if (authHeader.StartsWith("NTLM") ||
																				authHeader.StartsWith("Negotiate"))
																				return AuthenticationSchemes.IntegratedWindowsAuthentication;
																			return AuthenticationSchemes.Anonymous | AuthenticationSchemes.IntegratedWindowsAuthentication;
								                                       	}
			              	};

			listener.Start();

			while (true)
			{
				var ctx = listener.GetContext();
				Console.WriteLine("Request!");

				if(ctx.Request.QueryString["secret"] == "pass" || ctx.User != null)
				{
					Console.WriteLine("Success " + ctx.User.Identity.Name);
					ctx.Response.StatusCode = 200;
					ctx.Response.Close();
					continue;
				}

				ctx.Response.AddHeader("WWW-Authenticate", "Negotiate");
				ctx.Response.AddHeader("WWW-Authenticate", "NTLM");

				ctx.Response.StatusCode = 401;
				ctx.Response.Close();
			}
		}
	}
}