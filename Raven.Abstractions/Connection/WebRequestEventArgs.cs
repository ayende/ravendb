//-----------------------------------------------------------------------
// <copyright file="WebRequestEventArgs.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Net;

#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#endif

namespace Raven.Abstractions.Connection
{
	/// <summary>
	/// Event arguments for the event of creating a <see cref="WebRequest"/>
	/// </summary>
	public class WebRequestEventArgs : EventArgs
	{
#if NETFX_CORE

		/// <summary>
		/// Gets or sets the web request.
		/// </summary>
		/// <value>The request.</value>
		public System.Net.Http.HttpClient Client { get; set; }

#else

		/// <summary>
		/// Gets or sets the web request.
		/// </summary>
		/// <value>The request.</value>
		public WebRequest Request { get; set; }
#endif

#if SILVERLIGHT
	/// <summary>
	/// The RavenDB json request
	/// </summary>
		public HttpJsonRequest JsonRequest { get; set; }
#endif
	}
}