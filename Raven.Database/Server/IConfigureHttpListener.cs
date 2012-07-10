using System.ComponentModel.Composition;
using System.Net;
using Raven.Database.Config;
using Raven.Database.Server.Security;

namespace Raven.Database.Server
{
	[InheritedExport]
	public interface IConfigureHttpListener
	{
		void Initialize(MixedModeAuthorizer authorizer);
		void Configure(HttpListener listener, InMemoryRavenConfiguration config);
	}
}