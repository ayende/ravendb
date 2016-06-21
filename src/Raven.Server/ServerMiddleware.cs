using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace Raven.Server
{
	public interface IRavenServerMiddleware
	{
		Task Invoke(HttpContext ctx, Func<Task> next);
	}

    public static class ServerMiddleware
    {
	    public static IRavenServerMiddleware[] Instances { get; set; } = new IRavenServerMiddleware[0];

    }
}
