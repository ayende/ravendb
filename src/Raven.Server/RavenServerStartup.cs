using System;
using System.Text;
using AsyncFriendlyStackTrace;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Server.Routing;

namespace Raven.Server
{
    public class RavenServerStartup
    {
        public void ConfigureServices(IServiceCollection services)
        {
        }

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerfactory)
        {
            app.UseWebSockets(new WebSocketOptions
            {
                // TODO: KeepAlive causes "Unexpect reserved bit set" (we are sending our own hearbeats, so we do not need this)
                //KeepAliveInterval = Debugger.IsAttached ? 
                //    TimeSpan.FromHours(24) : TimeSpan.FromSeconds(30), 
                KeepAliveInterval = TimeSpan.FromHours(24),
                ReceiveBufferSize = 4096,
            });

            var router = app.ApplicationServices.GetService<RequestRouter>();
            app.Run(async context =>
            {
                try
                {
                    await router.HandlePath(context, context.Request.Method, context.Request.Path.Value);
                }
                catch (Exception e)
                {
                    if (context.RequestAborted.IsCancellationRequested)
                        return;

                    //TODO: special handling for argument exception (400 bad request)
                    //TODO: database not found (503)
                    //TODO: operaton cancelled (timeout)
                    //TODO: Invalid data exception 422

                    var response = context.Response;
                    response.StatusCode = 500;
                    var sb = new StringBuilder();
                    sb.Append(context.Request.Path).Append('?').Append(context.Request.QueryString)
                        .AppendLine()
                        .Append("- - - - - - - - - - - - - - - - - - - - -")
                        .AppendLine();
                    sb.Append(e);
                    string errorString;

                    try
                    {
                        errorString = e.ToAsyncString();
                    }
                    catch (Exception)
                    {
                        errorString = e.ToString();
                    }
                    await response.WriteAsync(errorString);
                }
            });
        }
    }
}