using System;
using System.Linq;
using Raven.Server.Routing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Routing
{
    public class RouteMatcherTests : NoDisposalNeeded
    {
        public RouteMatcherTests(ITestOutputHelper output) : base(output)
        {
        }

        private static RouteMatcher<string> Build(params string[] paths) =>
            new(paths.Select(x => ("GET", x, x)));

        private static string Match(RouteMatcher<string> matcher, string path, string method = "GET") =>
            matcher.TryMatch(method, path, out _, out var route) ? route : null;

        [RavenFact(RavenTestCategory.Core)]
        public void CanMatch()
        {
            var matcher = Build("/admin/databases", "/databases/*/docs", "/databases/*/queries", "/fs/*/files", "/admin/debug-info");

            Assert.Equal("/admin/databases", Match(matcher, "/admin/databases"));
            Assert.Equal("/admin/debug-info", Match(matcher, "/admin/debug-info"));
            Assert.Equal("/fs/*/files", Match(matcher, "/fs/abc/files"));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanMatchWithRoot()
        {
            var matcher = Build("/", "/build/version", "/databases", "/databases/*/docs", "/databases/*/indexes");

            Assert.Equal("/", Match(matcher, "/"));
            Assert.Equal("/build/version", Match(matcher, "/build/version"));
            Assert.Equal("/databases", Match(matcher, "/databases"));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("databases/northwind/docs", "databases/*/docs", "northwind")]
        [InlineData("databases", "databases", null)]
        [InlineData("databases/northwind/indexes/Raven/DocumentsByEntityName", "databases/*/indexes/$", "northwind")]
        [InlineData("Databases/northwind/Docs", "databases/*/docs", "northwind")]
        [InlineData("Databases/רוח-צפונית/Docs", "databases/*/docs", "רוח-צפונית")]
        [InlineData("DATABASES/x/QUERIES", "databases/*/queries", "x")]
        [InlineData("databases//docs", "databases/*/docs", "")]
        public void CanMatchWithCapture(string path, string expectedRoute, string expectedCapture)
        {
            var matcher = Build("admin/databases", "databases/*/docs", "databases", "databases/*/queries", "databases/*/indexes/$", "fs/*/files", "admin/debug-info");

            Assert.True(matcher.TryMatch("GET", path, out var match, out var route));
            Assert.Equal(expectedRoute, route);
            if (expectedCapture != null)
                Assert.Equal(expectedCapture, match.GetCapture().ToString());
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("/databases/db/docs/")]
        [InlineData("/databases/db/doc")]
        [InlineData("/databases/db/docsx")]
        [InlineData("/databases/db")]
        [InlineData("/databases/")]
        [InlineData("/admin")]
        [InlineData("/admin/databasesx")]
        [InlineData("")]
        [InlineData(null)]
        [InlineData("/databases/db/ſueries")] // long s, upper cased is 'S' by Unicode, but routes ignore only the ASCII case
        [InlineData("/admın/databases")] // dotless i
        [InlineData("/admin/databaKses")] // Kelvin sign
        public void DoesNotMatch(string path)
        {
            var matcher = Build("/admin/databases", "/databases/*/docs", "/databases/*/queries", "/studio/$");

            Assert.Null(Match(matcher, path));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void MatchesTheMethodIgnoringCase()
        {
            var matcher = new RouteMatcher<string>([("GET", "/docs", "get"), ("Post", "/docs", "post"), ("RESET", "/docs", "reset")]);

            Assert.Equal("get", Match(matcher, "/docs", "GET"));
            Assert.Equal("get", Match(matcher, "/docs", "get"));
            Assert.Equal("post", Match(matcher, "/docs", "POST"));
            Assert.Equal("reset", Match(matcher, "/docs", new string("ReSeT".ToCharArray())));
            Assert.Null(Match(matcher, "/docs", "PUT"));
            Assert.Null(Match(matcher, "/docs", "CONNECT"));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("/studio/", "/studio/$", 8)]
        [InlineData("/studio/index.html", "/studio/$", 8)]
        [InlineData("/STUDIO/app/index.html", "/studio/$", 8)]
        [InlineData("/studio/upgrade-info", "/studio/upgrade-info", 20)]
        [InlineData("/studio/upgrade-infox", "/studio/$", 8)] // a prefix route still matches when a longer route almost does
        [InlineData("/databases/db/transformers/a/b", "/databases/*/transformers/$", 27)]
        [InlineData("/databases/db/transformers/", "/databases/*/transformers/$", 27)]
        public void MatchesPrefixRoutes(string path, string expectedRoute, int expectedMatchLength)
        {
            var matcher = Build("/studio/$", "/studio/upgrade-info", "/databases/*/transformers/$", "/databases/*/docs");

            Assert.True(matcher.TryMatch("GET", path, out var match, out var route));
            Assert.Equal(expectedRoute, route);
            Assert.Equal(expectedMatchLength, match.MatchLength);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("/docs", "/DOCS")] // duplicate, ignoring case
        [InlineData("/docs", "/döcs")] // not ASCII
        [InlineData("/databases/*x/docs", "/a")] // a capture must be a whole segment
        [InlineData("/databases/*/docs/*", "/a")] // a single capture
        [InlineData("/studio/$/x", "/a")] // '$' only at the end
        [InlineData("/databases/*/docs", "/databases/info")] // a route that starts like the capture
        [InlineData("/databases/*/docs", "/databases/admin/*/x")] // overlapping captures
        public void RejectsInvalidRoutes(string first, string second)
        {
            Assert.Throws<InvalidOperationException>(() => Build(first, second));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EveryServerRouteMatchesItsOwnPath()
        {
            var routes = RouteScanner.AllRoutes.Values.ToList();
            var matcher = new RouteMatcher<RouteInformation>(routes.Select(x => (x.Method, x.Path, x)));

            foreach (var route in routes)
            {
                var path = route.Path.Replace("*", "Db.1-x").Replace("$", "app/index.html");
                foreach (var (method, casedPath) in new[]
                         {
                             (route.Method, path),
                             (route.Method.ToLowerInvariant(), path.ToUpperInvariant()),
                             (route.Method, path.ToLowerInvariant())
                         })
                {
                    Assert.True(matcher.TryMatch(method, casedPath, out var match, out var found), $"{method} {casedPath} should match {route.Method} {route.Path}");
                    Assert.Same(route, found);

                    if (route.Path.Contains('*'))
                        Assert.Equal(route.Path.IndexOf('*'), match.CaptureStart);
                    if (route.Path.Contains('*'))
                        Assert.Equal("Db.1-x".Length, match.CaptureLength);

                    var expectedMatchLength = route.Path.EndsWith('$') ? route.Path.Replace("*", "Db.1-x").Length - 1 : casedPath.Length;
                    Assert.Equal(expectedMatchLength, match.MatchLength);
                }

                if (route.Path.EndsWith('$') == false)
                {
                    matcher.TryMatch(route.Method, path + "x", out _, out var longer);
                    Assert.NotSame(route, longer);
                }
            }
        }
    }
}
