﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using System.Linq;
using Raven.Database.Server.WebApi;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public abstract class RavenDbApiController : RavenBaseApiController
	{
		public string DatabaseName
		{
			get;
			private set;
		}

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			var result = new HttpResponseMessage();
			if (InnerRequest.Method.Method != "OPTIONS")
			{
				result = await RequestManager.HandleActualRequest(this, async () =>
				{
					SetHeaders();
					return await ExecuteActualRequest(controllerContext, cancellationToken, authorizer);
				}, httpException => GetMessageWithObject(new {Error = httpException.Message}, HttpStatusCode.ServiceUnavailable));
			}

			RequestManager.AddAccessControlHeaders(this, result);

			return result;
		}

		private void SetHeaders()
		{
			foreach (var innerHeader in InnerHeaders)
			{
				CurrentOperationContext.Headers.Value[innerHeader.Key] = innerHeader.Value.FirstOrDefault();
			}
		}

		private async Task<HttpResponseMessage> ExecuteActualRequest(HttpControllerContext controllerContext, CancellationToken cancellationToken,
			MixedModeRequestAuthorizer authorizer)
		{
			HttpResponseMessage authMsg;
			if (authorizer.TryAuthorize(this, out authMsg) == false)
				return authMsg;

			var internalHeader = GetHeader("Raven-internal-request");
			if (internalHeader == null || internalHeader != "true")
				RequestManager.IncrementRequestCount();

			if (DatabaseName != null && await DatabasesLandlord.GetDatabaseInternal(DatabaseName) == null)
			{
				var msg = "Could not find a database named: " + DatabaseName;
				return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
			}

			var sp = Stopwatch.StartNew();

			var result = await base.ExecuteAsync(controllerContext, cancellationToken);
			sp.Stop();
			AddRavenHeader(result, sp);

			return result;
		}

		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
			base.InnerInitialization(controllerContext);
			landlord = (DatabasesLandlord)controllerContext.Configuration.Properties[typeof(DatabasesLandlord)];
			requestManager = (RequestManager)controllerContext.Configuration.Properties[typeof(RequestManager)];

			var values = controllerContext.Request.GetRouteData().Values;
			if (values.ContainsKey("MS_SubRoutes"))
			{
				var routeDatas = (IHttpRouteData[])controllerContext.Request.GetRouteData().Values["MS_SubRoutes"];
				var selectedData = routeDatas.FirstOrDefault(data => data.Values.ContainsKey("databaseName"));

				if (selectedData != null)
					DatabaseName = selectedData.Values["databaseName"] as string;
				else
					DatabaseName = null;
			}
			else
			{
				if (values.ContainsKey("databaseName"))
					DatabaseName = values["databaseName"] as string;
				else
					DatabaseName = null;
			}
		}

		public override HttpResponseMessage GetEmptyMessage(HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var result = base.GetEmptyMessage(code, etag);
			AddAccessControlHeaders(result);
			HandleReplication(result);
			return result;
		}

		public override HttpResponseMessage GetMessageWithObject(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var result = base.GetMessageWithObject(item, code, etag);

			AddAccessControlHeaders(result);
			HandleReplication(result);
			return result;
		}

		public override HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
		{
			var result =base.GetMessageWithString(msg, code, etag);
			AddAccessControlHeaders(result);
			HandleReplication(result);
			return result;
		}

		private void AddRavenHeader(HttpResponseMessage msg, Stopwatch sp)
		{
			AddHeader("Raven-Server-Build", DocumentDatabase.BuildVersion, msg);
			AddHeader("Temp-Request-Time", sp.ElapsedMilliseconds.ToString("#,#;;0", CultureInfo.InvariantCulture), msg);
		}

		private void AddAccessControlHeaders(HttpResponseMessage msg)
		{
			if (string.IsNullOrEmpty(DatabasesLandlord.SystemConfiguration.AccessControlAllowOrigin))
				return;

			AddHeader("Access-Control-Allow-Credentials", "true", msg);

			var originAllowed = DatabasesLandlord.SystemConfiguration.AccessControlAllowOrigin == "*" ||
					DatabasesLandlord.SystemConfiguration.AccessControlAllowOrigin.Split(' ')
						.Any(o => o == GetHeader("Origin"));
			if (originAllowed)
			{
				AddHeader("Access-Control-Allow-Origin", GetHeader("Origin"), msg);
			}

			AddHeader("Access-Control-Max-Age", DatabasesLandlord.SystemConfiguration.AccessControlMaxAge, msg);
			AddHeader("Access-Control-Allow-Methods", DatabasesLandlord.SystemConfiguration.AccessControlAllowMethods, msg);

			if (string.IsNullOrEmpty(DatabasesLandlord.SystemConfiguration.AccessControlRequestHeaders))
			{
				// allow whatever headers are being requested
				var hdr = GetHeader("Access-Control-Request-Headers"); // typically: "x-requested-with"
				if (hdr != null) AddHeader("Access-Control-Allow-Headers", hdr, msg);
			}
			else
			{
				AddHeader("Access-Control-Request-Headers", DatabasesLandlord.SystemConfiguration.AccessControlRequestHeaders, msg);
			}
		}

		private DatabasesLandlord landlord;
		public DatabasesLandlord DatabasesLandlord
		{
			get
			{
				if (Configuration == null)
					return landlord;
				return (DatabasesLandlord)Configuration.Properties[typeof(DatabasesLandlord)];
			}
		}

		private RequestManager requestManager;
		public RequestManager RequestManager
		{
			get
			{
				if (Configuration == null)
					return requestManager;
				return (RequestManager)Configuration.Properties[typeof(RequestManager)];
			}
		}

		public DocumentDatabase Database
		{
			get
			{
				var database = DatabasesLandlord.GetDatabaseInternal(DatabaseName);
				if (database == null)
				{
					throw new InvalidOperationException("Could not find a database named: " + DatabaseName);
				}

				return database.Result;
			}
		}

		public StringBuilder CustomRequestTraceInfo { get; private set; }

		public void AddRequestTraceInfo(string info)
		{
			if(string.IsNullOrEmpty(info))
				return;

			if (CustomRequestTraceInfo == null)
				CustomRequestTraceInfo = new StringBuilder(info);
			else
				CustomRequestTraceInfo.Append(info);
		}

		protected bool EnsureSystemDatabase()
		{
			return DatabasesLandlord.SystemDatabase == Database;
		}

        public int GetNextPageStart()
        {
            bool isNextPage;
            if (bool.TryParse(GetQueryStringValue("next-page"), out isNextPage) && isNextPage)
                return GetStart();

            return 0;
        }

		protected TransactionInformation GetRequestTransaction()
		{
			if (InnerRequest.Headers.Contains("Raven-Transaction-Information") == false)
				return null;
			var txInfo = InnerRequest.Headers.GetValues("Raven-Transaction-Information").FirstOrDefault();
			if (string.IsNullOrEmpty(txInfo))
				return null;
			var parts = txInfo.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
			if (parts.Length != 2)
				throw new ArgumentException("'Raven-Transaction-Information' is in invalid format, expected format is: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, hh:mm:ss'");
			
			return new TransactionInformation
			{
				Id = parts[0],
				Timeout = TimeSpan.ParseExact(parts[1], "c", CultureInfo.InvariantCulture)
			};
		}

		protected IndexQuery GetIndexQuery(int maxPageSize)
		{
			var query = new IndexQuery
			{
				Query = GetQueryStringValue("query") ?? "",
				Start = GetStart(),
				Cutoff = GetCutOff(),
                WaitForNonStaleResultsAsOfNow = GetWaitForNonStaleResultsAsOfNow(),
				CutoffEtag = GetCutOffEtag(),
				PageSize = GetPageSize(maxPageSize),
				SkipTransformResults = GetSkipTransformResults(),
				FieldsToFetch = GetQueryStringValues("fetch"),
				DefaultField = GetQueryStringValue("defaultField"),

				DefaultOperator =
					string.Equals(GetQueryStringValue("operator"), "AND", StringComparison.OrdinalIgnoreCase) ?
						QueryOperator.And :
						QueryOperator.Or,

				SortedFields = EnumerableExtension.EmptyIfNull(GetQueryStringValues("sort"))
					.Select(x => new SortedField(x))
					.ToArray(),
				HighlightedFields = GetHighlightedFields().ToArray(),
				HighlighterPreTags = GetQueryStringValues("preTags"),
				HighlighterPostTags = GetQueryStringValues("postTags"),
				ResultsTransformer = GetQueryStringValue("resultsTransformer"),
				QueryInputs = ExtractQueryInputs(),
				ExplainScores = GetExplainScores(),
				SortHints = GetSortHints(),
				IsDistinct = IsDistinct()
			};

            if (query.WaitForNonStaleResultsAsOfNow)
                query.Cutoff = SystemTime.UtcNow;


			var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
			var queryShape = GetQueryStringValue("queryShape");
			SpatialUnits units;
			var unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
			double distanceErrorPct;
			if (!double.TryParse(GetQueryStringValue("distErrPrc"), out distanceErrorPct))
				distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
			SpatialRelation spatialRelation;
			
			if (Enum.TryParse(GetQueryStringValue("spatialRelation"), false, out spatialRelation) && !string.IsNullOrWhiteSpace(queryShape))
			{
				return new SpatialIndexQuery(query)
				{
					SpatialFieldName = spatialFieldName,
					QueryShape = queryShape,
					RadiusUnitOverride = unitsSpecified ? units : (SpatialUnits?)null,
					SpatialRelation = spatialRelation,
					DistanceErrorPercentage = distanceErrorPct,
				};
			}

			return query;
		}

		private bool IsDistinct()
		{
			var distinct = GetQueryStringValue("distinct");
			if (string.Equals("true", distinct, StringComparison.OrdinalIgnoreCase))
				return true;
			var aggAsString = GetQueryStringValue("aggregation"); // 2.x legacy support
			if (aggAsString == null)
				return false;

			if (string.Equals("Distinct", aggAsString, StringComparison.OrdinalIgnoreCase))
				return true;

			if (string.Equals("None", aggAsString, StringComparison.OrdinalIgnoreCase))
				return false;

			throw new NotSupportedException("AggregationOperation (except Distinct) is no longer supported");
		}

		private Dictionary<string, SortOptions> GetSortHints()
		{
			var result = new Dictionary<string, SortOptions>();

			foreach (var header in InnerRequest.Headers.Where(pair => pair.Key.StartsWith("SortHint-")))
			{
				SortOptions sort;
				Enum.TryParse(GetHeader(header.Key), true, out sort);
				result.Add(Uri.UnescapeDataString(header.Key), sort);
			}

			return result;
		}

		public Etag GetCutOffEtag()
		{
			var etagAsString = GetQueryStringValue("cutOffEtag");
			if (etagAsString != null)
			{
				etagAsString = Uri.UnescapeDataString(etagAsString);

				return Etag.Parse(etagAsString);
			}

			return null;
		}

		private bool GetExplainScores()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("explainScores"), out result);
			return result;
		}

        private bool GetWaitForNonStaleResultsAsOfNow()
        {
            bool result;
            bool.TryParse(GetQueryStringValue("waitForNonStaleResultsAsOfNow"), out result);
            return result;
        }

		public DateTime? GetCutOff()
		{
			var etagAsString = GetQueryStringValue("cutOff");
			if (etagAsString != null)
			{
				etagAsString = Uri.UnescapeDataString(etagAsString);

				DateTime result;
				if (DateTime.TryParseExact(etagAsString, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
					return result;
				throw new BadRequestException("Could not parse cut off query parameter as date");
			}

			return null;
		}

		public bool GetSkipTransformResults()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("skipTransformResults"), out result);
			return result;
		}

		public IEnumerable<HighlightedField> GetHighlightedFields()
		{
			var highlightedFieldStrings = EnumerableExtension.EmptyIfNull(GetQueryStringValues("highlight"));
			var fields = new HashSet<string>();

			foreach (var highlightedFieldString in highlightedFieldStrings)
			{
				HighlightedField highlightedField;
				if (HighlightedField.TryParse(highlightedFieldString, out highlightedField))
				{
					if (!fields.Add(highlightedField.Field))
						throw new BadRequestException("Duplicate highlighted field has found: " + highlightedField.Field);

					yield return highlightedField;
				}
				else 
					throw new BadRequestException("Could not parse highlight query parameter as field highlight options");
			}
		}

		public Dictionary<string, RavenJToken> ExtractQueryInputs()
		{
			var result = new Dictionary<string, RavenJToken>();
			foreach (var key in InnerRequest.GetQueryNameValuePairs().Select(pair => pair.Key))
			{
				if (string.IsNullOrEmpty(key)) continue;
				if (key.StartsWith("qp-"))
				{
					var realkey = key.Substring(3);
					result[realkey] = GetQueryStringValue(key);
				}
			}

			return result;
		}

		protected bool GetOverwriteExisting()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("overwriteExisting"), out result);
			return result;
		}

		protected bool GetCheckReferencesInIndexes()
		{
			bool result;
			bool.TryParse(GetQueryStringValue("checkReferencesInIndexes"), out result);
			return result;
		}

		protected bool GetAllowStale()
		{
			bool stale;
			bool.TryParse(GetQueryStringValue("allowStale"), out stale);
			return stale;
		}

		public string GetRequestUrl()
		{
			var rawUrl = InnerRequest.RequestUri.PathAndQuery;
			return UrlExtension.GetRequestUrlFromRawUrl(rawUrl, DatabasesLandlord.SystemConfiguration);
		}

		protected void HandleReplication(HttpResponseMessage msg)
		{
			var clientPrimaryServerUrl = GetHeader(Constants.RavenClientPrimaryServerUrl);
			var clientPrimaryServerLastCheck = GetHeader(Constants.RavenClientPrimaryServerLastCheck);
			if (string.IsNullOrEmpty(clientPrimaryServerUrl) || string.IsNullOrEmpty(clientPrimaryServerLastCheck))
			{
				return;
			}

			DateTime primaryServerLastCheck;
			if (DateTime.TryParse(clientPrimaryServerLastCheck, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out primaryServerLastCheck) == false)
			{
				return;
			}

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if (replicationTask == null)
			{
				return;
			}

			if (replicationTask.IsHeartbeatAvailable(clientPrimaryServerUrl, primaryServerLastCheck))
			{
				msg.Headers.TryAddWithoutValidation(Constants.RavenForcePrimaryServerCheck, "True");
			}
		}
	}
}