﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Queries;

namespace Raven.Database.Server.Controllers
{
	public class FacetsController : RavenDbApiController
	{
		[HttpGet]
		[Route("facets/{*id}")]
        [Route("databases/{databaseName}/facets/{*id}")]
		public async Task<HttpResponseMessage> FacetsGet(string id)
		{
			return await Facets(id, "GET");
		}

		[HttpPost]
		[Route("facets/{*id}")]
        [Route("databases/{databaseName}/facets/{*id}")]
		public async Task<HttpResponseMessage> FacetsPost(string id)
		{
			return await Facets(id, "POST");
		}

		[HttpPost]
		[Route("facets/multisearch")]
		[Route("databases/{databaseName}/facets/multisearch")]
		public async Task<HttpResponseMessage> MultiSearch()
		{
			var str = await ReadStringAsync();
			var facetedQueries = JsonConvert.DeserializeObject<FacetQuery[]>(str);

			try
			{
				var results =
					facetedQueries.Select(
						facetedQuery =>
						{
							if (facetedQuery.FacetSetupDoc != null)
								return Database.ExecuteGetTermsQuery(facetedQuery.IndexName, facetedQuery.Query, facetedQuery.FacetSetupDoc,
																	 facetedQuery.PageStart, facetedQuery.PageSize);
							if (facetedQuery.Facets != null)
								return Database.ExecuteGetTermsQuery(facetedQuery.IndexName, facetedQuery.Query, facetedQuery.Facets,
								                              facetedQuery.PageStart,
								                              facetedQuery.PageSize);

							throw new InvalidOperationException("Missing a facet setup document or a list of facets");
						}).ToArray();

				return GetMessageWithObject(results);
			}
			catch (Exception ex)
			{
				if (ex is ArgumentException || ex is InvalidOperationException)
				{
					throw new BadRequestException(ex.Message, ex);
				}

				throw;
			}

		}

		private Etag etag;

		private async Task<HttpResponseMessage> Facets(string index, string method)
		{
			var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
			var facetStart = GetFacetStart();
			var facetPageSize = GetFacetPageSize();

			var msg = await TryGetFacets(index, method);
			if (msg.StatusCode != HttpStatusCode.OK)
				return msg;

			if (MatchEtag(etag))
			{
				msg.StatusCode = HttpStatusCode.NotModified;
				return msg;
			}

			try
			{
				return GetMessageWithObject(Database.ExecuteGetTermsQuery(index, indexQuery, facets, facetStart, facetPageSize), HttpStatusCode.OK, etag);
			}
			catch (Exception ex)
			{
				if (ex is ArgumentException || ex is InvalidOperationException)
				{
					throw new BadRequestException(ex.Message, ex);
				}

				throw;
			}
		}

		private List<Facet> facets; 
		private async Task<HttpResponseMessage> TryGetFacets(string index, string method)
		{
			etag = null;
			facets = null;
			switch (method)
			{
				case "GET":
					var facetSetupDoc = GetFacetSetupDoc();
					if (string.IsNullOrEmpty(facetSetupDoc))
					{
						var facetsJson = GetQueryStringValue("facets");
						if (string.IsNullOrEmpty(facetsJson) == false)
							return TryGetFacetsFromString(index, out etag, out facets, facetsJson);
					}

					var jsonDocument = Database.Get(facetSetupDoc, null);
					if (jsonDocument == null)
					{
						return GetMessageWithString("Could not find facet document: " + facetSetupDoc, HttpStatusCode.NotFound);
					}

					etag = GetFacetsEtag(jsonDocument, index);

					facets = jsonDocument.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

					if (facets == null || !facets.Any())
						return GetMessageWithString("No facets found in facets setup document:" + facetSetupDoc, HttpStatusCode.NotFound);
					break;
				case "POST":
					return TryGetFacetsFromString(index, out etag, out facets, await ReadStringAsync());
				default:
					return GetMessageWithString("No idea how to handle this request", HttpStatusCode.BadRequest);

			}

			return GetEmptyMessage();
		}

		private HttpResponseMessage TryGetFacetsFromString(string index, out Etag etag, out List<Facet> facets, string facetsJson)
		{
			etag = GetFacetsEtag(facetsJson, index);

			facets = JsonConvert.DeserializeObject<List<Facet>>(facetsJson);

			if (facets == null || !facets.Any())
				return GetMessageWithString("No facets found in request body", HttpStatusCode.BadRequest);

			return GetMessageWithObject(null, HttpStatusCode.OK, etag);
		}

		private Etag GetFacetsEtag(JsonDocument jsonDocument, string index)
		{
			return jsonDocument.Etag.HashWith(Database.GetIndexEtag(index, null));
		}

		private Etag GetFacetsEtag(string jsonFacets, string index)
		{
			using (var md5 = MD5.Create())
			{
				var etagBytes = md5.ComputeHash(Database.GetIndexEtag(index, null).ToByteArray().Concat(Encoding.UTF8.GetBytes(jsonFacets)).ToArray());
				return Etag.Parse(etagBytes);
			}
		}

		private string GetFacetSetupDoc()
		{
			return GetQueryStringValue("facetDoc") ?? "";
		}

		private int GetFacetStart()
		{
			int start;
			return int.TryParse(GetQueryStringValue("facetStart"), out start) ? start : 0;
		}

		private int? GetFacetPageSize()
		{
			int pageSize;
			if (int.TryParse(GetQueryStringValue("facetPageSize"), out pageSize))
				return pageSize;
			return null;
		}
	}
}