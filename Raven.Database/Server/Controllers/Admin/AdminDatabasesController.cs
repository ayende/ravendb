﻿using System;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminDatabasesController : BaseAdminController
	{
		[HttpGet][Route("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesGet(string id)
		{
			if (IsSystemDatabase(id))
			{
				//fetch fake (empty) system database document
				var systemDatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
				return GetMessageWithObject(systemDatabaseDocument);
			}

			var docKey = "Raven/Databases/" + id;

			var document = Database.Documents.Get(docKey, null);
			if (document == null)
				return GetMessageWithString("Database " + id + " not found", HttpStatusCode.NotFound);

			var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			dbDoc.Id = id;
			DatabasesLandlord.Unprotect(dbDoc);
			return GetMessageWithObject(dbDoc);
		}

		[HttpPut][Route("admin/databases/{*id}")]
		public async Task<HttpResponseMessage> DatabasesPut(string id)
		{
			if (IsSystemDatabase(id))
				return GetMessageWithString("System Database document cannot be changed", HttpStatusCode.Forbidden);

			var docKey = "Raven/Databases/" + id;
			var dbDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			DatabasesLandlord.Protect(dbDoc);
			var json = RavenJObject.FromObject(dbDoc);
			json.Remove("Id");

			Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

			return GetEmptyMessage();
		}

		[HttpDelete][Route("admin/databases/{*id}")]
		public HttpResponseMessage DatabasesDelete(string id)
		{
			if (IsSystemDatabase(id))
				return GetMessageWithString("System Database document cannot be deleted", HttpStatusCode.Forbidden);

			var docKey = "Raven/Databases/" + id;
			var configuration = DatabasesLandlord.CreateTenantConfiguration(id);
			var databasedocument = Database.Documents.Get(docKey, null);

			if (configuration == null)
				return GetEmptyMessage();

			Database.Documents.Delete(docKey, null, null);
			bool result;

			if (bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result)
			{
				IOExtensions.DeleteDirectory(configuration.DataDirectory);
				IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

				if (databasedocument != null)
				{
					var dbDoc = databasedocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
					if (dbDoc != null && dbDoc.Settings.ContainsKey(Constants.RavenLogsPath))
						IOExtensions.DeleteDirectory(dbDoc.Settings[Constants.RavenLogsPath]);
				}
			}

			return GetEmptyMessage();
		}

	}
}