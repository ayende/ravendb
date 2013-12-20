//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Database.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
#if SILVERLIGHT || NETFX_CORE
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
#else
using System.Transactions;
#endif
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Listeners;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using System.Collections.Specialized;

namespace Raven.Client.Connection.Async
{
	/// <summary>
	/// Access the database commands in async fashion
	/// </summary>
	public class AsyncServerClient : IAsyncDatabaseCommands, IAsyncAdminDatabaseCommands, IAsyncInfoDatabaseCommands,
									 IAsyncGlobalAdminDatabaseCommands
	{
		private readonly ProfilingInformation profilingInformation;
		private readonly IDocumentConflictListener[] conflictListeners;
		private readonly string url;
		private readonly string rootUrl;
		private readonly OperationCredentials credentials;
		internal readonly DocumentConvention convention; 
		private NameValueCollection operationsHeaders = new NameValueCollection();
		internal readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? sessionId;
		private readonly Func<string, ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private int requestCount;
		private int readStripingBase;

		private readonly ICredentials _credentials;
		private readonly string _apiKey;

		public string Url
		{
			get { return url; }
		}

		public ReplicationInformer ReplicationInformer
		{
			get { return replicationInformer; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		public AsyncServerClient(string url, DocumentConvention convention, string apiKey, ICredentials credentials,
								 HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId,
								 Func<string, ReplicationInformer> replicationInformerGetter, string databaseName,
								 IDocumentConflictListener[] conflictListeners)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			this.url = url;
			if (this.url.EndsWith("/"))
				this.url = this.url.Substring(0, this.url.Length - 1);
			rootUrl = this.url;
			var databasesIndex = rootUrl.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
			if (databasesIndex > 0)
			{
				rootUrl = rootUrl.Substring(0, databasesIndex);
			}
			this.jsonRequestFactory = jsonRequestFactory;
			this.sessionId = sessionId;
			this.convention = convention;
			this.credentials = new OperationCredentials(apiKey, credentials);
			_apiKey = apiKey;
			_credentials = credentials;
			this.databaseName = databaseName;
			this.conflictListeners = conflictListeners;
			this.replicationInformerGetter = replicationInformerGetter;
			this.replicationInformer = replicationInformerGetter(databaseName);
			this.readStripingBase = replicationInformer.GetReadStripingBase();
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
		}

		/// <summary>
		/// Gets the index names from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var json = (RavenJArray)await operationMetadata.Url.IndexNames(start, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
														   .AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer,
																						convention.FailoverBehavior,
																						HandleReplicationStatusChanges)
														   .ReadResponseJsonAsync();

				return json.Select(x => x.Value<string>()).ToArray();
			});
		}

		/// <summary>
		/// Gets the indexes from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/indexes/?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", credentials, convention));
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var json = (RavenJArray)await request.ReadResponseJsonAsync();
				//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
				return json.Select(x =>
				{
					var value = ((RavenJObject)x)["definition"].ToString();
					return JsonConvert.DeserializeObject<IndexDefinition>(value, new JsonToJsonConverter());
				})
							.ToArray();
			});
		}

		/// <summary>
		/// Gets the transformers from the server asynchronously
		/// </summary>
		public Task<TransformerDefinition[]> GetTransformersAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/transformers?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", credentials, convention));
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var json = (RavenJArray)await request.ReadResponseJsonAsync();

				//NOTE: To review, I'm not confidence this is the correct way to deserialize the transformer definition
				return
					json.Select(
						x =>
						JsonConvert.DeserializeObject<TransformerDefinition>(((RavenJObject)x)["definition"].ToString(),
																			 new JsonToJsonConverter()))
							.ToArray();
			});
		}


		/// <summary>
		/// Resets the specified index asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task ResetIndexAsync(string name)
		{
			return ExecuteWithReplication("RESET", operationMetadata =>
			{
				var httpJsonRequestAsync =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/" + name,
																							 "RESET", credentials, convention));
				httpJsonRequestAsync.AddOperationHeaders(OperationsHeaders);
				httpJsonRequestAsync.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
																 HandleReplicationStatusChanges);

				return httpJsonRequestAsync.ReadResponseJsonAsync();
			});
		}

		public Task<string> PutIndexAsync<TDocument, TReduceResult>(string name,
					 IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite = false)
		{
			return PutIndexAsync(name, indexDef.ToIndexDefinition(convention), overwrite);
		}

		/// <summary>
		/// Puts the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite)
		{
			return ExecuteWithReplication("PUT", opUrl => DirectPutIndexAsync(name, indexDef, overwrite, opUrl));
		}

		/// <summary>
		/// Puts the transformer definition for the specified name asynchronously
		/// </summary>
		public Task<string> PutTransformerAsync(string name, TransformerDefinition transformerDefinition)
		{
			return ExecuteWithReplication("PUT", opUrl => DirectPutTransformerAsync(name, transformerDefinition, opUrl));
		}

		/// <summary>
		/// Puts the index definition for the specified name asynchronously with url
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		/// <param name="operationMetadata">The metadata that contains URL and credentials to perform operation</param>
		public async Task<string> DirectPutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, OperationMetadata operationMetadata)
		{
			var requestUri = operationMetadata.Url + "/indexes/" + Uri.EscapeUriString(name) + "?definition=yes";
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			webRequest.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
												   HandleReplicationStatusChanges);

			try
			{
				await webRequest.ExecuteRequestAsync();
				if (overwrite == false)
					throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
			}
			catch (ErrorResponseException e)
			{
				if (e.Response.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "PUT", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);

			ErrorResponseException responseException;
			try
			{
				await request.WriteAsync(serializeObject);
				var result = await request.ReadResponseJsonAsync();
				return result.Value<string>("Index");
			}
			catch (ErrorResponseException e)
			{
				if (e.Response.StatusCode != HttpStatusCode.BadRequest)
					throw;
				responseException = e;
			}
			var error =
				await
				responseException.TryReadErrorResponseObject(
																	new { Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = "" });
			if (error == null)
				throw responseException;

			throw new IndexCompilationException(error.Message)
			{
				IndexDefinitionProperty = error.IndexDefinitionProperty,
				ProblematicText = error.ProblematicText
			};
		}

		/// <summary>
		/// Puts the transformer definition for the specified name asynchronously with url
		/// </summary>
		public async Task<string> DirectPutTransformerAsync(string name, TransformerDefinition transformerDefinition,
															OperationMetadata operationMetadata)
		{
			var requestUri = operationMetadata.Url + "/transformers/" + name;

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "PUT", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			var serializeObject = JsonConvert.SerializeObject(transformerDefinition, Default.Converters);

			ErrorResponseException responseException;
			try
			{
				await request.WriteAsync(serializeObject);
				var result = await request.ReadResponseJsonAsync();
				return result.Value<string>("Transformer");
			}
			catch (ErrorResponseException e)
			{
				if (e.Response.StatusCode != HttpStatusCode.BadRequest)
					throw;

				responseException = e;
			}
			var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "" });
			if (error == null)
				throw responseException;

			throw new TransformCompilationException(error.Message);
		}

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			return ExecuteWithReplication("DELETE", operationMetadata => operationMetadata.Url.Indexes(name)
																				.ToJsonRequest(this, credentials, convention,
																							   OperationsHeaders, "DELETE")
																				.AddReplicationStatusHeaders(url, operationMetadata.Url,
																											 replicationInformer,
																											 convention
																												 .FailoverBehavior,
																											 HandleReplicationStatusChanges)
																		.ExecuteRequestAsync());
		}

		public Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete)
		{
			return DeleteByIndexAsync(indexName, queryToDelete, false);
		}

		public Task<Operation> DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			return ExecuteWithReplication("DELETE", async operationMetadata =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, "DELETE", credentials, convention)
								.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
						HandleReplicationStatusChanges);
				RavenJToken jsonResponse;
				try
				{
					jsonResponse = await request.ReadResponseJsonAsync();
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName, e);
					throw;
				}

				// Be compitable with the resopnse from v2.0 server
				var serverBuild = request.ResponseHeaders.GetAsInt("Raven-Server-Build");
				if (serverBuild < 2500)
				{
					if (serverBuild != 13 || (serverBuild == 13 && jsonResponse.Value<long>("OperationId") == default(long)))
					{
						return null;
					}
				}

				var opId = ((RavenJObject)jsonResponse)["OperationId"];

				if (opId == null || opId.Type != JTokenType.Integer)
					return null;

				return new Operation(this, opId.Value<long>());
			});
		}

		public Task DeleteTransformerAsync(string name)
		{
			return ExecuteWithReplication("DELETE", operationMetadata => operationMetadata.Url.Transformer(name)
			                                                                              .ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
			                                                                              .AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
			                                                                              .ExecuteRequestAsync());
		}

		/// <summary>
		/// Deletes the document for the specified id asynchronously
		/// </summary>
		/// <param name="id">The id.</param>
		public Task DeleteDocumentAsync(string id)
		{
			return ExecuteWithReplication("DELETE", operationMetadata =>
			{
				return operationMetadata.Url.Doc(id)
					.ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
					.ExecuteRequestAsync();
			});
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, bool ignoreMissing)
		{
			var batchResults = await BatchAsync(new ICommandData[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patches,
							}
					});
			if (!ignoreMissing && batchResults[0].PatchResult != null &&
				batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, Etag etag)
		{
			var batchResults = await BatchAsync(new ICommandData[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patches,
								Etag = etag
							}
					});
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patchesToExisting,
												   PatchRequest[] patchesToDefault, RavenJObject defaultMetadata)
		{
			var batchResults = await BatchAsync(new ICommandData[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patchesToExisting,
								PatchesIfMissing = patchesToDefault,
								Metadata = defaultMetadata
							}
					});
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
			var batchResults = await BatchAsync(new ICommandData[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patch,
				}
			});
			if (!ignoreMissing && batchResults[0].PatchResult != null &&
				batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, Etag etag)
		{
			var batchResults = await BatchAsync(new ICommandData[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patch,
					Etag = etag
				}
			});
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patchExisting,
												   ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata)
		{
			var batchResults = await BatchAsync(new ICommandData[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patchExisting,
					PatchIfMissing = patchDefault,
					Metadata = defaultMetadata
				}
			});
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		public Task<PutResult> PutAsync(string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			return ExecuteWithReplication("PUT", operationMetadata => DirectPutAsync(operationMetadata, key, etag, document, metadata));
		}

		private async Task<PutResult> DirectPutAsync(OperationMetadata operationMetadata, string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			if (etag != null)
				metadata["ETag"] = new RavenJValue((string)etag);

			if (key != null)
				key = Uri.EscapeDataString(key);

			var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, method, metadata, credentials, convention)
						.AddOperationHeaders(OperationsHeaders));


			request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
												HandleReplicationStatusChanges);

			try
			{
				await request.WriteAsync(document.ToString());
			}
			catch (ErrorResponseException)
			{
				throw;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Unable to write to server", e);
			}

			ErrorResponseException responseException;
			try
			{
				var result = await request.ReadResponseJsonAsync();
				return convention.CreateSerializer().Deserialize<PutResult>(new RavenJTokenReader(result));
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode != HttpStatusCode.Conflict)
					throw;
				responseException = e;
			}
			throw await FetchConcurrencyException(responseException);
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IAsyncDatabaseCommands ForDatabase(string database)
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			databaseUrl = databaseUrl + "/databases/" + database + "/";
			if (databaseUrl == url)
				return this;
			return new AsyncServerClient(databaseUrl, convention, _apiKey, _credentials, jsonRequestFactory, sessionId,
										 replicationInformerGetter, database, conflictListeners)
			{
				operationsHeaders = operationsHeaders
			};
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IAsyncDatabaseCommands ForSystemDatabase()
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			if (databaseUrl == url)
				return this;
			return new AsyncServerClient(databaseUrl, convention, _apiKey, _credentials, jsonRequestFactory, sessionId,
										 replicationInformerGetter, databaseName, conflictListeners)
			{
				operationsHeaders = operationsHeaders
			};
		}




		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		public NameValueCollection OperationsHeaders
		{
			get { return operationsHeaders; }
			set { operationsHeaders = value; }
		}

		/// <summary>
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<JsonDocument> GetAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return ExecuteWithReplication("GET", operationMetadata => DirectGetAsync(operationMetadata, key));
		}




		/// <summary>
		/// Gets the transformer definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<TransformerDefinition> GetTransformerAsync(string name)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				try
				{
					var transformerDefinitionJson = (RavenJObject)await operationMetadata.Url.Transformer(name)
						.NoCache()
						.ToJsonRequest(this, credentials, convention)
						.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
						.ReadResponseJsonAsync();

					var value = transformerDefinitionJson.Value<RavenJObject>("Transformer");
					return convention.CreateSerializer().Deserialize<TransformerDefinition>(new RavenJTokenReader(value));
				}
				catch (ErrorResponseException we)
				{
					if (we.StatusCode == HttpStatusCode.NotFound)
						return null;

					throw;
				}
			});
		}


		/// <summary>
		/// Gets the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				try
				{
					var indexDefinitionJson = (RavenJObject)await operationMetadata.Url.IndexDefinition(name)
																			   .NoCache()
																			   .ToJsonRequest(this, credentials, convention)
																			   .AddReplicationStatusHeaders(url, operationMetadata.Url,
																											replicationInformer,
																											convention.FailoverBehavior,
																											HandleReplicationStatusChanges)
																			   .ReadResponseJsonAsync();

					var value = indexDefinitionJson.Value<RavenJObject>("Index");
					return convention.CreateSerializer().Deserialize<IndexDefinition>(new RavenJTokenReader(value));
				}
				catch (ErrorResponseException we)
				{
					if (we.StatusCode == HttpStatusCode.NotFound)
						return null;

					throw;
				}

			});
		}

		public async Task<JsonDocument> DirectGetAsync(OperationMetadata operationMetadata, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																						 (operationMetadata.Url + "/docs/" + Uri.EscapeDataString(key)),
																						 "GET", metadata, credentials, convention)
				.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
												HandleReplicationStatusChanges);

			Task<JsonDocument> resolveConflictTask;
			try
			{
				var requestJson = await request.ReadResponseJsonAsync();
				var docKey = request.ResponseHeaders.Get(Constants.DocumentIdFieldName) ?? key;
				docKey = Uri.UnescapeDataString(docKey);
				request.ResponseHeaders.Remove(Constants.DocumentIdFieldName);
				var deserializeJsonDocument = SerializationHelper.DeserializeJsonDocument(docKey, requestJson,
																						  request.ResponseHeaders,
																		  request.Response.StatusCode);
				return deserializeJsonDocument;
			}
			catch (ErrorResponseException e)
			{
				switch (e.StatusCode)
				{
					case HttpStatusCode.NotFound:
						return null;
					case HttpStatusCode.Conflict:
						resolveConflictTask = ResolveConflict(e.Response, operationMetadata, key);
						break;
					default:
						throw;
				}
			}
			return await resolveConflictTask;
		}

		private async Task<JsonDocument> ResolveConflict(HttpResponseMessage httpWebResponse, OperationMetadata operationMetadata, string key)
		{
			var conflicts = new StreamReader(await httpWebResponse.GetResponseStreamWithHttpDecompression());
			var conflictsDoc = RavenJObject.Load(new RavenJsonTextReader(conflicts));
			var result =
				await TryResolveConflictOrCreateConcurrencyException(operationMetadata, key, conflictsDoc, httpWebResponse.GetEtagHeader());
			if (result != null)
				throw result;
			return await DirectGetAsync(operationMetadata, key);
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, string transformer = null,
											  Dictionary<string, RavenJToken> queryInputs = null, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", operationMetadata => DirectGetAsync(operationMetadata, keys, includes, transformer, queryInputs, metadataOnly));
		}

		private async Task<MultiLoadResult> DirectGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, string transformer,
														   Dictionary<string, RavenJToken> queryInputs, bool metadataOnly)
		{
			var path = operationMetadata.Url + "/queries/?";
			if (metadataOnly)
				path += "metadata-only=true&";
			if (includes != null && includes.Length > 0)
			{
				path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			if (string.IsNullOrEmpty(transformer) == false)
				path += "&transformer=" + transformer;

			if (queryInputs != null)
			{
				path = queryInputs.Aggregate(path,
											 (current, queryInput) =>
											 current + ("&" + string.Format("qp-{0}={1}", queryInput.Key, queryInput.Value)));
			}

			var uniqueIds = new HashSet<string>(keys);
			HttpJsonRequest request;
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load > 128 items are going to be rare
			if (uniqueIds.Sum(x => x.Length) < 1024)
			{
				path += "&" + string.Join("&", uniqueIds.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
				request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", credentials,
																							 convention)
																 .AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var result = await request.ReadResponseJsonAsync();
				return await CompleteMultiGetAsync(operationMetadata, keys, includes, result);
			}
			request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, "POST", credentials, convention)
															 .AddOperationHeaders(OperationsHeaders));

			await request.WriteAsync(new RavenJArray(uniqueIds).ToString(Formatting.None));
			var responseResult = await request.ReadResponseJsonAsync();
			return await CompleteMultiGetAsync(operationMetadata, keys, includes, responseResult);
		}

		private async Task<MultiLoadResult> CompleteMultiGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes,
																  RavenJToken result)
		{
			ErrorResponseException responseException;
			try
			{
				var multiLoadResult = new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				};

				var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

				return
					await
					RetryOperationBecauseOfConflict(operationMetadata, docResults, multiLoadResult,
													() => DirectGetAsync(operationMetadata, keys, includes, null, null, false));
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode != HttpStatusCode.Conflict)
					throw;
				responseException = e;
			}
			throw await FetchConcurrencyException(responseException);
		}

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", async url =>
			{
                var result = await GetDocumentsInternalAsync(start, null, pageSize, metadataOnly);

			    return result.Cast<RavenJObject>()
			                 .ToJsonDocuments()
			                 .ToArray();
			});
		}

	    public Task<JsonDocument[]> GetDocumentsAsync(Etag fromEtag, int pageSize, bool metadataOnly = false)
	    {
            return ExecuteWithReplication("GET", async url =>
            {
                var result = await GetDocumentsInternalAsync(null, fromEtag, pageSize, metadataOnly);
                return result.Cast<RavenJObject>()
                             .ToJsonDocuments()
                             .ToArray();
            });
	    }

        public async Task<RavenJArray> GetDocumentsInternalAsync(int? start, Etag fromEtag, int pageSize, bool metadataOnly = false)
        {
            var requestUri = url + "/docs/?";
            if (start.HasValue && start.Value > 0)
            {
                requestUri += "start=" + start;
            }
			else if (fromEtag != null)
            {
                requestUri += "etag=" + fromEtag;
            }
            requestUri += "&pageSize=" + pageSize;
	        if (metadataOnly)
	            requestUri += "&metadata-only=true";
	        var @params = new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
	            .AddOperationHeaders(OperationsHeaders);
	        return (RavenJArray) await jsonRequestFactory.CreateHttpJsonRequest(@params)
	                                                    .ReadResponseJsonAsync();
	    }

		public Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			var requestData = RavenJObject.FromObject(patch).ToString(Formatting.Indented);
			return UpdateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, "EVAL");
		}

		public Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests,
				bool allowStale = false)
		{
			var requestData = new RavenJArray(patchRequests.Select(x => x.ToJson())).ToString(Formatting.Indented);
			return UpdateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, "PATCH");
		}

		public async Task<MultiLoadResult> MoreLikeThisAsync(MoreLikeThisQuery query)
		{
			var requestUrl = query.GetRequestUri();
			EnsureIsNotNullOrEmpty(requestUrl, "url");
			var result = await ExecuteWithReplication("GET", async serverUrl =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, serverUrl + requestUrl, "GET", metadata, credentials, convention)
								.AddOperationHeaders(OperationsHeaders));

				return await request.ReadResponseJsonAsync();
			});
			return ((RavenJObject)result).Deserialize<MultiLoadResult>(convention);
		}

		public Task<long> NextIdentityForAsync(string name)
		{
			return ExecuteWithReplication("POST", async url =>
			{
				var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, url + "/identity/next?name=" + Uri.EscapeDataString(name), "POST", credentials, convention)
								.AddOperationHeaders(OperationsHeaders));
				var readResponseJson = await request.ReadResponseJsonAsync();
				return readResponseJson.Value<long>("Value");
			});
		}

		private Task<Operation> UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, bool allowStale, String requestData, String method)
		{
			return ExecuteWithReplication(method, async operationMetadata =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + allowStale;

				var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, method, credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				await request.WriteAsync(requestData);

				RavenJToken jsonResponse;
				try
				{
					jsonResponse = await request.ReadResponseJsonAsync();
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}

				return new Operation(this, jsonResponse.Value<long>("OperationId"));
			});
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc, int start = 0,
												 int? pageSize = null)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/facets/{0}?facetDoc={1}&query={2}&facetStart={3}&facetPageSize={4}",
				Uri.EscapeUriString(index),
				Uri.EscapeDataString(facetSetupDoc),
				Uri.EscapeDataString(query.Query),
				start,
				pageSize);

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var json = (RavenJObject)await request.ReadResponseJsonAsync();
				return json.JsonDeserialization<FacetResults>();
			});
		}

		public Task<FacetResults[]> GetMultiFacetsAsync(FacetQuery[] facetedQueries)
		{
			return ExecuteWithReplication("POST", async operationMetadata =>
			{

				var requestUri = operationMetadata.Url + "/facets/multisearch";

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "POST", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var data = JsonConvert.SerializeObject(facetedQueries);
				await request.WriteAsync(data);
				var response = (RavenJArray)await request.ReadResponseJsonAsync();

				return convention.CreateSerializer().Deserialize<FacetResults[]>(new RavenJTokenReader(response));
			});
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facets">List of facets</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, List<Facet> facets, int start = 0,
												 int? pageSize = null)
		{

			string facetsJson = JsonConvert.SerializeObject(facets);
			var method = facetsJson.Length > 1024 ? "POST" : "GET";
			return ExecuteWithReplication(method, async operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/facets/{0}?{1}&facetStart={2}&facetPageSize={3}",
																Uri.EscapeUriString(index),
																query.GetMinimalQueryString(),
																start,
																pageSize);

				if (method == "GET")
					requestUri += "&facets=" + Uri.EscapeDataString(facetsJson);

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), method, credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
												.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer,
																			 convention.FailoverBehavior,
																			 HandleReplicationStatusChanges);

				if (method != "GET")
					request.WriteAsync(facetsJson).Wait();

				var json = (RavenJObject)await request.ReadResponseJsonAsync();
				return json.JsonDeserialization<FacetResults>();
			});
		}

		public Task<LogItem[]> GetLogsAsync(bool errorsOnly)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var requestUri = url + "/logs";
				if (errorsOnly)
					requestUri += "?type=error";

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET",
																							 credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var result = await request.ReadResponseJsonAsync();
				return convention.CreateSerializer().Deserialize<LogItem[]>(new RavenJTokenReader(result));
			});
		}

		public async Task<LicensingStatus> GetLicenseStatusAsync()
		{
			var request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/license/status").NoCache(),
																						 "GET", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);

			var result = await request.ReadResponseJsonAsync();
			return convention.CreateSerializer().Deserialize<LicensingStatus>(new RavenJTokenReader(result));
		}

		public async Task<BuildNumber> GetBuildNumberAsync()
		{
			var request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/build/version").NoCache(),
																						 "GET", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);

			var result = await request.ReadResponseJsonAsync();
			return convention.CreateSerializer().Deserialize<BuildNumber>(new RavenJTokenReader(result));
		}

		public Task StartIndexingAsync()
		{
			return ExecuteWithReplication("POST", operationMetadata =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationMetadata.Url + "/admin/StartIndexing")
																								 .NoCache(),
																							 "POST", credentials, convention));

				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/admin/backup").NoCache(), "POST", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteWriteAsync(new RavenJObject
			                                 {
				                                 {"BackupLocation", backupLocation},
				                                 {"DatabaseDocument", RavenJObject.FromObject(databaseDocument)}
			                                 }.ToString(Formatting.None));
		}

		public Task StartRestoreAsync(string restoreLocation, string databaseLocation, string name = null, bool defrag = false)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/admin/restore?defrag=" + defrag).NoCache(), "POST", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteWriteAsync(new RavenJObject
			{
				{"RestoreLocation", restoreLocation},
				{"DatabaseLocation", databaseLocation},
				{"DatabaseName", name}
			}.ToString(Formatting.None));
		}

		public Task<string> GetIndexingStatusAsync()
		{
			throw new NotImplementedException();
		}

		public Task StopIndexingAsync()
		{
			return ExecuteWithReplication("POST", operationMetadata =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationMetadata.Url + "/admin/StopIndexing")
																								 .NoCache(),
																							 "POST", credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, string matches, int start, int pageSize, bool metadataOnly = false, string exclude = null)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var actualUrl = string.Format("{0}/docs?startsWith={1}&matches={4}&exclude={5}&start={2}&pageSize={3}", operationMetadata,
																		 Uri.EscapeDataString(keyPrefix), start.ToInvariantString(), pageSize.ToInvariantString(),
																		 Uri.EscapeDataString(matches ?? ""),
																		 Uri.EscapeDataString(exclude ?? ""));
				if (metadataOnly)
					actualUrl += "&metadata-only=true";
				var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, actualUrl.NoCache(), "GET", metadata, credentials, convention)
								.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
								.ContinueWith(
										task =>
										SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)task.Result).OfType<RavenJObject>()).ToArray());

			});
		}

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests)
		{
			return ExecuteWithReplication("GET", async operationMetadata => // logical GET even though the actual request is a POST
			{
				var multiGetOperation = new MultiGetOperation(this, convention, operationMetadata.Url, requests);

				var httpJsonRequest =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 multiGetOperation.RequestUri.NoCache(),
																							 "POST", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				httpJsonRequest.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
															HandleReplicationStatusChanges);

				var requestsForServer = multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

				var postedData = JsonConvert.SerializeObject(requestsForServer);

				if (multiGetOperation.CanFullyCache(jsonRequestFactory, httpJsonRequest, postedData))
				{
					var cachedResponses = multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length], jsonRequestFactory);
					return cachedResponses;
				}

				await httpJsonRequest.WriteAsync(postedData);
				var result = await httpJsonRequest.ReadResponseJsonAsync();
				var responses = convention.CreateSerializer().Deserialize<GetResponse[]>(new RavenJTokenReader(result));
				return multiGetOperation.HandleCachingResponse(responses, jsonRequestFactory);
			});
		}

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <returns></returns>
		public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes, bool metadataOnly = false, bool indexEntriesOnly = false)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				EnsureIsNotNullOrEmpty(index, "index");
				var path = query.GetIndexQueryUrl(operationMetadata.Url, index, "indexes");
				if (metadataOnly)
					path += "&metadata-only=true";
				if (indexEntriesOnly)
					path += "&debug=entries";
				if (includes != null && includes.Length > 0)
				{
					path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
				}

				var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", credentials, convention)
						{
							AvoidCachingRequest = query.DisableCaching
						}.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(operationMetadata.Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				ErrorResponseException responseException;
				try
				{
					var result = (RavenJObject)await request.ReadResponseJsonAsync();
					return SerializationHelper.ToQueryResult(result, request.ResponseHeaders.GetEtagHeader(),
																									 request.ResponseHeaders.Get("Temp-Request-Time"));
				}
				catch (ErrorResponseException e)
				{
					if (e.Response.StatusCode == HttpStatusCode.NotFound)
					{
						var text = new StreamReader(e.Response.GetResponseStreamWithHttpDecompression().Result).ReadToEnd();
						if (text.Contains("maxQueryString"))
							throw new ErrorResponseException(e.Response, text);
						throw new ErrorResponseException(e.Response, "There is no index named: " + index);
					}
					responseException = e;
				}
				if (await HandleException(responseException))
					return null;
				throw responseException;
			});
		}

		/// <summary>
		/// Attempts to handle an exception raised when receiving a response from the server
		/// </summary>
		/// <param name="e">The exception to handle</param>
		/// <returns>returns true if the exception is handled, false if it should be thrown</returns>
		private async Task<bool> HandleException(ErrorResponseException e)
		{
			if (e.StatusCode == HttpStatusCode.InternalServerError)
			{
				var content = new StreamReader(await e.Response.GetResponseStreamWithHttpDecompression());
				var json = RavenJObject.Load(new JsonTextReader(content));
				var error = json.Deserialize<ServerRequestError>(convention);

				throw new ErrorResponseException(e.Response, error.Error);
			}
			return false;
		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery)
		{
			if (suggestionQuery == null)
				throw new ArgumentNullException("suggestionQuery");

			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&popularity={4}",
					Uri.EscapeUriString(index),
					Uri.EscapeDataString(suggestionQuery.Term),
					Uri.EscapeDataString(suggestionQuery.Field),
					Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToInvariantString()),
															  suggestionQuery.Popularity);

				if (suggestionQuery.Accuracy.HasValue)
					requestUri += "&accuracy=" + suggestionQuery.Accuracy.Value.ToInvariantString();

				if (suggestionQuery.Distance.HasValue)
					requestUri += "&distance=" + suggestionQuery.Distance;

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				var json = (RavenJObject)await request.ReadResponseJsonAsync();
				return new SuggestionQueryResult
				{
					Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
				};
			});
		}

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <returns></returns>
		public Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas)
		{
			return ExecuteWithReplication("POST", async operationMetadata =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var req =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/bulk_docs", "POST",
																							 metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				req.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
												HandleReplicationStatusChanges);

				var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
				var data = jArray.ToString(Formatting.None);

				ErrorResponseException responseException;
				try
				{
					await req.WriteAsync(data);
					var response = (RavenJArray)await req.ReadResponseJsonAsync();
					return convention.CreateSerializer().Deserialize<BatchResult[]>(new RavenJTokenReader(response));
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode != HttpStatusCode.Conflict)
						throw;
					responseException = e;
				}
				throw await FetchConcurrencyException(responseException);
			});
		}

		private static async Task<ConcurrencyException> FetchConcurrencyException(ErrorResponseException e)
		{
			using (var sr = new StreamReader(await e.Response.GetResponseStreamWithHttpDecompression()))
			{
				var text = sr.ReadToEnd();
				var errorResults = JsonConvert.DeserializeAnonymousType(text, new
				{
					url = (string)null,
					actualETag = Etag.Empty,
					expectedETag = Etag.Empty,
					error = (string)null
				});
				return new ConcurrencyException(errorResults.error)
				{
					ActualETag = errorResults.actualETag,
					ExpectedETag = errorResults.expectedETag
				};
			}
		}

		private static void AddTransactionInformation(RavenJObject metadata)
		{
#if !SILVERLIGHT && !NETFX_CORE
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier,
										  TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
#endif
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		/// <summary>
		/// Begins retrieving the statistics for the database
		/// </summary>
		/// <returns></returns>
		public async Task<DatabaseStatistics> GetStatisticsAsync()
		{
			var json = (RavenJObject)await url.Stats()
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
											   .ReadResponseJsonAsync();

			return json.Deserialize<DatabaseStatistics>(convention);
		}

		/// <summary>
		/// Gets the list of databases from the server asynchronously
		/// </summary>
		public async Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0)
		{
			var result = await url.Databases(pageSize, start)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
								  .ReadResponseJsonAsync();
			var json = (RavenJArray)result;
			return json.Select(x => x.ToString())
				.ToArray();
		}

        public Task<AttachmentInformation[]> GetAttachmentsAsync(Etag startEtag, int pageSize)
	    {
	        return ExecuteWithReplication("GET", async operationMetadata =>
	        {
                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (operationMetadata.Url + "/static/?pageSize=" + pageSize + "&etag=" + startEtag).NoCache(), "GET", credentials, convention)
	                .AddOperationHeaders(OperationsHeaders));

                request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                var json = (RavenJArray)await request.ReadResponseJsonAsync();
                return convention.CreateSerializer().Deserialize<AttachmentInformation[]>(new RavenJTokenReader(json));
	        });
	    }

	    /// <summary>
		/// Puts the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data stream.</param>
		/// <param name="metadata">The metadata.</param>
		public Task PutAttachmentAsync(string key, Etag etag, Stream data, RavenJObject metadata)
		{
			return ExecuteWithReplication("PUT", operationMetadata =>
			{
				if (metadata == null)
					metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue((string)etag);

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationMetadata.Url, key), "PUT",
																							 metadata, credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				return request.ExecuteWriteAsync(data);
			});
		}

		/// <summary>
		/// Gets the attachment by the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<Attachment> GetAttachmentAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return ExecuteWithReplication("GET", operationMetadata => DirectGetAttachmentAsync(key, operationMetadata, "GET"));
		}

		public Task<Attachment> HeadAttachmentAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return ExecuteWithReplication("HEAD", operationMetadata => DirectGetAttachmentAsync(key, operationMetadata, "HEAD"));
		}

		private async Task<Attachment> DirectGetAttachmentAsync(string key, OperationMetadata operationMetadata, string method)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																						 (operationMetadata.Url + "/static/" +
																						  key).NoCache(), method, metadata,
																						 operationMetadata.Credentials,
																						 convention)
															 .AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
												HandleReplicationStatusChanges);

			ErrorResponseException responseException;
			try
			{
				var result = await request.ReadResponseBytesAsync();
				var memoryStream = new MemoryStream(result);
				return new Attachment
				{
					Key = key,
					Data = () => memoryStream,
					Size = result.Length,
					Etag = request.Response.GetEtagHeader(),
					Metadata = request.ResponseHeaders.FilterHeadersAttachment()
				};
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode == HttpStatusCode.NotFound)
					return null;
				if (e.StatusCode != HttpStatusCode.Conflict)
					throw;
				responseException = e;
			}
			var conflictsDoc =
				RavenJObject.Load(new BsonReader(await responseException.Response.GetResponseStreamWithHttpDecompression()));
			var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

			throw new ConflictException(
				"Conflict detected on " + key + ", conflict must be resolved before the attachment will be accessible", true)
			{
				ConflictedVersionIds = conflictIds,
				Etag = responseException.Response.GetEtagHeader()
			};
		}


		/// <summary>
		/// Deletes the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public Task DeleteAttachmentAsync(string key, Etag etag)
		{
			return ExecuteWithReplication("DELETE", operationMetadata =>
			{
				var metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue((string)etag);

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationMetadata.Url, key), "DELETE",
																							 metadata, credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
													HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public static string Static(string url, string key)
		{
			return url + "/static/" + Uri.EscapeUriString(key);
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			return jsonRequestFactory.DisableAllCaching();
		}

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
		{
			return ExecuteWithReplication("GET", async operationMetadata =>
			{
				var result = await operationMetadata.Url.Terms(index, field, fromValue, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
											   .AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer,
																			convention.FailoverBehavior,
																			HandleReplicationStatusChanges)
											   .ReadResponseJsonAsync();
				var json = ((RavenJArray)result);
				return json.Select(x => x.Value<string>()).ToArray();
			});
		}

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return profilingInformation; }
		}

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { replicationInformer.FailoverStatusChanged += value; }
			remove { replicationInformer.FailoverStatusChanged -= value; }
		}

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public IDisposable ForceReadFromMaster()
		{
			var old = readStripingBase;
			readStripingBase = -1;// this means that will have to use the master url first
			return new DisposableAction(() => readStripingBase = old);
		}

		public Task<JsonDocumentMetadata> HeadAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");
			return ExecuteWithReplication("HEAD", u => DirectHeadAsync(u, key));
		}

#if NETFX_CORE
		//TODO: Mono implement 
		public Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo)
		{
			throw new NotImplementedException();
		}

		 public Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0,
                                                                        int pageSize = Int32.MaxValue)
                {
                        throw new NotImplementedException();
                }

#else
		public async Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			string path = query.GetIndexQueryUrl(url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false);
			var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
																			.AddReplicationStatusHeaders(Url, url, replicationInformer,
																																	 convention.FailoverBehavior,
																																	 HandleReplicationStatusChanges);

			var webResponse = await request.RawExecuteRequestAsync();
			queryHeaderInfo.Value = new QueryHeaderInformation
			{
				Index = webResponse.Headers["Raven-Index"],
				IndexTimestamp = DateTime.ParseExact(webResponse.Headers["Raven-Index-Timestamp"], Default.DateTimeFormatsToRead,
																CultureInfo.InvariantCulture, DateTimeStyles.None),
				IndexEtag = Etag.Parse(webResponse.Headers["Raven-Index-Etag"]),
				ResultEtag = Etag.Parse(webResponse.Headers["Raven-Result-Etag"]),
				IsStable = bool.Parse(webResponse.Headers["Raven-Is-Stale"]),
				TotalResults = int.Parse(webResponse.Headers["Raven-Total-Results"])
			};

			return new YieldStreamResults(webResponse);
		}

		public class YieldStreamResults : IAsyncEnumerator<RavenJObject>
		{
			private readonly WebResponse webResponse;
			private readonly Stream stream;
			private readonly StreamReader streamReader;
			private readonly JsonTextReaderAsync reader;
			private bool complete;

			private bool wasInitalized;

			public YieldStreamResults(WebResponse webResponse)
			{
				this.webResponse = webResponse;
				stream = webResponse.GetResponseStreamWithHttpDecompression();
				streamReader = new StreamReader(stream);
				reader = new JsonTextReaderAsync(streamReader);

			}

			private async Task InitAsync()
			{
				if (await reader.ReadAsync() == false || reader.TokenType != JsonToken.StartObject)
					throw new InvalidOperationException("Unexpected data at start of stream");

				if (await reader.ReadAsync() == false || reader.TokenType != JsonToken.PropertyName ||
					Equals("Results", reader.Value) == false)
					throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

				if (await reader.ReadAsync() == false || reader.TokenType != JsonToken.StartArray)
					throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");
			}

			public void Dispose()
			{
				reader.Close();
				streamReader.Close();
				stream.Close();
				webResponse.Close();
			}

			public async Task<bool> MoveNextAsync()
			{
				if (complete)
				{
					// to parallel IEnumerable<T>, subsequent calls to MoveNextAsync after it has returned false should
					// also return false, rather than throwing
					return false;
				}

				if (wasInitalized == false)
				{
					await InitAsync();
					wasInitalized = true;
				}

				if (await reader.ReadAsync() == false)
					throw new InvalidOperationException("Unexpected end of data");

				if (reader.TokenType == JsonToken.EndArray)
				{
					complete = true;
					return false;
				}

				Current = (RavenJObject)await RavenJToken.ReadFromAsync(reader);
				return true;
			}

			public RavenJObject Current { get; private set; }
		}

		public async Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(
						Etag fromEtag = null, string startsWith = null,
						string matches = null, int start = 0,
						int pageSize = Int32.MaxValue,
						string exclude = null)
		{
			if (fromEtag != null && startsWith != null)
				throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

			var sb = new StringBuilder(url).Append("/streams/docs?");

			if (fromEtag != null)
			{
				sb.Append("etag=")
						.Append(fromEtag)
						.Append("&");
			}
			else
			{
				if (startsWith != null)
				{
					sb.Append("startsWith=").Append(Uri.EscapeDataString(startsWith)).Append("&");
				}
				if (matches != null)
				{
					sb.Append("matches=").Append(Uri.EscapeDataString(matches)).Append("&");
				}
				if (exclude != null)
				{
					sb.Append("exclude=").Append(Uri.EscapeDataString(exclude)).Append("&");
				}
			}
			if (start != 0)
				sb.Append("start=").Append(start).Append("&");
			if (pageSize != int.MaxValue)
				sb.Append("pageSize=").Append(pageSize).Append("&");


			var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, sb.ToString().NoCache(), "GET", credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
																			.AddReplicationStatusHeaders(Url, url, replicationInformer,
																																	 convention.FailoverBehavior,
																																	 HandleReplicationStatusChanges);
			var webResponse = await request.RawExecuteRequestAsync();
			return new YieldStreamResults(webResponse);
		}

		public Task DeleteAsync(string key, Etag etag)
		{
			EnsureIsNotNullOrEmpty(key, "key");
			return ExecuteWithReplication("DELETE", operationMetadata => operationMetadata.Url.Doc(key)
			 .ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
			 .ExecuteRequestAsync());
		}

		private async Task DirectDeleteAsync(string key, Etag etag, string operationUrl)
		{
			var metadata = new RavenJObject();
			if (etag != null)
				metadata.Add("ETag", etag.ToString());
			AddTransactionInformation(metadata);
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationUrl + "/docs/" + key, "DELETE", metadata, credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
							.AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			try
			{
				await httpJsonRequest.ExecuteRequestAsync();
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode != HttpStatusCode.NotFound)
					throw;
			}
		}

		public string UrlFor(string documentKey)
		{
			return url + "/docs/" + documentKey;
		}

#endif
#if SILVERLIGHT
		/// <summary>
		/// Get the low level  bulk insert operation
		/// </summary>
		public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)
		{
			return new RemoteBulkInsertOperation(options, this, changes);
		}
#endif


		/// <summary>
		/// Do a direct HEAD request against the server for the specified document
		/// </summary>
		private async Task<JsonDocumentMetadata> DirectHeadAsync(OperationMetadata operationMetadata, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			HttpJsonRequest request = jsonRequestFactory.CreateHttpJsonRequest(
																			   new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, "HEAD", credentials, convention)
																				   .AddOperationHeaders(OperationsHeaders))
														.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer,
																					 convention.FailoverBehavior,
																					 HandleReplicationStatusChanges);

			try
			{
				await request.ReadResponseJsonAsync();
				return SerializationHelper.DeserializeJsonDocumentMetadata(key, request.ResponseHeaders,
																		   request.Response.StatusCode);
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode == HttpStatusCode.NotFound)
					return null;
				if (e.StatusCode == HttpStatusCode.Conflict)
				{
					throw new ConflictException("Conflict detected on " + key +
										", conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict",
										true)
					{
						Etag = e.Response.GetEtagHeader()
					};
				}
				throw;
			}
		}

        public Task<RavenJToken> ExecuteGetRequest(string requestUrl)
        {
            EnsureIsNotNullOrEmpty(requestUrl, "url");
            return ExecuteWithReplication("GET", serverUrl =>
            {
                var metadata = new RavenJObject();
                AddTransactionInformation(metadata);
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                        new CreateHttpJsonRequestParams(this, serverUrl + requestUrl, "GET", metadata, credentials, convention)
                                .AddOperationHeaders(OperationsHeaders));

                return request.ReadResponseJsonAsync();
            });
        }

		public HttpJsonRequest CreateRequest(string requestUrl, string method, bool disableRequestCompression = false)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var createHttpJsonRequestParams =
				new CreateHttpJsonRequestParams(this, url + requestUrl, method, metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders);
			createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
			return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
		}

		public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, string method, bool disableRequestCompression = false)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);

			var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, (currentServerUrl + requestUrl).NoCache(), method, credentials,
																			  convention).AddOperationHeaders(OperationsHeaders);
			createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;

			return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams)
									 .AddReplicationStatusHeaders(url, currentServerUrl, replicationInformer,
																  convention.FailoverBehavior, HandleReplicationStatusChanges);
		}

		public Task UpdateAttachmentMetadataAsync(string key, Etag etag, RavenJObject metadata)
		{
			return ExecuteWithReplication("POST", operationMetadata => DirectUpdateAttachmentMetadata(key, metadata, etag, operationMetadata.Url));
		}

		private async Task DirectUpdateAttachmentMetadata(string key, RavenJObject metadata, Etag etag, string operationUrl)
		{
			if (etag != null)
			{
				metadata["ETag"] = etag.ToString();
			}
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, "POST", metadata, credentials, convention))
							.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			ErrorResponseException responseException;
			try
			{
				await webRequest.ExecuteRequestAsync();
				return;
			}
			catch (ErrorResponseException e)
			{
				responseException = e;
			}
			if (!await HandleException(responseException))
				throw responseException;
		}

		public Task<IAsyncEnumerator<Attachment>> GetAttachmentHeadersStartingWithAsync(string idPrefix, int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata => DirectGetAttachmentHeadersStartingWith("GET", idPrefix, start, pageSize, operationMetadata.Url));
		}

		private async Task<IAsyncEnumerator<Attachment>> DirectGetAttachmentHeadersStartingWith(string method, string idPrefix, int start, int pageSize, string operationUrl)
		{
			var webRequest =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																																									 operationUrl + "/static/?startsWith=" +
																																									 idPrefix + "&start=" + start + "&pageSize=" +
																																									 pageSize, method, credentials, convention))
																																									 .AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			RavenJToken result = await webRequest.ReadResponseJsonAsync();

			List<Attachment> attachments =
					convention.CreateSerializer().Deserialize<Attachment[]>(new RavenJTokenReader(result))
							.Select(x => new Attachment
							{
								Etag = x.Etag,
								Metadata = x.Metadata,
								Size = x.Size,
								Key = x.Key,
								Data =
										() => { throw new InvalidOperationException("Cannot get attachment data from an attachment header"); }
							}).ToList();
			return new AsyncEnumeratorBridge<Attachment>(attachments.GetEnumerator());

		}

		public Task CommitAsync(string txId)
		{
			return ExecuteWithReplication("POST", operationMetadata => DirectCommit(txId, operationMetadata.Url));
		}

		private Task DirectCommit(string txId, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/commit?tx=" + txId, "POST",
							credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior,
							HandleReplicationStatusChanges);

			return httpJsonRequest.ReadResponseJsonAsync();
		}

		public Task RollbackAsync(string txId)
		{
			return ExecuteWithReplication("POST", operationMetadata => DirectRollback(txId, operationMetadata.Url));
		}

		private Task DirectRollback(string txId, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/rollback?tx=" + txId, "POST", credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
							.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return httpJsonRequest.ReadResponseJsonAsync();
		}

		public Task PrepareTransactionAsync(string txId)
		{
			return ExecuteWithReplication("POST", operationMetadata => DirectPrepareTransaction(txId, operationMetadata.Url));
		}

		private Task DirectPrepareTransaction(string txId, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/prepare?tx=" + txId, "POST", credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
							.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return httpJsonRequest.ReadResponseJsonAsync();
		}

		private async Task<Attachment> DirectGetAttachment(string method, string key, string operationUrl)
		{
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, method, credentials, convention))
											.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
			try
			{
				int len;
				Func<Stream> data;
				if (method == "GET")
				{
					var memoryStream = new MemoryStream(await webRequest.ReadResponseBytesAsync());
					data = () => memoryStream;
					len = (int)memoryStream.Length;
				}
				else
				{
					await webRequest.ExecuteRequestAsync();

					len = (int)webRequest.Response.Content.Headers.ContentLength;
					data = () =>
					{
						throw new InvalidOperationException("Cannot get attachment data because it was loaded using: " +
																								method);
					};
				}

				HandleReplicationStatusChanges(webRequest.ResponseHeaders[Constants.RavenForcePrimaryServerCheck], url,
						operationUrl);

				return new Attachment
				{
					Data = data,
					Size = len,
					Etag = webRequest.GetEtagHeader(),
					Metadata = webRequest.ResponseHeaders.FilterHeadersAttachment()
				};
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode == HttpStatusCode.Conflict)
				{
					var conflictsDoc = RavenJObject.Load(new BsonReader(e.Response.GetResponseStreamWithHttpDecompression().Result)); //TODO can't await in a catch block
					var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

					throw new ConflictException("Conflict detected on " + key +
																			", conflict must be resolved before the attachment will be accessible", true)
					{
						ConflictedVersionIds = conflictIds,
						Etag = e.Response.GetEtagHeader()
					};
				}
				if (e.StatusCode == HttpStatusCode.NotFound)
					return null;
				throw;
			}
		}



		private void HandleReplicationStatusChanges(string forceCheck, string primaryUrl, string currentUrl)
		{
			if (primaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
				return;

			bool shouldForceCheck;
			if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
			{
				replicationInformer.ForceCheck(primaryUrl, shouldForceCheck);
			}
		}

		internal Task ExecuteWithReplication(string method, Func<OperationMetadata, Task> operation)
		{
			// Convert the Func<string, Task> to a Func<string, Task<object>>
			return ExecuteWithReplication(method, u => operation(u).ContinueWith<object>(t =>
			{
				t.AssertNotFailed();
				return null;
			}));
		}

		private volatile bool currentlyExecuting;
		private bool resolvingConflict;
		private bool resolvingConflictRetries;

		internal async Task<T> ExecuteWithReplication<T>(string method, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && convention.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

			currentlyExecuting = true;
			try
			{
				return
					await replicationInformer.ExecuteWithReplicationAsync(method, url, credentials, currentRequest, readStripingBase, operation);
			}
			finally
			{
				currentlyExecuting = false;
			}
		}

		private async Task<bool> AssertNonConflictedDocumentAndCheckIfNeedToReload(OperationMetadata operationMetadata, RavenJObject docResult)
		{
			if (docResult == null)
				return (false);
			var metadata = docResult[Constants.Metadata];
			if (metadata == null)
				return (false);

			if (metadata.Value<int>("@Http-Status-Code") == 409)
			{
				var etag = HttpExtensions.EtagHeaderToEtag(metadata.Value<string>("@etag"));
				var e = await TryResolveConflictOrCreateConcurrencyException(operationMetadata, metadata.Value<string>("@id"), docResult, etag);
				if (e != null)
					throw e;
				return true;

			}
			return (false);
		}

		private async Task<ConflictException> TryResolveConflictOrCreateConcurrencyException(OperationMetadata operationMetadata, string key,
																							 RavenJObject conflictsDoc,
																							 Etag etag)
		{
			var ravenJArray = conflictsDoc.Value<RavenJArray>("Conflicts");
			if (ravenJArray == null)
				throw new InvalidOperationException(
					"Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");

			var conflictIds = ravenJArray.Select(x => x.Value<string>()).ToArray();

			var result = await TryResolveConflictByUsingRegisteredListenersAsync(key, etag, conflictIds, operationMetadata);
			if (result)
				return null;

			return
				new ConflictException(
					"Conflict detected on " + key + ", conflict must be resolved before the document will be accessible",
												 true)
				{
					ConflictedVersionIds = conflictIds,
					Etag = etag
				};
		}

		internal async Task<bool> TryResolveConflictByUsingRegisteredListenersAsync(string key, Etag etag, string[] conflictIds, OperationMetadata operationMetadata = null)
		{
			if (operationMetadata == null)
				operationMetadata = new OperationMetadata(Url);

			if (conflictListeners.Length > 0 && resolvingConflict == false)
			{
				resolvingConflict = true;
				try
				{
					var result = await DirectGetAsync(operationMetadata, conflictIds, null, null, null, false);
					var results = result.Results.Select(SerializationHelper.ToJsonDocument).ToArray();

					foreach (var conflictListener in conflictListeners)
					{
						JsonDocument resolvedDocument;
						if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
						{
							await DirectPutAsync(operationMetadata, key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata);
							return true;
						}
					}

					return false;
				}
				finally
				{
					resolvingConflict = false;
				}
			}

			return false;
		}

		private async Task<T> RetryOperationBecauseOfConflict<T>(OperationMetadata operationMetadata, IEnumerable<RavenJObject> docResults,
																 T currentResult, Func<Task<T>> nextTry)
		{
			bool requiresRetry = false;
			foreach (var docResult in docResults)
			{
				requiresRetry |= await AssertNonConflictedDocumentAndCheckIfNeedToReload(operationMetadata, docResult);
			}
			if (!requiresRetry)
				return currentResult;

			if (resolvingConflictRetries)
				throw new InvalidOperationException(
					"Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");
			resolvingConflictRetries = true;
			try
			{
				return await nextTry();
			}
			finally
			{
				resolvingConflictRetries = false;
			}
		}

		public async Task<RavenJToken> GetOperationStatusAsync(long id)
		{
			var request = jsonRequestFactory
				.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/operation/status?id=" + id).NoCache(), "GET",
																	   credentials, convention)
				.AddOperationHeaders(OperationsHeaders));

			try
			{
				return await request.ReadResponseJsonAsync();
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode == HttpStatusCode.NotFound)
					return null;
				throw;
			}
		}


		#region IAsyncGlobalAdminDatabaseCommands

		public IAsyncGlobalAdminDatabaseCommands GlobalAdmin
		{
            get { return (IAsyncGlobalAdminDatabaseCommands)this.ForSystemDatabase(); }
		}

		async Task<AdminStatistics> IAsyncGlobalAdminDatabaseCommands.GetStatisticsAsync()
		{
			var json = (RavenJObject)await rootUrl.AdminStats()
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
												   .ReadResponseJsonAsync();

			return json.Deserialize<AdminStatistics>(convention);
		}

		#endregion

		public async Task<string> GetSingleAuthToken()
		{
			var tokenRequest = CreateRequest("/singleAuthToken", "GET", disableRequestCompression: true);

			var response = await tokenRequest.ReadResponseJsonAsync();
			return response.Value<string>("Token");
		}

		private async Task<string> ValidateThatWeCanUseAuthenticateTokens(string token)
		{
			var request = CreateRequest("/singleAuthToken", "GET", disableRequestCompression: true);

			request.DisableAuthentication();
#if !SILVERLIGHT
			request.webRequest.ContentLength = 0;
#endif
			request.AddOperationHeader("Single-Use-Auth-Token", token);
			var result = await request.ReadResponseJsonAsync();
			return result.Value<string>("Token");
		}

		#region IAsyncAdminDatabaseCommands

		/// <summary>
		/// Admin operations, like create/delete database.
		/// </summary>
		public IAsyncAdminDatabaseCommands Admin
		{
			get { return this; }
		}

		public Task CreateDatabaseAsync(DatabaseDocument databaseDocument)
		{
			if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
				throw new InvalidOperationException("The Raven/DataDir setting is mandatory");

			var dbname = databaseDocument.Id.Replace("Raven/Databases/", "");
			MultiDatabase.AssertValidDatabaseName(dbname);
			var doc = RavenJObject.FromObject(databaseDocument);
			doc.Remove("Id");


			var req = CreateRequest("/admin/databases/" + Uri.EscapeDataString(dbname), "PUT");
			return req.ExecuteWriteAsync(doc.ToString(Formatting.Indented));
		}

		public Task DeleteDatabaseAsync(string databaseName, bool hardDelete = false)
		{
			throw new NotImplementedException();
		}

		public Task CompactDatabaseAsync(string databaseName)
		{
			throw new NotImplementedException();
		}

		#endregion

		#region IAsyncInfoDatabaseCommands

		public IAsyncInfoDatabaseCommands Info
		{
			get { return this; }
		}

		async Task<ReplicationStatistics> IAsyncInfoDatabaseCommands.GetReplicationInfoAsync()
		{
			var json = (RavenJObject)await url.ReplicationInfo()
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
											   .ReadResponseJsonAsync();

			return json.Deserialize<ReplicationStatistics>(convention);
		}

		#endregion



		/// <summary>
		/// Returns a new <see cref="IAsyncDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new AsyncServerClient(url, convention, _apiKey, credentialsForSession, jsonRequestFactory, sessionId,
										 replicationInformerGetter, databaseName, conflictListeners);
		}
	}
}
