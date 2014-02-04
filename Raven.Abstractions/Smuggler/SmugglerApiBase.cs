#if !NETFX_CORE
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;

#if !SILVERLIGHT
using System.IO.Compression;
#else
using Ionic.Zlib;
#endif

using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Smuggler
{
	public abstract class SmugglerApiBase : ISmugglerApi
	{
		private readonly Stopwatch stopwatch = Stopwatch.StartNew();

        public SmugglerOptions SmugglerOptions { get; set; }

		protected abstract Task<RavenJArray> GetIndexes(int totalCount);
		protected abstract Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag, int limit);
		protected abstract Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag);
		protected abstract Task<RavenJArray> GetTransformers(int start);

		protected abstract Task PutIndex(string indexName, RavenJToken index);
		protected abstract Task PutAttachment(AttachmentExportInfo attachmentExportInfo);
		protected abstract Task PutDocument(RavenJObject document);
		protected abstract Task PutTransformer(string transformerName, RavenJToken transformer);

		protected abstract Task<string> GetVersion();

		protected abstract Task<DatabaseStatistics> GetStats();

		protected abstract Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript);

		protected abstract void ShowProgress(string format, params object[] args);

		protected bool EnsuredDatabaseExists;
	    private const string IncrementalExportStateFile = "IncrementalExport.state.json";

		public virtual async Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions, SmugglerOptions options)
		{
	        SetSmugglerOptions(options);

            var result = new ExportDataResult
            {
				FilePath = exportOptions.ToFile,
                LastAttachmentsEtag = options.StartAttachmentsEtag,
                LastDocsEtag = options.StartDocsEtag
            };

#if !SILVERLIGHT
			if (options.Incremental)
			{
                if (Directory.Exists(result.FilePath) == false)
				{
                    if (File.Exists(result.FilePath))
                        result.FilePath = Path.GetDirectoryName(result.FilePath) ?? result.FilePath;
					else
                        Directory.CreateDirectory(result.FilePath);
				}

				if (options.StartDocsEtag == Etag.Empty && options.StartAttachmentsEtag == Etag.Empty)
				{
					ReadLastEtagsFromFile(result);
				}

				result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(result.FilePath))
				{
					var counter = 1;
					while (true)
					{
// ReSharper disable once AssignNullToNotNullAttribute
						result.FilePath = Path.Combine(Path.GetDirectoryName(result.FilePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + " - " + counter + ".ravendb-incremental-dump");

						if (File.Exists(result.FilePath) == false)
							break;
						counter++;
					}
				}
			}
#else
			if(options.Incremental)
				throw new NotSupportedException("Incremental exports are not supported in SL.");
#endif
			await DetectServerSupportedFeatures();

		    SmugglerExportException lastException = null;

			bool ownedStream = exportOptions.ToStream == null;
			var stream = exportOptions.ToStream ?? File.Create(result.FilePath);
			try
			{
				using (var gZipStream = new GZipStream(stream, CompressionMode.Compress,
#if SILVERLIGHT
                    CompressionLevel.BestCompression,
#endif
				                                       leaveOpen: true))
				using (var streamWriter = new StreamWriter(gZipStream))
				{
					var jsonWriter = new JsonTextWriter(streamWriter)
					{
						Formatting = Formatting.Indented
					};
					jsonWriter.WriteStartObject();
					jsonWriter.WritePropertyName("Indexes");
					jsonWriter.WriteStartArray();
					if (options.OperateOnTypes.HasFlag(ItemType.Indexes))
					{
						await ExportIndexes(jsonWriter);
					}
					jsonWriter.WriteEndArray();

					jsonWriter.WritePropertyName("Docs");
					jsonWriter.WriteStartArray();
					if (options.OperateOnTypes.HasFlag(ItemType.Documents))
					{
					    try
					    {
                            result.LastDocsEtag = await ExportDocuments(options, jsonWriter, result.LastDocsEtag);
                        }
					    catch (SmugglerExportException e)
					    {
					        result.LastDocsEtag = e.LastEtag;
                            e.File = ownedStream ? result.FilePath : null;
					        lastException = e;
					    }
					}
					jsonWriter.WriteEndArray();

					jsonWriter.WritePropertyName("Attachments");
					jsonWriter.WriteStartArray();
					if (options.OperateOnTypes.HasFlag(ItemType.Attachments) && lastException == null)
					{
					    try
					{
						result.LastAttachmentsEtag = await ExportAttachments(jsonWriter, result.LastAttachmentsEtag);
					}
					    catch (SmugglerExportException e)
					    {
					        result.LastAttachmentsEtag = e.LastEtag;
                            e.File = ownedStream ? result.FilePath : null;
					        lastException = e;
					    }
					}
					jsonWriter.WriteEndArray();

					jsonWriter.WritePropertyName("Transformers");
					jsonWriter.WriteStartArray();
					if (options.OperateOnTypes.HasFlag(ItemType.Transformers) && lastException == null)
					{
						await ExportTransformers(jsonWriter);
					}
					jsonWriter.WriteEndArray();

					jsonWriter.WriteEndObject();
					streamWriter.Flush();
				}

#if !SILVERLIGHT
				if (options.Incremental)
					WriteLastEtagsFromFile(result, result.FilePath);
#endif

			    if (lastException != null)
			        throw lastException;

				return result;
			}
			finally
			{
				if (ownedStream && stream != null)
					stream.Dispose();
			}
		}

	    protected void SetSmugglerOptions(SmugglerOptions options)
	    {
            if (options == null)
                throw new ArgumentNullException("options");

	        SmugglerOptions = options;
	    }

        public static void ReadLastEtagsFromFile(ExportDataResult result)
		{
			var log = LogManager.GetCurrentClassLogger();
			var etagFileLocation = Path.Combine(result.FilePath, IncrementalExportStateFile);
			if (!File.Exists(etagFileLocation))
				return;

			using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
			using (var jsonReader = new JsonTextReader(streamReader))
			{
				RavenJObject ravenJObject;
				try
				{
					ravenJObject = RavenJObject.Load(jsonReader);
				}
				catch (Exception e)
				{
					log.WarnException("Could not parse etag document from file : " + etagFileLocation + ", ignoring, will start from scratch", e);
					return;
				}
                result.LastDocsEtag = Etag.Parse(ravenJObject.Value<string>("LastDocEtag"));
                result.LastAttachmentsEtag = Etag.Parse(ravenJObject.Value<string>("LastAttachmentEtag"));
			}
		}

		public static void WriteLastEtagsFromFile(ExportDataResult result, string backupPath)
		{
// ReSharper disable once AssignNullToNotNullAttribute
			var etagFileLocation = Path.Combine(Path.GetDirectoryName(backupPath), IncrementalExportStateFile);
			using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
			{
				new RavenJObject
					{
						{"LastDocEtag", result.LastDocsEtag.ToString()},
						{"LastAttachmentEtag", result.LastAttachmentsEtag.ToString()}
					}.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
			}
		}

		private async Task ExportTransformers(JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				var transformers = await GetTransformers(totalCount);
				if (transformers.Length == 0)
				{
					ShowProgress("Done with reading transformers, total: {0}", totalCount);
					break;
				}

				totalCount += transformers.Length;
				ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Length, totalCount);

				foreach (var transformer in transformers)
				{
					transformer.WriteTo(jsonWriter);
				}
			}
		}

		private async Task<Etag> ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter, Etag lastEtag)
		{
			var totalCount = 0;
			var lastReport = SystemTime.UtcNow;
			var reportInterval = TimeSpan.FromSeconds(2);
			var errorsCount = 0;
			ShowProgress("Exporting Documents");
			
			while (true)
			{
				bool hasDocs = false;
                try {
                    var maxRecords = options.Limit - totalCount;
                    if (maxRecords > 0)
			        {
			            using (var documents = await GetDocuments(lastEtag, maxRecords))
			            {
			                var watch = Stopwatch.StartNew();

			                while (await documents.MoveNextAsync())
			                {
			                    hasDocs = true;
			                    var document = documents.Current;
			                    lastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));

			                    if (!options.MatchFilters(document))
			                        continue;

			                    if (options.ShouldExcludeExpired && options.ExcludeExpired(document))
			                        continue;
			                    document.WriteTo(jsonWriter);
			                    totalCount++;

			                    if (totalCount%1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
			                    {
			                        ShowProgress("Exported {0} documents", totalCount);
			                        lastReport = SystemTime.UtcNow;
			                    }

			                    if (watch.ElapsedMilliseconds > 100)
			                        errorsCount++;

			                    watch.Start();
			                }
			            }
			        
			            if (hasDocs)
			                continue;

			            // The server can filter all the results. In this case, we need to try to go over with the next batch.
			            // Note that if the ETag' server restarts number is not the same, this won't guard against an infinite loop.
                        // (This code provides support for legacy RavenDB version: 1.0)
			            var databaseStatistics = await GetStats();
			            var lastEtagComparable = new ComparableByteArray(lastEtag);
			            if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
			            {
                            lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
			                ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);

			                continue;
			            }
                    }
			    }
                catch (Exception e)
                {
                    ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }

			    ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
				return lastEtag;
			}
		}

		public async Task WaitForIndexing(SmugglerOptions options)
		{
			var justIndexingWait = Stopwatch.StartNew();
			int tries = 0;
			while (true)
			{
				var databaseStatistics = await GetStats();
				if (databaseStatistics.StaleIndexes.Length != 0)
				{
					if (tries++ % 10 == 0)
					{
						Console.Write("\rWaiting {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
					}

					Thread.Sleep(1000);
					continue;
				}
				Console.WriteLine("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
				break;
			}
		}

		public virtual async Task ImportData(SmugglerImportOptions importOptions, SmugglerOptions options)
		{
            if (options.Incremental == false)
            {
				Stream stream = importOptions.FromStream;
                bool ownStream = false;
                try
                {
                    if (stream == null)
                    {
						stream = File.OpenRead(importOptions.FromFile);
                        ownStream = true;
                    }
					await ImportData(importOptions, options, stream);
                }
                finally
                {
                    if (stream != null && ownStream)
                        stream.Dispose();
                }
				return;
			}

#if SILVERLIGHT
		    throw new NotSupportedException("Silverlight doesn't support importing an incremental dump files.");
#else
			var files = Directory.GetFiles(Path.GetFullPath(importOptions.FromFile))
				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
				.OrderBy(File.GetLastWriteTimeUtc)
				.ToArray();

			if (files.Length == 0)
				return;

		    var optionsWithoutIndexes = new SmugglerOptions
		    {
		        Filters = options.Filters,
		        OperateOnTypes = options.OperateOnTypes & ~(ItemType.Indexes | ItemType.Transformers)
		    };

			for (var i = 0; i < files.Length - 1; i++)
			{
				using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files[i])))
				{
					await ImportData(importOptions, optionsWithoutIndexes, fileStream);
				}
			}

			using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files.Last())))
			{
                await ImportData(importOptions, options, fileStream);
			}
#endif
		}

		protected class AttachmentExportInfo
		{
			public Stream Data { get; set; }
			public RavenJObject Metadata { get; set; }
			public string Key { get; set; }
		}

		protected abstract Task EnsureDatabaseExists();

		public async virtual Task ImportData(SmugglerImportOptions importOptions, SmugglerOptions options, Stream stream)
		{
            SetSmugglerOptions(options);

			await DetectServerSupportedFeatures();

			await EnsureDatabaseExists();
			Stream sizeStream;

			var sw = Stopwatch.StartNew();
			// Try to read the stream compressed, otherwise continue uncompressed.
			JsonTextReader jsonReader;
            try
			{
				sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
				var streamReader = new StreamReader(sizeStream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}
			catch (Exception e)
			{
			    if (e is InvalidDataException == false 
#if SILVERLIGHT
                    && e is ZlibException == false
#endif
                    )
			        throw;

				stream.Seek(0, SeekOrigin.Begin);

                sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));

				var streamReader = new StreamReader(stream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}

			if (jsonReader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");

			ShowProgress("Begin reading indexes");
			var indexCount = await ImportIndexes(jsonReader, options);
			ShowProgress(string.Format("Done with reading indexes, total: {0}", indexCount));

			ShowProgress("Begin reading documents");
			var documentCount = await ImportDocuments(jsonReader, options);
			ShowProgress(string.Format("Done with reading documents, total: {0}", documentCount));

			ShowProgress("Begin reading attachments");
			var attachmentCount = await ImportAttachments(jsonReader, options);
			ShowProgress(string.Format("Done with reading attachments, total: {0}", attachmentCount));

			ShowProgress("Begin reading transformers");
			var transformersCount = await ImportTransformers(jsonReader, options);
			ShowProgress(string.Format("Done with reading transformers, total: {0}", transformersCount));

			sw.Stop();

			ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments in {2:#,#;;0} ms", documentCount, attachmentCount, sw.ElapsedMilliseconds);
		}

		private async Task<int> ImportTransformers(JsonTextReader jsonReader, SmugglerOptions options)
		{
			var count = 0;

			if (jsonReader.Read() == false || jsonReader.TokenType == JsonToken.EndObject)
				return count;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Transformers", jsonReader.Value) == false)
				throw new InvalidDataException("Transformers property was expected");
			if (jsonReader.Read() == false)
				return count;
			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var transformer = RavenJToken.ReadFrom(jsonReader);
				if ((options.OperateOnTypes & ItemType.Transformers) != ItemType.Transformers)
					continue;

				var transformerName = transformer.Value<string>("name");

				await PutTransformer(transformerName, transformer);

				count++;
			}

			await PutTransformer(null, null); // force flush

			return count;
		}

		private async Task<int> ImportAttachments(JsonTextReader jsonReader, SmugglerOptions options)
		{
			var count = 0;

			if (jsonReader.Read() == false || jsonReader.TokenType == JsonToken.EndObject)
				return count;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Attachments", jsonReader.Value) == false)
				throw new InvalidDataException("Attachment property was expected");
			if (jsonReader.Read() == false)
				return count;
			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var item = RavenJToken.ReadFrom(jsonReader);
				if ((options.OperateOnTypes & ItemType.Attachments) != ItemType.Attachments)
					continue;

				var attachmentExportInfo =
					new JsonSerializer
					{
						Converters =
							{
								new JsonToJsonConverter(),
                                new StreamFromJsonConverter()
							}
					}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));

				ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

				await PutAttachment(attachmentExportInfo);

				count++;
			}

			await PutAttachment(null); // force flush

			return count;
		}

        private long GetRoughSize(RavenJToken token)
        {
            long sum; 
            switch (token.Type)
            {
                case JTokenType.None:
                    return 0;
                case JTokenType.Object:
                    sum = 2;// {}
                    foreach (var prop in (RavenJObject)token)
                    {
                        sum += prop.Key.Length + 1; // name:
                        sum += GetRoughSize(prop.Value);
                    }
                    return sum;
                case JTokenType.Array:
                    // the 1 is for ,
                    return 2 + ((RavenJArray) token).Sum(prop => 1 + GetRoughSize(prop));
                case JTokenType.Constructor:
                case JTokenType.Property:
                case JTokenType.Comment:
                case JTokenType.Raw:
                    return 0;
                case JTokenType.Boolean:
                    return token.Value<bool>() ? 4 : 5;
                case JTokenType.Null:
                    return 4;
                case JTokenType.Undefined:
                    return 9;
                case JTokenType.Date:
                    return 21;
                case JTokenType.Bytes:
                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.String:
                case JTokenType.Guid:
                case JTokenType.TimeSpan:
                case JTokenType.Uri:
                    return token.Value<string>().Length;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

		private async Task<int> ImportDocuments(JsonTextReader jsonReader, SmugglerOptions options)
		{
			var count = 0;

			if (jsonReader.Read() == false)
				return count;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Docs", jsonReader.Value) == false)
				throw new InvalidDataException("Docs property was expected");
			if (jsonReader.Read() == false)
				return count;
			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
			    var size = GetRoughSize(document);
				if (size > 1024 * 1024)
				{
					Console.WriteLine("Large document warning: {0:#,#.##;;0} kb - {1}",
									  (double)size / 1024,
									  document["@metadata"].Value<string>("@id"));
				}
				if ((options.OperateOnTypes & ItemType.Documents) != ItemType.Documents)
					continue;
				if (options.MatchFilters(document) == false)
					continue;

				if (!string.IsNullOrEmpty(options.TransformScript))
					document = await TransformDocument(document, options.TransformScript);

				if (document == null)
					continue;

				await PutDocument(document);

				count++;

				if (count % options.BatchSize == 0)
				{
					ShowProgress("Read {0} documents", count);
				}
			}

			await PutDocument(null); // force flush

			return count;
		}

		private async Task<int> ImportIndexes(JsonReader jsonReader, SmugglerOptions options)
		{
			var count = 0;

			if (jsonReader.Read() == false)
				return count;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Indexes", jsonReader.Value) == false)
				throw new InvalidDataException("Indexes property was expected");
			if (jsonReader.Read() == false)
				return count;
			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var index = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
				if ((options.OperateOnTypes & ItemType.Indexes) != ItemType.Indexes)
					continue;

				var indexName = index.Value<string>("name");
				if (indexName.StartsWith("Temp/"))
					continue;
				if (index.Value<RavenJObject>("definition").Value<bool>("IsCompiled"))
					continue; // can't import compiled indexes

			    if ((options.OperateOnTypes & ItemType.RemoveAnalyzers) == ItemType.RemoveAnalyzers)
			    {
			        index.Value<RavenJObject>("definition").Remove("Analyzers");
			    }

				await PutIndex(indexName, index);

				count++;
			}

			await PutIndex(null, null);

			return count;
		}

		protected async Task ExportIndexes(JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				var indexes = await GetIndexes(totalCount);
				if (indexes.Length == 0)
				{
					ShowProgress("Done with reading indexes, total: {0}", totalCount);
					break;
				}
				totalCount += indexes.Length;
				ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
				foreach (var index in indexes)
				{
					index.WriteTo(jsonWriter);
				}
			}
		}

		private async Task DetectServerSupportedFeatures()
		{
#if !SILVERLIGHT
			var serverVersion = await GetVersion();
			if (string.IsNullOrEmpty(serverVersion))
			{
				IsTransformersSupported = false;
				IsDocsStreamingSupported = false;
				ShowProgress("Server version is not available. Running in legacy mode which does not support transformers.");
				return;
			}

            var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<SmugglerApiBase>()).ProductVersion;

			var subServerVersion = serverVersion.Substring(0, 3);
			var subSmugglerVersion = smugglerVersion.Substring(0, 3);

			var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));

			if (intServerVersion < 25)
			{
				IsTransformersSupported = false;
				IsDocsStreamingSupported = false;
				ShowProgress("Running in legacy mode, importing/exporting transformers is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
				return;
			}
#endif

			IsTransformersSupported = true;
			IsDocsStreamingSupported = true;
		}

		public bool IsTransformersSupported { get; private set; }
		public bool IsDocsStreamingSupported { get; private set; }
	}

    public class ExportDataResult
    {
        public ExportDataResult()
        {
            LastDocsEtag = Etag.Empty;
            LastAttachmentsEtag = Etag.Empty;
        }

        public string FilePath { get; set; }
        public Etag LastDocsEtag { get; set; }

        public Etag LastAttachmentsEtag { get; set; }
    }
}
#endif