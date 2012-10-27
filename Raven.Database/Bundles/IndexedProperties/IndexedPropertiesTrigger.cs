using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Util;
using Raven.Json.Linq;
using Document = Lucene.Net.Documents.Document;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.IndexedProperties
{
	[InheritedExport]
	[ExportMetadata("Bundle", "IndexedProperties")]
	public class IndexedPropertiesTrigger : AbstractIndexUpdateTrigger
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
		{
			//Only apply the trigger if there is a setup doc for this particular index
			var jsonSetupDoc = Database.Get("Raven/IndexedProperties/" + indexName, null);
			if (jsonSetupDoc == null)
				return null;
			var abstractViewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(indexName);
			var setupDoc = jsonSetupDoc.DataAsJson.JsonDeserialization<IndexedPropertiesSetupDoc>();
			return new IndexPropertyBatcher(Database, setupDoc, indexName, abstractViewGenerator);
		}

		public class IndexPropertyBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly DocumentDatabase database;
			private readonly IndexedPropertiesSetupDoc setupDoc;
			private readonly string index;
			private readonly AbstractViewGenerator viewGenerator;
			private readonly ConcurrentSet<string> itemsToRemove = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

			public IndexPropertyBatcher(DocumentDatabase database, IndexedPropertiesSetupDoc setupDoc, string index, AbstractViewGenerator viewGenerator)
			{
				this.database = database;
				this.setupDoc = setupDoc;
				this.index = index;
				this.viewGenerator = viewGenerator;
			}

            public override void OnIndexEntryDeleted(string entryKey)
            {
                if (entryKey == null)
                    return;

                if (!entryKey.TrimStart().StartsWith("{"))
                {
                    // Just a document id
                    log.Debug("Queueing {0} for removal of indexed properties", entryKey);
                    itemsToRemove.Add(entryKey);
                    return;
                }

                RavenJObject entry;
                try
                {
                    entry = RavenJObject.Parse(entryKey);
                }
                catch (Exception e)
                {
                    log.WarnException("Could not properly parse entry key for index: " + index, e);
                    return;
                }
                var documentId = entry.Value<string>(setupDoc.DocumentKey);
                if (documentId == null)
                {
                    log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey,
                             entryKey, index);
                    return;
                }

                log.Debug("Queueing {0} for removal of indexed properties", documentId);
                itemsToRemove.Add(documentId);
            }

		    public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				var resultDocId = document.GetField(setupDoc.DocumentKey);
				if (resultDocId == null)
				{
					log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey, entryKey, index);
					return;
				}

				var documentId = resultDocId.StringValue;

				if (itemsToRemove.TryRemove(documentId))
                    log.Debug("Removing {0} from queue for indexed property removal.", documentId);

				var resultDoc = database.Get(documentId, null);
				if (resultDoc == null)
				{
					log.Warn("Could not find a document with the id '{0}' for index '{1}'", documentId, index);
					return;
				}

				var entityName = resultDoc.Metadata.Value<string>(Constants.RavenEntityName);
				if(entityName != null && viewGenerator.ForEntityNames.Contains(entityName))
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes documents with entity name of '{2}'",
						documentId, index, entityName);
					return;
				}
				if(viewGenerator.ForEntityNames.Count == 0)
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes all documents",
						documentId, index);
					return;
				}

		        var changed = false;
				foreach (var mapping in setupDoc.FieldNameMappings)
				{
					var field = 
						document.GetFieldable(mapping.Key + "_Range") ??
						document.GetFieldable(mapping.Key);

					if (field == null)
						continue;

				    changed = true;
                    var value = GetValue(document, mapping.Key, field);
				    log.Debug("In {0}, setting {1} = {2}", resultDoc.Key, mapping.Value, value);
				    resultDoc.DataAsJson[mapping.Value] = value;
				}

                if (changed)
                    database.Put(resultDoc.Key, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
			}

            private RavenJToken GetValue(Document document, string fieldName, IFieldable field)
            {
                var isArray = document.GetFieldable(fieldName + "_IsArray") != null;

                if (isArray)
                    return new RavenJArray(GetValues(document, fieldName, field));
                
                return GetSingleValue(document, fieldName, field);
            }

            private RavenJToken GetSingleValue(Document document, string fieldName, IFieldable field)
            {
                var isJson = document.GetFieldable(fieldName + "_ConvertToJson") != null;

                if (isJson)
                    return GetComplexValue(field);

                if (field as NumericField != null)
                    return GetNumericValue((NumericField) field);

                return field.StringValue;
            }

            private RavenJObject GetComplexValue(IFieldable field)
            {
                return RavenJObject.Parse(field.StringValue);
            }

            private RavenJToken GetNumericValue(NumericField field)
            {
                return new RavenJValue(field.NumericValue);
            }

		    private IEnumerable<RavenJToken> GetValues(Document document, string fieldName, IFieldable field)
		    {
		        return document.GetFieldables(field.Name)
		            .Select(f => GetSingleValue(document, fieldName, field));
		    }

		    public override void Dispose()
			{
                log.Debug("Disposing {0}, {1} documents to update", GetType(), itemsToRemove.Count);

				foreach (var documentId in itemsToRemove)
				{
					var resultDoc = database.Get(documentId, null);
					if (resultDoc == null)
					{
						log.Warn("Could not find a document with the id '{0}' for index '{1}", documentId, index);
						return;
					}
					var changesMade = false;
					foreach (var mapping in from mapping in setupDoc.FieldNameMappings
											where resultDoc.DataAsJson.ContainsKey(mapping.Value)
											select mapping)
					{
						resultDoc.DataAsJson.Remove(mapping.Value);
                        log.Debug("Removing {0} from {1}", mapping.Value, resultDoc.Key);
						changesMade = true;
					}
					if (changesMade)
						database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
		
				}

				base.Dispose();
			}
		}
	}
}
