using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.ScriptedTriggers
{
    public class ScriptedPutTrigger : AbstractPutTrigger
    {

        public const string ConfigurationPrefix = "Raven/ScriptedTrigger/";
        public const string DefaultConfigurationId = "Raven/ScriptedTrigger/DefaultConfiguration";

        public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/"))
                return;

            var configuration = GetConfiguration(metadata, transactionInformation);

            if (configuration == null)
                return;

            var scriptProperty = configuration.DataAsJson["Script"];
            if (scriptProperty == null)
                return;

            var script = scriptProperty.Value<string>();

            if (string.IsNullOrWhiteSpace(script))
                return;
            
            var patcher = new ScriptedJsonPatcher(documentId => Database.Get(documentId, transactionInformation).DataAsJson);

            var newDocument = patcher.Apply(document, new ScriptedPatchRequest()
                                                          {
                                                              Script = script,
                                                              Values = {{"metadata", metadata}, {"key", key}}
                                                          });
            Copy(newDocument, document);
        }

        private JsonDocument GetConfiguration(RavenJObject metadata, TransactionInformation transactionInformation)
        {
            var entityName = metadata.Value<string>(Constants.RavenEntityName);

            return Database.Get(ConfigurationPrefix + entityName, transactionInformation) ??
                   Database.Get(DefaultConfigurationId, transactionInformation);
        }

        private void Copy(RavenJObject source, RavenJObject destination)
        {
            foreach (var key in destination.Keys)
                destination.Remove(key);

            foreach (var item in source)
                destination[item.Key] = item.Value;
        }
    }
}
