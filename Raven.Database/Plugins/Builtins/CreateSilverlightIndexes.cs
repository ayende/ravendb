using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Transformers;

namespace Raven.Database.Plugins.Builtins
{
    public class CreateSilverlightIndexes : ISilverlightRequestedAware
    {
        public void SilverlightWasRequested(DocumentDatabase database)
        {
            var ravenDocumentsByEntityName = new RavenDocumentsByEntityName { };
            database.Indexes.PutIndex(Constants.DocumentsByEntityNameIndex,
                ravenDocumentsByEntityName.CreateIndexDefinition());

            SilverlightTransformerWasRequested(database);
        }

        public void SilverlightTransformerWasRequested(DocumentDatabase database) {
            var ravenLabelsByCollectionName = new RavenLabelsByCollectionNameTransformer { };
            database.Transformers.PutTransform(Constants.LabelsByCollectionNameTransformer,
                ravenLabelsByCollectionName.Transformer);
        }
    }
}