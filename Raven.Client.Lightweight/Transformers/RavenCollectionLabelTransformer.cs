using System;
using Raven.Abstractions.Indexing;


namespace Raven.Client.Transformers {
    ///<summary>
    /// Create a transformer that retrieves collection label metadata.
    ///</summary>
    public class RavenLabelsByCollectionNameTransformer {
        public TransformerDefinition Transformer {
            get {
                return new TransformerDefinition {
                    Name = Raven.Abstractions.Data.Constants.LabelsByCollectionNameTransformer,
                    TransformResults = @"from result in results select new { Label = result[""@metadata""][""Raven-Entity-Label""] }"
                };
            }
        }
    }
}