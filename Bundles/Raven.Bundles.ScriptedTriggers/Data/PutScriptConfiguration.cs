using Raven.Abstractions.Data;

namespace Raven.Bundles.ScriptedTriggers.Data
{

    public class PutScriptConfiguration 
    {

        /// <summary>
        /// Raven/ScriptedTrigger/{Raven-Entity-Name} or 
        /// Raven/ScriptedTrigger/DefaultConfiguration
        /// </summary>
        /// <remarks>>
        /// The DefaultConfiguration's script is only applied when an entity-specific 
        /// configuration doesn't exist
        /// </remarks>
        public string Id { get; set; }

        /// <summary>
        /// The script to execute on PUT operations
        /// </summary>
        /// <remarks>>
        /// this is the document being put.
        /// metadata is the document's metadata.
        /// key is the document's id
        /// </remarks>
        public string Script { get; set; }
        
    }

}
