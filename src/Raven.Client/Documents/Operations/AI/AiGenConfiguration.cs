using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class AiGenConfiguration : EtlConfiguration<AiConnectionString>
{

    [JsonDeserializationIgnore]
    [JsonIgnore]
    public AiConnectorType AiConnectorType => Connection?.GetActiveProvider() ?? AiConnectorType.None;

    public override string GetDestination() => Name;
    public override string GetDefaultTaskName() => Name;
    
    public override EtlType EtlType => EtlType.AiGen;
    public override bool UsingEncryptedCommunicationChannel() => Connection?.UsingEncryptedCommunicationChannel() ?? false;

    public string GenerateIdentifier() => EmbeddingsGenerationConfiguration.GenerateIdentifier(Name);
}
