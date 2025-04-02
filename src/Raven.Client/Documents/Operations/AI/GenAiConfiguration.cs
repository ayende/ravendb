using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class GenAiConfiguration : EtlConfiguration<AiConnectionString>
{

    [JsonDeserializationIgnore]
    [JsonIgnore]
    public AiConnectorType AiConnectorType => Connection?.GetActiveProvider() ?? AiConnectorType.None;

    public override string GetDestination() => Name;
    public override string GetDefaultTaskName() => Name;
    
    public override EtlType EtlType => EtlType.AiGen;
    public override bool UsingEncryptedCommunicationChannel() => Connection?.UsingEncryptedCommunicationChannel() ?? false;

    public string GenerateIdentifier() => EmbeddingsGenerationConfiguration.GenerateIdentifier(Name);

    public string Prompt { get; set; }
    
    //TODO: Make this JSON objects? 
    public string JsonSchema { get; set; }
    public string SampleObject { get; set; }
    public string Update { get; set; }

    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        if (validateConnection && Initialized == false)
            throw new InvalidOperationException("AiGen configuration must be initialized");

        errors = [];

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of AiGen configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
            Connection.Validate(errors);

        if (Transforms.Count is 0)
        {
            errors.Add($"At least one transform script must be specified");
        }

        //TODO: probably need better checks here
        foreach (Transformation transform in Transforms)
        {
            if (transform.Collections.Count is 0)
            {
                errors.Add($"At least one collection must be provided");
            }

            if (transform.Script.Contains("context") is false)
            {
                errors.Add($"You must call the context(ctx, hash) function in your script");
            }
        }
        if(string.IsNullOrEmpty(Update))
            errors.Add($"You must provide an update function");
        return errors.Count == 0;
    }
}
