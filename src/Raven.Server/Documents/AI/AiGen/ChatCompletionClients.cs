using System;
using Raven.Client.Documents.Operations.AI;


namespace Raven.Server.Documents.AI.AiGen
{
    internal class OpenAiChatCompletionClient : AbstractChatCompletionClient
    {
        public OpenAiChatCompletionClient(GenAiConfiguration configuration) : base(baseUri: new Uri(configuration.Connection.OpenAiSettings.Endpoint),
            model: configuration.Connection.OpenAiSettings.Model, apiKey: configuration.Connection.OpenAiSettings.ApiKey,
            structuredOutputSchema: configuration.JsonSchema)
        {
        }
    }

    internal class OllamaChatCompletionClient : AbstractChatCompletionClient
    {
        public OllamaChatCompletionClient(GenAiConfiguration configuration) : base(baseUri: new Uri(configuration.Connection.OllamaSettings.Uri),
            model: configuration.Connection.OllamaSettings.Model, apiKey: null, structuredOutputSchema: configuration.JsonSchema)
        {
        }

    }
}
