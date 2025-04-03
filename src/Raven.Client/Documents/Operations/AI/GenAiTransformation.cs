using System.Text.RegularExpressions;

namespace Raven.Client.Documents.Operations.AI;

public class GenAiTransformation
{
    public string Script { get; set; }

    public bool ValidateScript(out string error)
    {
        error = string.Empty;
        if (Script.Contains("context")) 
            
            return true;
        error = "You must call the context(ctx, hash) function in your script";
        return false;

    }
}
