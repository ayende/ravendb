using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Corax.Querying.Planning;

public static class CSharpFormatter
{
    public static string Format(string sourceCode)
    {
        if (string.IsNullOrEmpty(sourceCode))
            return sourceCode;

        try
        {
            var tree = CSharpSyntaxTree.ParseText(sourceCode);
            var root = tree.GetCompilationUnitRoot();
            return root.NormalizeWhitespace().ToFullString();
        }
        catch
        {
            return sourceCode;
        }
    }
}
