using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Corax.Querying.Planning;

/// <summary>
/// Pretty-print emitted C# via Roslyn's <c>NormalizeWhitespace</c>.
/// The IL emitter writes C# alongside each IL primitive, so the raw output
/// is correct but flat — labels, braces, and goto statements land on whatever
/// indentation the primitive happened to emit. Roslyn re-indents and re-spaces
/// using its standard rules so the inspector shows readable code.
///
/// Any Roslyn parse failure falls back to the raw string so a malformed
/// (but still useful for debugging) emission never blanks out the explain panel.
/// </summary>
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
