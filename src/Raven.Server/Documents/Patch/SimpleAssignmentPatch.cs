using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch;

/// <summary>
/// A patch script made only of assignments of arguments to top level fields:
/// <c>this.Name = args.val_0;</c>, <c>this['Name'] = args.val_0;</c> or <c>this[args.field] = args.value;</c>
/// (the form the client's typed patch API generates). Such a script needs no JavaScript engine: the patched
/// document is the original with those fields replaced, so we can build it outside the transaction merger.
/// Anything else (calls, expressions, nested targets, non primitive values) is left to the script runner.
/// </summary>
internal sealed class SimpleAssignmentPatch
{
    private const int MaxCachedScripts = 1024;
    private static readonly ConcurrentDictionary<string, SimpleAssignmentPatch> Cache = new();
    private static readonly SimpleAssignmentPatch NotSimple = new(Array.Empty<Assignment>());

    private const string Identifier = @"[\p{L}_$][\p{L}\p{Nd}_$]*";
    private static readonly Regex Statement = new(
        @"^\s*this\s*(?:\.\s*(?<field>" + Identifier + @")|\[\s*args\s*\.\s*(?<fieldArg>" + Identifier + @")\s*\]|\[\s*'(?<quoted>[^'\\]*)'\s*\]|\[\s*""(?<dquoted>[^""\\]*)""\s*\])" +
        @"\s*=\s*args\s*\.\s*(?<value>" + Identifier + @")\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly record struct Assignment(string Field, string FieldArgument, string ValueArgument);

    private readonly Assignment[] _assignments;

    private SimpleAssignmentPatch(Assignment[] assignments)
    {
        _assignments = assignments;
    }

    public static SimpleAssignmentPatch TryGet(PatchRequest patch)
    {
        if (patch?.Script == null || patch.Type != PatchRequestType.Patch || patch.HasFunctions)
            return null;

        if (Cache.TryGetValue(patch.Script, out var parsed) == false)
        {
            parsed = Parse(patch.Script) ?? NotSimple;
            if (Cache.Count < MaxCachedScripts)
                Cache.TryAdd(patch.Script, parsed);
        }

        return ReferenceEquals(parsed, NotSimple) ? null : parsed;
    }

    private static SimpleAssignmentPatch Parse(string script)
    {
        var assignments = new List<Assignment>();
        // a ';' inside a quoted field name splits the statement, and the halves then fail to match: we only lose the fast path
        foreach (var statement in script.Split(';'))
        {
            if (string.IsNullOrWhiteSpace(statement))
                continue;

            var match = Statement.Match(statement);
            if (match.Success == false)
                return null;

            string field = match.Groups["field"].Success ? match.Groups["field"].Value :
                match.Groups["quoted"].Success ? match.Groups["quoted"].Value :
                match.Groups["dquoted"].Success ? match.Groups["dquoted"].Value : null;

            assignments.Add(new Assignment(field, match.Groups["fieldArg"].Success ? match.Groups["fieldArg"].Value : null, match.Groups["value"].Value));
        }

        return assignments.Count == 0 ? null : new SimpleAssignmentPatch(assignments.ToArray());
    }

    /// <summary>
    /// Applies the assignments to <paramref name="document"/>. Returns false when the arguments do not fit the
    /// fast path (missing argument, non string field name, metadata field, non primitive value); the caller then
    /// runs the script. <paramref name="modified"/> is null when every assigned field already holds its value.
    /// </summary>
    public bool TryApply(JsonOperationContext context, string id, BlittableJsonReaderObject document, BlittableJsonReaderObject args, out BlittableJsonReaderObject modified)
    {
        modified = null;
        if (args == null)
            return false;

        Dictionary<string, object> values = null;
        foreach (var assignment in _assignments)
        {
            var field = assignment.Field;
            if (field == null)
            {
                if (args.TryGetMember(assignment.FieldArgument, out object fieldValue) == false)
                    return false;

                field = fieldValue switch
                {
                    LazyStringValue lsv => lsv.ToString(),
                    LazyCompressedStringValue lcsv => lcsv.ToString(),
                    _ => null
                };
            }

            // '@metadata' and the other '@' properties carry server semantics, leave them to the script runner
            if (string.IsNullOrEmpty(field) || field[0] == '@')
                return false;

            if (args.TryGetMember(assignment.ValueArgument, out object value) == false)
                return false; // undefined in JavaScript

            switch (value)
            {
                case null:
                case LazyStringValue:
                case long:
                case LazyNumberValue:
                case double:
                case bool:
                    break;
                case LazyCompressedStringValue lcsv:
                    value = lcsv.ToString();
                    break;
                default:
                    return false; // objects and arrays
            }

            values ??= new Dictionary<string, object>(StringComparer.Ordinal);
            values[field] = value; // the last assignment to a field wins, as in JavaScript
        }

        var changed = false;
        foreach (var (field, value) in values)
        {
            if (document.TryGetMember(field, out object current) == false || AreEqual(current, value) == false)
            {
                changed = true;
                break;
            }
        }

        if (changed == false)
            return true;

        // keep the original property order (as the script engine does), new fields go last in assignment order
        var result = new DynamicJsonValue();
        var property = new BlittableJsonReaderObject.PropertyDetails();
        using (var order = document.GetPropertiesByInsertionOrder())
        {
            for (var i = 0; i < order.Size; i++)
            {
                document.GetPropertyByIndex(order.GetPropertyIdByInsertionOrder(i), ref property);
                string name = property.Name;
                result[name] = values.Remove(name, out var replacement) ? replacement : property.Value;
            }
        }

        foreach (var (field, value) in values)
            result[field] = value;

        modified = context.ReadObject(result, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        return true;
    }

    private static bool AreEqual(object current, object value)
    {
        switch (current)
        {
            case null:
                return value == null;
            case LazyStringValue or LazyCompressedStringValue or string:
                return value is LazyStringValue or string && string.Equals(current.ToString(), value.ToString(), StringComparison.Ordinal);
            case long l:
                return value is long other && other == l;
            case bool b:
                return value is bool other2 && other2 == b;
            case LazyNumberValue n:
                return value is LazyNumberValue otherNumber && string.Equals(n.ToString(), otherNumber.ToString(), StringComparison.Ordinal);
            default:
                return false;
        }
    }
}
