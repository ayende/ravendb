using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Corax.Querying.Planning;

public sealed class PlanDecisionTrail
{
    private readonly List<PlanDecisionEntry> _entries = new();
    public IReadOnlyList<PlanDecisionEntry> Entries => _entries;

    public void Record(string optimization, bool accepted, string reason)
    {
        _entries.Add(new PlanDecisionEntry(optimization, accepted, reason));
    }

    public DynamicJsonValue ToJson()
    {
        var arr = new DynamicJsonArray();
        foreach (var entry in _entries)
            arr.Add(entry.ToJson());
        return new DynamicJsonValue { [nameof(Entries)] = arr };
    }
}

public sealed class PlanDecisionEntry
{
    public string Optimization { get; }
    public bool Accepted { get; }
    public string Reason { get; }

    public PlanDecisionEntry(string optimization, bool accepted, string reason)
    {
        Optimization = optimization;
        Accepted = accepted;
        Reason = reason;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Optimization)] = Optimization,
            [nameof(Accepted)] = Accepted,
            [nameof(Reason)] = Reason
        };
    }
}
