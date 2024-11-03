using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Compression;
using Voron.Data.Containers;
using Voron.Debugging;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    private static string[] Templates =
    [
        @"<html>
<head>
    <script type=""text/javascript"" src=""https://unpkg.com/vis-network/standalone/umd/vis-network.min.js""></script>
</head>
<body>
    <h1>",
        @" </h1>
    <div id=""graph""></div>
    <script type=""text/javascript"">
        var nodes = [ ",
        @" ];
        var edges = [ ",
        @" ];
        var network = new vis.Network(
            document.getElementById(""graph""),
            { nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) },
            {physics: false});
    </script>
</body>

</html>"
    ];

    private static long GetPostingListCount(LowLevelTransaction llt,long postingListId)
    {
        switch (postingListId & 0b11)
        {
            case 0:
                return 0;
            case 0b01:
                return 1;
            case 0b10:
                var item = Container.Get(llt, postingListId & ~0b11);
                return VariableSizeEncoding.Read<int>(item.Address, out _);
            case 0b11:
                throw new NotImplementedException();
        }

        throw new NotSupportedException();
    }

    public static void RenderAndShow(LowLevelTransaction llt, long graphId)
    {
        var searchState = new SearchState(llt, graphId);
        for (int level = 0; level <= searchState.Options.MaxLevel; level++)
        {
            RenderAndShowLevel(llt, level, searchState);
        }
    }

    public static void RenderAndShowLevel(LowLevelTransaction llt, int level, SearchState searchState)
    {
        if (Debugger.IsAttached is false)
            return;
        string fileName = Path.GetTempFileName() + "-" + level + ".html";
        using (var f = File.CreateText(fileName))
        {
            f.Write(Templates[0]);
            f.Write(level);
            f.Write(Templates[1]);

            var edges = new HashSet<(long, long)>();
            
            for (long j = 1; j <= searchState.Options.CountOfVectors; j++)
            {
                ref var n = ref searchState.GetNodeById(j);
                if (level >= n.NeighborsPerLevel.Count)
                    continue;

                var postingListCount = GetPostingListCount(llt, n.PostingListId);
                var item = Container.Get(llt, n.VectorId);
                var vec = MemoryMarshal.Cast<byte, float>(item.ToSpan());
                f.Write($"{{ id: {j}, label: '#{j:##,###}', title: '{postingListCount} - [ ");
                for (int k = 0; k < Math.Min(8, vec.Length); k++)
                {
                    if (k > 0)
                        f.Write(',');
                    f.Write(vec[k]);
                }

                if (vec.Length > 8)
                {
                    f.Write(", ...");
                }

                f.WriteLine("]' }, ");
            }

            f.WriteLine(Templates[2]);

            for (long j = 1; j <= searchState.Options.CountOfVectors; j++)
            {
                ref var n = ref searchState.GetNodeById(j);
                if (level >= n.NeighborsPerLevel.Count)
                    continue;

                foreach (var to in n.NeighborsPerLevel[level])
                {
                    var key = (Math.Min(j, to), Math.Max(j, to));
                    if (edges.Add(key))
                    {
                        var dist = searchState.Distance(n.VectorId, searchState.GetNodeById(to).VectorId);
                        f.WriteLine($"{{ from: {j}, to: {to}, title: '{dist}' }},");
                    }
                }
            }

            f.WriteLine(Templates[3]);
        }
        DebugStuff.OpenBrowser(fileName);
    }
}
