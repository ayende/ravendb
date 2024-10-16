﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Generic;
using FastSerialization;

public class DotNetHeapInfo : IFastSerializable
{
    /// <summary>
    /// If we could not properly walk an object, this is incremented.
    /// Hopefully this is zero.
    /// </summary>
    public int CorruptedObject { get; internal set; }
    /// <summary>
    /// This is the number of bytes we had to skip because of errors walking the segments.
    /// </summary>
    public long UndumpedSegementRegion { get; internal set; }

    /// <summary>
    /// This is the sum of all space in the GC segments.
    /// </summary>
    public long SizeOfAllSegments { get; internal set; }
    /// <summary>
    /// The memory regions that user objects can be allocated from
    /// </summary>
    public List<GCHeapDumpSegment> Segments { get; internal set; }
    /// <summary>
    /// Given an object, determine what GC generation it is in.  Gen 3 is the large object heap
    /// returns -1 if the object is not in any GC segment.
    /// </summary>
    public int GenerationFor(ulong obj)
    {
        // Find the segment
        if ((m_lastSegment == null) || !(m_lastSegment.Start <= obj && obj < m_lastSegment.End))
        {
            if (Segments == null)
            {
                return -1;
            }

            for (int i = 0; ; i++)
            {
                if (i >= Segments.Count)
                {
                    return -1;
                }

                GCHeapDumpSegment segment = Segments[i];
                if (segment.Start <= obj && obj < segment.End)
                {
                    m_lastSegment = segment;
                    break;
                }
            }
        }

        if (obj < m_lastSegment.Gen4End)
        {
            return 4;
        }

        if (obj < m_lastSegment.Gen3End)
        {
            return 3;
        }

        if (obj < m_lastSegment.Gen2End)
        {
            return 2;
        }

        if (obj < m_lastSegment.Gen1End)
        {
            return 1;
        }

        if (obj < m_lastSegment.Gen0End)
        {
            return 0;
        }

        return -1;
    }

    #region private
    void IFastSerializable.ToStream(Serializer serializer)
    {
        serializer.Write(SizeOfAllSegments);
        if (Segments != null)
        {
            serializer.Write(Segments.Count);
            foreach (GCHeapDumpSegment segment in Segments)
            {
                serializer.Write(segment);
            }
        }
        else
        {
            serializer.Write(0);
        }
    }
    void IFastSerializable.FromStream(Deserializer deserializer)
    {
        SizeOfAllSegments = deserializer.ReadInt64();
        int count = deserializer.ReadInt();
        Segments = new List<GCHeapDumpSegment>(count);
        for (int i = 0; i < count; i++)
        {
            Segments.Add((GCHeapDumpSegment)deserializer.ReadObject());
        }
    }

    private GCHeapDumpSegment m_lastSegment;    // cache for GenerationFor
    #endregion
}

public class GCHeapDumpSegment : IFastSerializable, IFastSerializableVersion
{
    public ulong Start { get; internal set; }
    public ulong End { get; internal set; }
    public ulong Gen0End { get; internal set; }
    public ulong Gen1End { get; internal set; }
    public ulong Gen2End { get; internal set; }
    public ulong Gen3End { get; internal set; }
    public ulong Gen4End { get; internal set; }

    public int Version => 1;

    public int MinimumVersionCanRead => 0;

    public int MinimumReaderVersion => 1;

    #region private
    void IFastSerializable.ToStream(Serializer serializer)
    {
        serializer.Write((long)Start);
        serializer.Write((long)End);
        serializer.Write((long)Gen0End);
        serializer.Write((long)Gen1End);
        serializer.Write((long)Gen2End);
        serializer.Write((long)Gen3End);
        serializer.Write((long)Gen4End);
    }

    void IFastSerializable.FromStream(Deserializer deserializer)
    {
        Start = (ulong)deserializer.ReadInt64();
        End = (ulong)deserializer.ReadInt64();
        Gen0End = (ulong)deserializer.ReadInt64();
        Gen1End = (ulong)deserializer.ReadInt64();
        Gen2End = (ulong)deserializer.ReadInt64();
        Gen3End = (ulong)deserializer.ReadInt64();
        if (deserializer.VersionBeingRead >= 1)
        {
            Gen4End = (ulong)deserializer.ReadInt64();
        }
    }
    #endregion
}
