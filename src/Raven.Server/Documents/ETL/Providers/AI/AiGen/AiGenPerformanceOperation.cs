using System;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI.AiGen;

public sealed class AiGenPerformanceOperation(TimeSpan duration) : EtlPerformanceOperation(duration);
