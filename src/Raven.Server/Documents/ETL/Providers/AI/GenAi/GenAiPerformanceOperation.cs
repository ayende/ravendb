using System;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public sealed class GenAiPerformanceOperation(TimeSpan duration) : EtlPerformanceOperation(duration);
