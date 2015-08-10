﻿using System;
using System.Collections.Generic;
using Raven.Abstractions.Exceptions;

namespace Raven.Abstractions.Data
{
	public class AutoTunerInfo
	{
		public List<AutoTunerDecisionDescription> Reason { get; set; }
		public List<LowMemoryCalledRecord> LowMemoryCallsRecords { get; set; }

	}
	public class LowMemoryCalledRecord
	{

		public LowMemoryCalledRecord()
		{
			Operations = new List<LowMemoryHandlerStatistics>();
		}

		public DateTime StartedAt { get; set; }
		public TimeSpan Duration { get; set; }
		public string Reason { get; set; }

		public List<LowMemoryHandlerStatistics> Operations { get; set; }

	}
	public class LowMemoryHandlerStatistics
	{
		public string Name { get; set; }
		public long EstimatedUsedMemory { get; set; }
		public string DatabaseName { get; set; }
		public object Metadata { get; set; }
		public string Summary { get; set; }

	}
}
