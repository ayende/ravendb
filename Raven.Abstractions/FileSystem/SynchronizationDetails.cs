﻿using Raven.Abstractions.Data;

namespace Raven.Abstractions.FileSystem
{
	public class SynchronizationDetails
	{
		public string FileName { get; set; }
		public Etag FileETag { get; set; }
		public string DestinationUrl { get; set; }
		public SynchronizationType Type { get; set; }
	}
}