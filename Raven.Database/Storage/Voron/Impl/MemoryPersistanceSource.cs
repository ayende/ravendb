﻿namespace Raven.Database.Storage.Voron.Impl
{
	using global::Voron;

	public class MemoryPersistenceSource : IPersistenceSource
	{
		public MemoryPersistenceSource()
		{
			CreatedNew = true;
			Options = new StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions();
		}

		public StorageEnvironmentOptions Options { get; private set; }

		public bool CreatedNew { get; private set; }
	}
}