﻿using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Connection;
using Raven.Database;
using Raven.Database.Config.Retriever;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning
{
	internal static class VersioningUtil
	{
		public const string RavenDocumentRevision = "Raven-Document-Revision";
		public const string RavenDocumentParentRevision = "Raven-Document-Parent-Revision";
		public const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";

		public static VersioningConfiguration GetDocumentVersioningConfiguration(this DocumentDatabase database, RavenJObject metadata)
		{
			ConfigurationDocument<VersioningConfiguration> config = null;

			var entityName = metadata.Value<string>("Raven-Entity-Name");
			if (entityName != null)
				config = database.ConfigurationRetriever.GetConfigurationDocument<VersioningConfiguration>("Raven/Versioning/" + entityName);

			if (config == null)
				config = database.ConfigurationRetriever.GetConfigurationDocument<VersioningConfiguration>("Raven/Versioning/DefaultConfiguration");

			return config == null ? null : config.MergedDocument;
		}

		public static bool IsVersioningActive(this DocumentDatabase database, RavenJObject metadata)
		{
			var versioningConfiguration = database.GetDocumentVersioningConfiguration(metadata);
			return versioningConfiguration != null && versioningConfiguration.Exclude == false;
		}

        public static bool ChangesToRevisionsAllowed(this DocumentDatabase database)
        {
            var changesToRevisionsAllowed = database.Configuration.Settings["Raven/Versioning/ChangesToRevisionsAllowed"];
            if (changesToRevisionsAllowed == null)
                return false;
            bool result;
            if (bool.TryParse(changesToRevisionsAllowed, out result) == false)
                return false;
            return result;
        }
	}
}