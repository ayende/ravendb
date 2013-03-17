using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Silverlight.MissingFromSilverlight;
using System.Linq;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Query
{
    public class QueryState
    {
	    public QueryState(string indexName, string query, IEnumerable<string> sortOptions, bool isSpatialQuery, double? latitude, double? longitude, double? radius, bool x)
	    {
		    IndexName = indexName;
		    Query = query;
		    IsSpatialQuery = isSpatialQuery;
		    Latitude = latitude;
		    Longitude = longitude;
		    Radius = radius;
		    SortOptions = sortOptions.ToList();
	    }

	    public QueryState(QueryModel model)
	    {
			IndexName = model.IndexName;
			Query = model.Query;
			SortOptions = model.SortBy.Select(r => r.Value).ToList();
			IsSpatialQuery = model.IsSpatialQuery;
			Latitude = model.Latitude;
			Longitude = model.Longitude;
			Radius = model.Radius;
			DefaultOperator = model.DefaultOperator;
			ShowFields = model.ShowFields;
			ShowEntries = model.ShowEntries;
			UseTransformer = model.UseTransformer;
			SkipTransform = model.SkipTransformResults;
			Transformer = model.SelectedTransformer.Value;
	    }

	    public QueryState(SavedQuery savedQuery)
	    {
		    if (savedQuery == null)
			    return;
			IndexName = savedQuery.IndexName;
			Query = savedQuery.Query;
			SortOptions = savedQuery.SortOptions;
			IsSpatialQuery = savedQuery.IsSpatialQuery;
			Latitude = savedQuery.Latitude;
			Longitude = savedQuery.Longitude;
			Radius = savedQuery.Radius;
			DefaultOperator = savedQuery.DefaultOperator;
			ShowFields = savedQuery.ShowFields;
			ShowEntries = savedQuery.ShowEntries;
			UseTransformer = savedQuery.UseTransformer;
			SkipTransform = savedQuery.SkipTransform;
			Transformer = savedQuery.Transformer;
	    }

	    public string IndexName { get; private set; }

        public string Query { get; private set; }
        public bool IsSpatialQuery { get; private set; }
        public double? Latitude { get; private set; }
        public double? Longitude { get; private set; }
        public double? Radius { get; private set; }
		public QueryOperator DefaultOperator { get; set; }
		public bool ShowFields { get; set; }
		public bool ShowEntries { get; set; }
		public bool UseTransformer { get; set; }
		public bool SkipTransform { get; set; }
		public string Transformer { get; set; }

        public IList<string> SortOptions { get; private set; }

        public string GetHash()
        {
            return CreateQueryStateHash(this);
        }

	    public static string CreateQueryStateHash(QueryState state)
	    {
			var spatialString = state.IsSpatialQuery ? string.Format("{0}{1}{2}", state.Latitude, state.Longitude, state.Radius) : "";
		    var optionString = string.Format("{0}{1}{2}{3}{4}{5}", state.DefaultOperator, state.ShowFields, state.ShowEntries,
		                                     state.UseTransformer, state.SkipTransform, state.Transformer);
			return MD5Core.GetHashString(state.IndexName + state.Query + spatialString + optionString);
	    }
    }
}