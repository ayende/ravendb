﻿using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;

namespace Raven.Database.Queries
{
	public class FacetedQueryRunner
	{
		private readonly DocumentDatabase database;

		public FacetedQueryRunner(DocumentDatabase database)
		{
			this.database = database;
		}

		public FacetResults GetFacets(string index, IndexQuery indexQuery, JsonDocument facetSetup)
		{
			var facets = facetSetup.DataAsJson.JsonDeserialization<FacetSetup>().Facets;

			var results = new FacetResults();
			var defaultFacets = new Dictionary<string, Facet>();
			var rangeFacets = new Dictionary<string, List<ParsedRange>>();

			foreach (var facet in facets)
			{
				switch (facet.Mode)
				{
					case FacetMode.Default:
						//Remember the facet, so we can run them all under one query
						defaultFacets[facet.Name] = facet;
						results.Results[facet.Name] = new FacetResult();
						break;
					case FacetMode.Ranges:
						rangeFacets[facet.Name] = facet.Ranges.Select(range => ParseRange(facet.Name, range)).ToList();
						results.Results[facet.Name] = new FacetResult
						{
							Values = facet.Ranges.Select(range => new FacetValue
							{
								Range = range,
							}).ToList()
						};

						break;
					default:
						throw new ArgumentException(string.Format("Could not understand '{0}'", facet.Mode));
				}
			}

			new QueryForFacets(database, index, defaultFacets, rangeFacets, indexQuery, results).Execute();

			return results;
		}

		private static ParsedRange ParseRange(string field, string range)
		{
			var parts = range.Split(new[] { " TO " }, 2, StringSplitOptions.RemoveEmptyEntries);

			if (parts.Length != 2)
				throw new ArgumentException("Could not understand range query: " + range);

			var trimmedLow = parts[0].Trim();
			var trimmedHigh = parts[1].Trim();
			var parsedRange = new ParsedRange
			{
				Field = field,
				RangeText = range,
				LowInclusive = IsInclusive(trimmedLow.First()),
				HighInclusive = IsInclusive(trimmedHigh.Last()),
				LowValue = trimmedLow.Substring(1),
				HighValue = trimmedHigh.Substring(0, trimmedHigh.Length - 1)
			};

			if (RangeQueryParser.NumerciRangeValue.IsMatch(parsedRange.LowValue))
			{
				parsedRange.LowValue = NumericStringToSortableNumeric(parsedRange.LowValue);
			}

			if (RangeQueryParser.NumerciRangeValue.IsMatch(parsedRange.HighValue))
			{
				parsedRange.HighValue = NumericStringToSortableNumeric(parsedRange.HighValue);
			}


			if (parsedRange.LowValue == "NULL" || parsedRange.LowValue == "*")
				parsedRange.LowValue = null;
			if (parsedRange.HighValue == "NULL" || parsedRange.HighValue == "*")
				parsedRange.HighValue = null;



			return parsedRange;
		}

		private static string NumericStringToSortableNumeric(string value)
		{
			var number = NumberUtil.StringToNumber(value);
			if (number is int)
			{
				return NumericUtils.IntToPrefixCoded((int)number);
			}
			if (number is long)
			{
				return NumericUtils.LongToPrefixCoded((long)number);
			}
			if (number is float)
			{
				return NumericUtils.FloatToPrefixCoded((float)number);
			}
			if (number is double)
			{
				return NumericUtils.DoubleToPrefixCoded((double)number);
			}

			throw new ArgumentException("Uknown type for " + number.GetType() + " which started as " + value);
		}

		private static bool IsInclusive(char ch)
		{
			switch (ch)
			{
				case '[':
				case ']':
					return true;
				case '{':
				case '}':
					return false;
				default:
					throw new ArgumentException("Could not understand range prefix: " + ch);
			}
		}

		private class ParsedRange
		{
			public bool LowInclusive;
			public bool HighInclusive;
			public string LowValue;
			public string HighValue;
			public string RangeText;
			public string Field;

			public bool IsMatch(string value)
			{
				var compareLow =
					LowValue == null
						? -1
						: string.CompareOrdinal(value, LowValue);
				var compareHigh = HighValue == null ? 1 : string.CompareOrdinal(value, HighValue);
				// if we are range exclusive on either end, check that we will skip the edge values
				if (compareLow == 0 && LowInclusive == false ||
					compareHigh == 0 && HighInclusive == false)
					return false;

				if (LowValue != null && compareLow < 0)
					return false;

				if (HighValue != null && compareHigh > 0)
					return false;

				return true;
			}

			public override string ToString()
			{
				return string.Format("{0}:{1}", Field, RangeText);
			}
		}

		private class QueryForFacets
		{
			public QueryForFacets(
				DocumentDatabase database,
				string index,
				 Dictionary<string, Facet> facets,
				 Dictionary<string, List<ParsedRange>> ranges,
				 IndexQuery indexQuery,
				 FacetResults results)
			{
				Database = database;
				Index = index;
				Facets = facets;
				Ranges = ranges;
				IndexQuery = indexQuery;
				Results = results;
			}

			DocumentDatabase Database { get; set; }
			string Index { get; set; }
			Dictionary<string, Facet> Facets { get; set; }
			Dictionary<string, List<ParsedRange>> Ranges { get; set; }
			IndexQuery IndexQuery { get; set; }
			FacetResults Results { get; set; }

			struct FacetMatch
			{
				public string Value;
				public int Count;
			}

			public void Execute()
			{
				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
				var allCollector = new GatherAllCollector();
				var facetsByName = new Dictionary<string, List<FacetMatch>>();

				IndexSearcher currentIndexSearcher;
				using (Database.IndexStorage.GetCurrentIndexSearcher(Index, out currentIndexSearcher))
				{
					var baseQuery = Database.IndexStorage.GetLuceneQuery(Index, IndexQuery, Database.IndexQueryTriggers);
					currentIndexSearcher.Search(baseQuery, allCollector);
					var fields = Facets.Values.Select(x => x.Name).Concat(Ranges.Select(x => x.Key)).ToArray();

					foreach (var field in Facets.Values)
					{
						facetsByName.Add(field.Name, new List<FacetMatch>((int)Math.Log(allCollector.Documents.Count)));
					}

					List<FacetMatch> current = null;
					string currentField = null;

					IndexedTerms.ReadEntriesForFields(currentIndexSearcher.IndexReader,
						fields,
						allCollector.Documents,
						(term, count) =>
						{
							List<ParsedRange> list;
							if (Ranges.TryGetValue(term.Field, out list))
							{
								for (int i = 0; i < list.Count; i++)
								{
									var parsedRange = list[i];
									if (parsedRange.IsMatch(term.Text))
									{
										Results.Results[term.Field].Values[i].Hits += count;
									}
								}
							}
							else
							{
								if (currentField != term.Field || current == null)
								{
									currentField = term.Field;
									current = facetsByName[currentField];
								}
								current.Add(new FacetMatch { Count = count, Value = term.Text });
							}
						});
				}

				UpdateFacetResults(facetsByName);
			}

			private void UpdateFacetResults(Dictionary<string, List<FacetMatch>> facetsByName)
			{
				foreach (var kvp in facetsByName)
				{
					var facet = Facets[kvp.Key];

					int maxResults = Math.Min(facet.MaxResults ?? Database.Configuration.MaxPageSize, Database.Configuration.MaxPageSize);

					var values = kvp.Value;

					IOrderedEnumerable<FacetMatch> ordered = null;
					switch (facet.TermSortMode)
					{
						case FacetTermSortMode.ValueAsc:
							ordered = values.OrderBy(x => x.Value).ThenBy(x => x.Count);
							break;
						case FacetTermSortMode.ValueDesc:
							ordered = values.OrderByDescending(x => x.Value).ThenBy(x => x.Count);
							break;
						case FacetTermSortMode.HitsAsc:
							ordered = values.OrderBy(x => x.Count).ThenBy(x => x.Value);
							break;
						case FacetTermSortMode.HitsDesc:
							ordered = values.OrderByDescending(x => x.Count).ThenBy(x => x.Value);
							break;
					}

					Results.Results[facet.Name] = new FacetResult
					{
						Values = values.Take(maxResults).Select(x => new FacetValue { Hits = x.Count, Range = x.Value }).ToList(),
						RemainingTermsCount = values.Count - maxResults,
						RemainingHits = values.Skip(maxResults).Sum(x => x.Count),
						RemainingTerms = facet.InclueRemainingTerms ? values.Skip(maxResults).Select(x => x.Value).ToList() : null,
					};
				}
			}


			private static object NumericStringToNum(string value)
			{
				try
				{
					return NumericUtils.PrefixCodedToDouble(value);
				}
				catch (Exception)
				{

				}

				try
				{
					return NumericUtils.PrefixCodedToFloat(value);
				}
				catch (Exception)
				{

				}

				try
				{
					return NumericUtils.PrefixCodedToLong(value);
				}
				catch (Exception)
				{

				}

				try
				{
					return NumericUtils.PrefixCodedToInt(value);
				}
				catch (Exception)
				{

				}

				return null;
			}


		}
	}


}
