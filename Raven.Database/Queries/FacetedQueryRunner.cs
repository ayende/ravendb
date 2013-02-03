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

		public FacetResults GetFacets(string index, IndexQuery indexQuery, string facetSetupDoc, int start = 0, int? pageSize = null)
		{
			var facetSetup = database.Get(facetSetupDoc, null);
			if (facetSetup == null)
				throw new InvalidOperationException("Could not find facets document: " + facetSetupDoc);

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

			new QueryForFacets(database, index, defaultFacets, rangeFacets, indexQuery, results, start, pageSize).Execute();

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

			if (RangeQueryParser.NumericRangeValue.IsMatch(parsedRange.LowValue))
			{
				parsedRange.LowValue = NumericStringToSortableNumeric(parsedRange.LowValue);
			}

			if (RangeQueryParser.NumericRangeValue.IsMatch(parsedRange.HighValue))
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

			throw new ArgumentException("Unknown type for " + number.GetType() + " which started as " + value);
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
				 FacetResults results,
				 int start,
				 int? pageSize)
			{
				Database = database;
				Index = index;
				Facets = facets;
				Ranges = ranges;
				IndexQuery = indexQuery;
				Results = results;
				Start = start;
				PageSize = pageSize;
			}

			DocumentDatabase Database { get; set; }
			string Index { get; set; }
			Dictionary<string, Facet> Facets { get; set; }
			Dictionary<string, List<ParsedRange>> Ranges { get; set; }
			IndexQuery IndexQuery { get; set; }
			FacetResults Results { get; set; }
			private int Start { get; set; }
			private int? PageSize { get; set; }

			public void Execute()
			{
				//We only want to run the base query once, so we capture all of the facet-ing terms then run the query
				//	once through the collector and pull out all of the terms in one shot
				var allCollector = new GatherAllCollector();
				var facetsByName = new Dictionary<string, Dictionary<string, int>>();

				IndexSearcher currentIndexSearcher;
				using (Database.IndexStorage.GetCurrentIndexSearcher(Index, out currentIndexSearcher))
				{
					var baseQuery = Database.IndexStorage.GetLuceneQuery(Index, IndexQuery, Database.IndexQueryTriggers);
					currentIndexSearcher.Search(baseQuery, allCollector);
					var fields = Facets.Values.Select(x => x.Name)
							.Concat(Ranges.Select(x => x.Key));
					var fieldsToRead = new HashSet<string>(fields);
					IndexedTerms.ReadEntriesForFields(currentIndexSearcher.IndexReader,
						fieldsToRead,
						allCollector.Documents,
						term =>
						{
							List<ParsedRange> list;
							if (Ranges.TryGetValue(term.Field, out list))
							{
								for (int i = 0; i < list.Count; i++)
								{
									var parsedRange = list[i];
									if (parsedRange.IsMatch(term.Text))
									{
										Results.Results[term.Field].Values[i].Hits++;
									}
								}
							}

							Facet value;
							if (Facets.TryGetValue(term.Field, out value))
							{
								var facetValues = facetsByName.GetOrAdd(term.Field);
								facetValues[term.Text] = facetValues.GetOrDefault(term.Text) + 1;
							}
						});
				}

				UpdateFacetResults(facetsByName);
			}

			private void UpdateFacetResults(IDictionary<string, Dictionary<string, int>> facetsByName)
			{
				foreach (var facet in Facets.Values)
				{
					var values = new List<FacetValue>();
					List<string> allTerms;

					int maxResults = Math.Min(PageSize ?? facet.MaxResults ?? Database.Configuration.MaxPageSize, Database.Configuration.MaxPageSize);
					var groups = facetsByName.GetOrDefault(facet.Name);

					if (groups == null)
						continue;

					switch (facet.TermSortMode)
					{
						case FacetTermSortMode.ValueAsc:
							allTerms = new List<string>(groups.OrderBy(x => x.Key).ThenBy(x => x.Value).Select(x => x.Key));
							break;
						case FacetTermSortMode.ValueDesc:
							allTerms = new List<string>(groups.OrderByDescending(x => x.Key).ThenBy(x => x.Value).Select(x => x.Key));
							break;
						case FacetTermSortMode.HitsAsc:
							allTerms = new List<string>(groups.OrderBy(x => x.Value).ThenBy(x => x.Key).Select(x => x.Key));
							break;
						case FacetTermSortMode.HitsDesc:
							allTerms = new List<string>(groups.OrderByDescending(x => x.Value).ThenBy(x => x.Key).Select(x => x.Key));
							break;
						default:
							throw new ArgumentException(string.Format("Could not understand '{0}'", facet.TermSortMode));
					}

					foreach (var term in allTerms.Skip(Start).TakeWhile(term => values.Count < maxResults))
					{
						values.Add(new FacetValue
						{
							Hits = groups.GetOrDefault(term),
							Range = term
						});
					}

					var previousHits = allTerms.Take(Start).Sum(allTerm => groups.GetOrDefault(allTerm));
					Results.Results[facet.Name] = new FacetResult
					{
						Values = values,
						RemainingTermsCount = allTerms.Count - (Start + values.Count),
						RemainingHits = groups.Values.Sum() - (previousHits + values.Sum(x => x.Hits)),
					};

					if (facet.InclueRemainingTerms)
						Results.Results[facet.Name].RemainingTerms = allTerms.Skip(Start + values.Count).ToList();
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
