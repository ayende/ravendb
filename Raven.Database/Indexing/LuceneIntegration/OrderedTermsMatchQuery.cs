using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Queries;

namespace Raven.Database.Indexing.LuceneIntegration
{
	public class OrderedTermsMatchQuery : TermsMatchQuery
	{
		public List<string> OrderedValues { get; private set; }

		public OrderedTermsMatchQuery(string field, IEnumerable<string> matches)
			: base(field, matches)
		{
			OrderedValues = matches.ToList();
		}

		public SortedField GetSortedField()
		{
			return new SortedField(this);
		}

		public class SortField : Lucene.Net.Search.SortField
		{

			public OrderedTermsMatchQuery Query { get; private set; }

			internal SortField(OrderedTermsMatchQuery query)
				: base(query.Field, SortField.STRING)
			{
				Query = query;
			}

			public override Lucene.Net.Search.FieldComparator GetComparator(int numHits, int sortPos)
			{
				return new FieldComparator(Query, numHits);
			}

		}

		public class FieldComparator : Lucene.Net.Search.FieldComparator
		{

			private string[] values;
			private string[] currentReaderValues;
			private int bottom;
			public OrderedTermsMatchQuery Query { get; private set; }

			public FieldComparator(OrderedTermsMatchQuery query, int numHits)
			{
				values = new string[numHits];
				Query = query;
			}

			public override IComparable this[int slot]
			{
				get
				{
					return (IComparable)values[slot];
				}
			}

			public override int Compare(int slot1, int slot2)
			{
				var num1 = Query.OrderedValues.IndexOf(values[slot1]);
				var num2 = Query.OrderedValues.IndexOf(values[slot2]);
				if (num1 > num2)
					return 1;
				return num1 < num2 ? -1 : 0;
			}

			public override int CompareBottom(int doc)
			{
				var num = Query.OrderedValues.IndexOf(this.currentReaderValues[doc]);
				if (bottom > num)
					return 1;
				return bottom < num ? -1 : 0;

			}

			public override void Copy(int slot, int doc)
			{
				values[slot] = currentReaderValues[doc];
			}

			public override void SetBottom(int slot)
			{
				bottom = Query.OrderedValues.IndexOf(values[bottom]);
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				currentReaderValues = FieldCache_Fields.DEFAULT.GetStrings(reader, Query.Field);
			}
		}

		public class SortedField : Raven.Abstractions.Data.SortedField, ISortFieldGenerator
		{
			public OrderedTermsMatchQuery Query { get; private set; }

			public SortedField(OrderedTermsMatchQuery query)
				: base(query.Field)
			{
				Query = query;
			}

			public override bool Descending { get { return false; } set { throw new NotSupportedException(); } }

			public Lucene.Net.Search.SortField GetSortField()
			{
				return new SortField(Query);
			}
		}
	}
}