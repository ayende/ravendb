﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;
using SpellChecker.Net.Search.Spell;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Database.Queries
{
	public class SuggestionQueryIndexExtension : IIndexExtension
	{
		private readonly string key;
		private readonly string field;
		private readonly Directory directory;
		private readonly SpellChecker.Net.Search.Spell.SpellChecker spellChecker;

		[CLSCompliant(false)]
		public SuggestionQueryIndexExtension(
			string key,
			IndexReader reader,
			StringDistance distance, 
			string field, 
			float accuracy)
		{
			this.key = key;
			this.field = field;
			
			if(reader.Directory() is RAMDirectory)
			{
				directory = new RAMDirectory();
			}
			else
			{
				directory = FSDirectory.Open(new DirectoryInfo(key));
			}

			this.spellChecker = new SpellChecker.Net.Search.Spell.SpellChecker(directory, distance);
			this.spellChecker.SetAccuracy(accuracy);
		}

		public void Init(IndexReader reader)
		{
			spellChecker.IndexDictionary(new LuceneDictionary(reader, field));
		}

		public SuggestionQueryResult Query(SuggestionQuery suggestionQuery, IndexReader indexReader)
		{
			if(suggestionQuery.Term.StartsWith("<<") && suggestionQuery.Term.EndsWith(">>"))
			{
				return QueryOverMultipleWords(suggestionQuery, indexReader, 
					suggestionQuery.Term.Substring(2, suggestionQuery.Term.Length - 4));
			}
			if (suggestionQuery.Term.StartsWith("(") && suggestionQuery.Term.EndsWith(")"))
			{
				return QueryOverMultipleWords(suggestionQuery, indexReader,
					suggestionQuery.Term.Substring(1, suggestionQuery.Term.Length - 2));
			}
			string[] suggestions = spellChecker.SuggestSimilar(suggestionQuery.Term,
			                                                   suggestionQuery.MaxSuggestions,
			                                                   indexReader, 
			                                                   suggestionQuery.Field,
			                                                   true);

			return new SuggestionQueryResult
			{
				Suggestions = suggestions
			};
		}

		private SuggestionQueryResult QueryOverMultipleWords(SuggestionQuery suggestionQuery, IndexReader indexReader,
		                                                     string queryText)
		{
			var individualTerms = queryText.Split(new[] {' ', '\t', '\r', '\n'}, StringSplitOptions.RemoveEmptyEntries);
			var result = new List<string>();

			foreach (var term in individualTerms)
			{
				result.AddRange(spellChecker.SuggestSimilar(term,
				                                            suggestionQuery.MaxSuggestions,
				                                            indexReader,
				                                            suggestionQuery.Field,
				                                            suggestionQuery.Popularity));
			}

			return new SuggestionQueryResult
			{
				Suggestions = result.ToArray()
			};
		}

		public void OnDocumentsIndexed(IEnumerable<Document> documents)
		{
			spellChecker.IndexDictionary(new EnumerableDictionary(documents, field));
		}

		public class EnumerableDictionary : SpellChecker.Net.Search.Spell.IDictionary
		{
			private readonly IEnumerable<Document> documents;
			private readonly string field;

			public EnumerableDictionary(IEnumerable<Document> documents, string field)
			{
				this.documents = documents;
				this.field = field;
			}

			public IEnumerator<string> GetWordsIterator()
			{
				return (from document in documents 
						from fieldable in document.GetFieldables(field) 
						select fieldable.StringValue
						).GetEnumerator();
			}
		}

		public void Dispose()
		{
			spellChecker.Close();
			GC.SuppressFinalize(spellChecker);//stupid! but it doesn't do it on its own
		}
	}
}
