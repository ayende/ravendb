﻿using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.CompressionAndEncryption
{
	public class Indexes : CompressionAndEncryption
	{
		[Fact]
		public void SimpleIndexes()
		{
			const string FirstCompany = "FirstCompany";
			const string SecondCompany = "SecondCompany";
			const string IndexName = "TestIndex";

			documentStore.DatabaseCommands.PutIndex(IndexName,
				new IndexDefinition
				{
					Map =
						@"
							from c in docs.Companies
							select new 
								{
									c.Name
								}
						",
					Stores =
					{
						{ "Name", FieldStorage.Yes }
					}
				});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = SecondCompany });
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}

			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany);
		}


		[Fact]
		public void Restart()
		{
			const string FirstCompany = "FirstCompany";
			const string SecondCompany = "SecondCompany";
			const string IndexName = "TestIndex";

			documentStore.DatabaseCommands.PutIndex(IndexName,
				new IndexDefinition
				{
					Map =
						@"
							from c in docs.Companies
							select new 
								{
									c.Name
								}
						",
					Stores =
					{
						{ "Name", FieldStorage.Yes }
					}
				});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = SecondCompany });
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}

			RecycleServer();

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<Company>("Name")
					.ToList();
			}


			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany);
		}
		

		[Fact]
		public void MapReduce()
		{
			const string FirstCompany = "FirstCompany";
			const string SecondCompany = "SecondCompany";
			const string IndexName = "TestIndex";

			documentStore.DatabaseCommands.PutIndex(IndexName,
				new IndexDefinition
				{
					Map =
						@"
							from c in docs.Companies
							select new 
							{
								Name = c.Name,
								Count = 1
							}
						",
					Reduce = 
						@"
							from doc in results
							group doc by doc.Name into g
							select new
							{
								Name = g.Key,
								Count = g.Sum(x => x.Count)
							}
						",
					Stores =
					{
						{ "Name", FieldStorage.Yes },
						{ "Count", FieldStorage.Yes },
					}
				});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = SecondCompany });
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Company>(IndexName)
					.WaitForNonStaleResults()
					.SelectFields<CompanyCount>("Name", "Count")
					.ToList();
			}

			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany);
		}
	}

	class CompanyCount
	{
		public string Name { get; set; }
		public int Count { get; set; }
	}
}
