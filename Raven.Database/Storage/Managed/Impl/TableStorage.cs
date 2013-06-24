//-----------------------------------------------------------------------
// <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Util;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Munin;

namespace Raven.Storage.Managed.Impl
{
	public class TableStorage : Munin.Database
	{
		public TableStorage(IPersistentSource persistentSource)
			: base(persistentSource)
		{
			Details = Add(new Table("Details"));

			Identity = Add(new Table(x => x.Value<string>("name"), "Identity"));

			Attachments = Add(new Table(x => x.Value<string>("key"), "Attachments")
			{
				{"ByKey", x => x.Value<string>("key")},
				{"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))},
			});

			Documents = Add(new Table(x => x.Value<string>("key"), "Documents")
			{
				{"ByKey", x => x.Value<string>("key")},
				{"ById", x => x.Value<string>("id")},
				{"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))}
			});

			DocumentsModifiedByTransactions =
				Add(new Table(x => new RavenJObject{{"key", x.Value<string>("key")}},
							  "DocumentsModifiedByTransactions")
				{
					{"ByTxId", x => new ComparableByteArray(x.Value<byte[]>("txId"))}
				});

			IndexingStats =
				Add(new Table(x => x.Value<string>("index"), "IndexingStats"));

			LastIndexedEtags =
				Add(new Table(x => x.Value<string>("index"), "LastIndexedEtags"));

			MappedResults = Add(new Table("MappedResults")
			{
				{"ByView", x=> x.Value<string>("view")},
				{"ByViewAndReduceKey", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"))},
				{"ByViewAndDocumentId", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("docId"))},
				{"ByViewAndEtagDesc", x => Tuple.Create(x.Value<string>("view"), new ReverseComparableByteArrayWhichIgnoresNull(x.Value<byte[]>("etag")))},
				{"ByViewAndEtag", x => Tuple.Create(x.Value<string>("view"), new ComparableByteArray(x.Value<byte[]>("etag")))},
				{"ByViewReduceKeyAndBucket", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"), x.Value<int>("bucket"))}
			});

			ReduceResults = Add(new Table("ReducedResults")
			{
				{"ByView", x=> x.Value<string>("view")},
				{"ByViewReduceKeyAndSourceBucket", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"), x.Value<int>("sourceBucket"))},
				{"ByViewReduceKeyLevelAndBucket", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"), x.Value<int>("level"), x.Value<int>("bucket"))}
			});

			Queues = Add(new Table(x => new RavenJObject
			{
				{"name", x.Value<string>("name")},
				{"id", x.Value<byte[]>("id")}
			}, "Queues")
			{
				{"ByName", x => x.Value<string>("name")}
			});

			Tasks = Add(new Table(x => new RavenJObject
			{
				{"index", x.Value<string>("index")},
				{"id", x.Value<byte[]>("id")}
			}, "Tasks")
			{
				{"ByIndexAndTime", x => Tuple.Create(x.Value<string>("index"), x.Value<DateTime>("time"))},
				{"ByIndexAndType", x => Tuple.Create(x.Value<string>("index"), x.Value<string>("type"))}
			});

			Lists = Add(new Table(x => new RavenJObject
			{
				{"name", x.Value<string>("name")},
				{"key", x.Value<string>("key")},
			}, "Lists")
			{
				{"ByNameAndEtag", x => Tuple.Create(x.Value<string>("name"), new ComparableByteArray(x.Value<byte[]>("etag")))},
			});

			ScheduleReductions = Add(new Table("ScheduleReductions")
			{
				{"ByView", x=> x.Value<string>("view")},
				{"ByViewAndReduceKey", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"))},
				{"ByViewLevelReduceKeyAndBucket", x => Tuple.Create(x.Value<string>("view"), x.Value<int>("level"), x.Value<string>("reduceKey"), x.Value<int>("bucket"))},
			});

			ReduceKeys = Add(new Table(x => new RavenJObject()
			{
				{"view", x.Value<string>("view")},
				{"reduceKey", x.Value<string>("reduceKey")}                                      
			}, "ReduceTypesPerKey")
			{
				{"ByView", x=> x.Value<string>("view")},
				{"ByViewAndReduceKey", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"))},
			});

			DocumentReferences = Add(new Table(x => new RavenJObject()
			{
				{"view", x.Value<string>("view")},
				{"key", x.Value<string>("key")} ,                                  
				{"ref", x.Value<string>("ref")} ,
			}, "DocumentReferences")
			{
				{"ByKey", x=> x.Value<string>("key")},
				{"ByView", x=> x.Value<string>("view")},
				{"ByRef", x=> x.Value<string>("ref")},
				{"ByViewAndKey", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("key"))},
			});

			EtagSynchronization = Add(new Table(x => x.Value<string>("key"), "EtagSynchronization"));
		}

		public Table Lists { get; private set; }

		public Table Details { get; private set; }

		public Table Tasks { get; private set; }

		public Table Queues { get; private set; }

		public Table MappedResults { get; private set; }

		public Table ReduceResults { get; private set; }

		public Table ScheduleReductions { get; private set; }

		public Table IndexingStats { get; private set; }

		public Table LastIndexedEtags { get; private set; }

		public Table DocumentsModifiedByTransactions { get; private set; }

		public Table Documents { get; private set; }

		public Table Attachments { get; private set; }

		public Table Identity { get; private set; }

		public Table ReduceKeys { get; private set; }

		public Table DocumentReferences { get; private set; }

		public Table EtagSynchronization { get; private set; }
	}
}