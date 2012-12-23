// -----------------------------------------------------------------------
//  <copyright file="From40To41.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From42To43 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "4.2"; }
		}

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			using (var documents = new Table(session, dbid, "documents", OpenTableGrbit.None))
			{
				// my esent skills are non-existant...  How to get the column id?  how/where to loop through the records?  how to "pulse the transaction"?
				throw new NotImplementedException("Matt is a dummy when it comes to esent.  Fix me please!");

				//JET_COLUMNID oldLastModifiedColumnId = ???
				//var lastModified = Api.RetrieveColumnAsDateTime(session, documents, oldLastModifiedColumnId).Value;
				
				//Api.JetDeleteColumn(session, documents, "last_modified");

				//JET_COLUMNID newLastModifiedColumnId;
				//Api.JetAddColumn(session, documents, "last_modified",
				//				 new JET_COLUMNDEF
				//				 {
				//					 cbMax = 64,
				//					 coltyp = JET_coltyp.Binary,
				//					 grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
				//				 }, null, 0, out newLastModifiedColumnId);

				//Api.SetColumn(session, documents, newLastModifiedColumnId, lastModified.ToBinary());
			}

			SchemaCreator.UpdateVersion(session, dbid, "4.3");
		}
	}
}
