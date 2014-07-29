// -----------------------------------------------------------------------
//  <copyright file="DocumentStoreConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.Document
{
	public class DocumentStoreConfiguration
	{
		public FailoverBehavior? FailoverBehavior { get; set; }
	}
}