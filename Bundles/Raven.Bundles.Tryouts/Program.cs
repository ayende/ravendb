//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Bundles.Tests.Expiration;
using Raven.Bundles.Tests.Replication;
using Raven.Bundles.Tests.Replication.Bugs;

namespace Raven.Bundles.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var wr = WebRequest.Create("http://localhost:8080");
			wr.Credentials = CredentialCache.DefaultCredentials;
			wr.GetResponse();
		}
	}
}
