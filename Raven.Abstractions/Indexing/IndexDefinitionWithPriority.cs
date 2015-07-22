using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Indexing
{
	public class IndexDefinitionWithPriority
	{
		public IndexDefinition Definition { get; set; }
		public IndexingPriority Priority { get; set; }
		public string Name { get; set; }

	}
}
