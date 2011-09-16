using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Raven.Client.Document;
using Raven.Client;
using Raven.Json.Linq;
using Raven.Database.Json;
using Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;

namespace Raven.Tests.Patching
{
	public class AdvancedPatching : RavenTest
	{        
		CustomType test = new CustomType
			{
				Id = "someId",
				Value = 12143,
				Comments = new List<string>(new[] { "one", "two", "seven" })
			};

		//splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
		string sampleScript = @"this.Id = 'Something new'; 
this.Value++; 
this.Comments.splice(2, 1);
this.newValue = ""err!!"";
this.Comments.Map(function(comment) {   
return (comment == ""one"") ? comment + "" test"" : comment;
});";

		[Fact]
		public void CanApplyBasicScriptAsPatch()
		{                        			
			var resultJson = new AdvancedJsonPatcher(RavenJObject.FromObject(test)).Apply(sampleScript);
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			Assert.Equal("Something new", result.Id);
			Assert.Equal(2, result.Comments.Count);
			Assert.Equal("one test", result.Comments[0]);
			Assert.Equal("two", result.Comments[1]);
			Assert.Equal(12144, result.Value);
			Assert.Equal("err!!", resultJson["newValue"]);
		}

		//[Fact]
		//public void CanPerformAdvancedPatching_Remotely()
		//{
		//    using (GetNewServer())
		//    using (var store = new DocumentStore
		//    {
		//        Url = "http://localhost:8080"
		//    }.Initialize())
		//    {
		//        ExecuteTest(store);
		//    }
		//}

		[Fact]
		public void CanPerformAdvancedPatching_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteTest(store);
			}
		}

		private void ExecuteTest(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				s.Store(test);
				s.SaveChanges();
			}
			
			store.DatabaseCommands.Patch(test.Id, sampleScript);

			///TODO this is wierd, we can change the Id in the Json to something other than the Key
			/// so we end up with a do that we can load via "someId" but result.Id = "Something new"
			/// we need to make sure the javascript can't change the Id field, or something else!??!
			var resultJson = store.DatabaseCommands.Get(test.Id).DataAsJson;
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			Assert.Equal("Something new", result.Id);
			Assert.Equal(2, result.Comments.Count);
			Assert.Equal("one test", result.Comments[0]);
			Assert.Equal("two", result.Comments[1]);
			Assert.Equal(12144, result.Value);
			Assert.Equal("err!!", resultJson["newValue"]);			
		}

		class CustomType
		{
			public string Id { get; set; }
			public int Value { get; set; }
			public List<string> Comments { get; set; }
		}
	}
}
