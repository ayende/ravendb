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

namespace Raven.Tests.Patching
{
    public class AdvancedPatching : RavenTest
    {        
        [Fact]
        public void CanApplyBasicScriptAsPatch()
        {
            var test = new CustomType
            {
                id = "someId",
                value = 12143,
                comments = new List<string>(new[] { "one", "two", "seven" })
            };
            
            //splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
            var sampleScript = @"this.id = 'Something new'; 
this.value++; 
this.comments.splice(2, 1);
this.comments.Map(function(comment) { 
if (comment == ""one"")
    return comment + "" test"" 
else
    return comment;
});";

            var resultJson = new AdvancedJsonPatcher(RavenJObject.FromObject(test)).Apply(sampleScript);

            var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

            Assert.Equal("Something new", result.id);
            Assert.Equal(2, result.comments.Count);
            Assert.Equal("one test", result.comments[0]);
            Assert.Equal("two", result.comments[1]);
            Assert.Equal(12144, result.value);
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

        //[Fact]
        //public void CanPerformAdvancedPatching_Embedded()
        //{
        //    using (var store = NewDocumentStore())
        //    {
        //        ExecuteTest(store);
        //    }
        //}

        //private void ExecuteTest(IDocumentStore store)
        //{
        //    var patchedDoc = new AdvancedJsonPatcher(doc).Apply("");
        //}

        class CustomType
        {
            public string id { get; set; }
            public int value { get; set; }
            public List<string> comments { get; set; }
        }
    }
}
