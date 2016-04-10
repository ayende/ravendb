using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace FastTests.Client.Conventions
{
    public class DateTimeZoneHandlingOverride
    {
        [Fact]
        public void ShouldBlockDateTimeZoneHandlingEditing()
         {
             var conventions = new DocumentConvention
             {
                 CustomizeJsonSerializer = serializer =>
                 {
                     serializer.DateTimeZoneHandling = DateTimeZoneHandling.Utc;
                 }
             };
 
             Assert.Throws<NotSupportedException>(() => conventions.CreateSerializer());
         }

    }
}
