// -----------------------------------------------------------------------
//  <copyright file="RDBQA_13.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Tests.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_13 : RavenTest
    {
        [Fact]
        public void CanPatchWithNullPrevVal()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Name = "marcin"
                    }, "users/1");
                    s.SaveChanges();
                }

                var request = ((ServerClient) store.DatabaseCommands).CreateRequest("PATCH",
                                                                                    "/docs/users/1");
                request.Write("[{ Type: 'Set', Name: 'Age', Value: 10, PrevVal: null}]");

                request.ExecuteRequest();
                using (var s = store.OpenSession())
                {
                    var user = s.Load<User>("users/1");
                    Assert.Equal(10, user.Age);
                }
            }
        }


    }

}