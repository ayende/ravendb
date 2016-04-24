// -----------------------------------------------------------------------
//  <copyright file="AaronSt.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using System.Net;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Listeners;
using Raven.Client;

namespace Raven.Tests.MailingList
{
    public class ValerioBorioni2 : RavenTest
    {
        [Fact]
        public void saveChanges_listener_can_fail_before_saving()
        {
            string entityId = "myEntity/1";

            var failingBeforeListener = new FailingBeforeSaveChangesListener();

            using (var store = NewDocumentStore())
            {
                store.RegisterListener(failingBeforeListener);

                using (var session = store.OpenSession())
                {
                    session.Store(new MyEntity { Id = entityId });
                    Assert.Throws<NotImplementedException>(() => session.SaveChanges());
                }
            };
        }

        [Fact]
        public void saveChanges_listener_can_fail_after_saving()
        {
            string entityId = "myEntity/1";

            var failingAfterListener = new FailingAfterSaveChangesListener();

            using (var store = NewDocumentStore())
            {
                store.RegisterListener(failingAfterListener);

                using (var session = store.OpenSession())
                {
                    session.Store(new MyEntity { Id = entityId });
                    Assert.Throws<NotImplementedException>(() => session.SaveChanges());
                }

                using (var session = store.OpenSession())
                {
                    var entity = session.Query<MyEntity>().Customize(r => r.WaitForNonStaleResults()).Single();
                    Assert.Equal(entityId, entity.Id);
                }
            };

        }

        public class MyEntity
        {
            public string Id { get; set; }
            public string Body { get; set; }

            public MyEntity()
            {
                Body = "";
            }
        }

        public class FailingBeforeSaveChangesListener : IDocumentSaveChangesListener
        {
            public void BeforeSaveChanges(IDocumentSession session)
            {
                throw new NotImplementedException();
            }

            public void AfterSaveChanges(IDocumentSession session)
            {
               
            }

        }
        public class FailingAfterSaveChangesListener : IDocumentSaveChangesListener
        {
            public void BeforeSaveChanges(IDocumentSession session)
            {

            }

            public void AfterSaveChanges(IDocumentSession session)
            {
                throw new NotImplementedException();
            }

        }

    }

}
