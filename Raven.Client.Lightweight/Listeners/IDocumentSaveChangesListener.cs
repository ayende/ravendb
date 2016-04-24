//-----------------------------------------------------------------------
// <copyright file="IDocumentStoreListener.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Client.Listeners
{
    /// <summary>
    /// Hook for users to provide additional logic on store operations
    /// </summary>
    /// 
    public interface IDocumentSaveChangesListener
    {
        void BeforeSaveChanges(IDocumentSession session);

        void AfterSaveChanges(IDocumentSession session);
    }
}
