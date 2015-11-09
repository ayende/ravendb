// -----------------------------------------------------------------------
//  <copyright file="ClusterConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Raft.Dto
{
    public class ClusterConfiguration
    {
        public ClusterConfiguration()
        {
            EnableReplication = true;
        }

        public bool EnableReplication { get; set; }
    }
}
