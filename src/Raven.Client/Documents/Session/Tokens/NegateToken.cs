﻿using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class NegateToken : QueryToken
    {
        private NegateToken()
        {
        }

        public static readonly NegateToken Instance = new NegateToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("not");
        }
    }
}
