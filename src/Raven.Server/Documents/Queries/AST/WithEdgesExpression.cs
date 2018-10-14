﻿using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class WithEdgesExpression : QueryExpression
    {
        public QueryExpression Where;

        public StringSegment? EdgeType;

        public StringSegment? FromAlias;

        public StringSegment Path;

        public QueryParser.EdgePathType EdgePathType;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;

        public WithEdgesExpression(QueryExpression @where, string edgeType, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy, StringSegment path = default ,QueryParser.EdgePathType edgePathType = QueryParser.EdgePathType.EdgeProperty)
        {
            Where = @where;
            OrderBy = orderBy;
            //null edges means all edges 
            EdgeType = edgeType;
            Type = ExpressionType.WithEdge;
            EdgePathType = edgePathType;
            Path = path;
        }

        public override string ToString() => GetText();
        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            var sb = new StringBuilder("WITH EDGES");
            sb.Append("(").Append(EdgeType).Append(")");

            var visitor = new StringQueryVisitor(sb);

            if (Where != null)
            {
                visitor.VisitWhereClause(Where);
            }

            if (OrderBy != null)
            {
                visitor.VisitOrderBy(OrderBy);
            }

            return sb.ToString();
        }

       
        public override bool Equals(QueryExpression other)
        {
            if (!(other is WithEdgesExpression ie))
                return false;

            if (EdgeType != ie.EdgeType)
                return false;

            if ((Where != null) != (ie.Where != null) || 
                (OrderBy != null) != (ie.OrderBy != null))
                return false;

            if (Where != null && Where.Equals(ie.Where) == false)
                return false;

            if(OrderBy != null)
            {
                if (OrderBy.Count != ie.OrderBy.Count)
                    return false;

                for (int i = 0; i < OrderBy.Count; i++)
                {
                    if (OrderBy[i].Equals(ie.OrderBy.Count) == false)
                        return false;
                }
            }

            return true;
        }
    }
}
