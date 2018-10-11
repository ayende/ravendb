using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Esprima;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class QueryParser
    {
        private static readonly string[] OperatorStartMatches = { ">=", "<=", "<>", "<", ">", "==", "=", "!=", "BETWEEN", "IN", "ALL IN", "(" };
        private static readonly string[] BinaryOperators = { "OR", "AND" };
        private static readonly string[] StaticValues = { "true", "false", "null" };
        private static readonly string[] OrderByOptions = { "ASC", "DESC", "ASCENDING", "DESCENDING" };
        private static readonly string[] OrderByAsOptions = { "string", "long", "double", "alphaNumeric" };


        private int _counter;
        private int _depth;
        private NextTokenOptions _state = NextTokenOptions.Parenthesis;

        private int _statePos;

        public QueryScanner Scanner = new QueryScanner();
        private Dictionary<StringSegment, (StringSegment Collection, QueryExpression Filter, bool IsEdge, List<StringSegment> path)> _synteticWithQueries;

        public void Init(string q)
        {
            _depth = 0;
            Scanner.Init(q);
        }

        public Query Parse(QueryType queryType = QueryType.Select, bool recursive = false)
        {
            var q = new Query
            {
                QueryText = Scanner.Input
            };

            while (Scanner.TryScan("DECLARE"))
            {
                var (name, func) = DeclaredFunction();

                if (q.TryAddFunction(name, func) == false)
                    ThrowParseException(name + " function was declared multiple times");
            }

            while (Scanner.TryScan("WITH"))
            {
                if (recursive == true)
                    ThrowParseException("With clause is not allow inside inner query");

                WithClause(q);
            }

            if (Scanner.TryScan("MATCH") == false)
            {
                if(q.GraphQuery != null)
                    ThrowParseException("Missing a 'match' clause after 'with' caluse");

                q.From = FromClause();

                if (Scanner.TryScan("GROUP BY"))
                    q.GroupBy = GroupBy();

                if (Scanner.TryScan("WHERE") && Expression(out q.Where) == false)
                    ThrowParseException("Unable to parse WHERE clause");
            }
            else
            {
                if (q.GraphQuery == null)
                    q.GraphQuery = new GraphQuery();

                if(BinaryGraph(out var op) == false)
                    ThrowParseException("Unexpceted input when trying to parse the MATCH clause");
                q.GraphQuery.MatchClause = op;

                if(_synteticWithQueries != null)
                {
                    foreach (var (alias, sq) in _synteticWithQueries)
                    {
                        if (sq.IsEdge)
                        {
                            var with = new WithEdgesExpression(sq.Filter, sq.Collection, null,sq.path);
                            q.TryAddWithEdgePredicates(with, alias);
                        }
                        else
                        {
                             q.TryAddWithClause(new Query
                             {
                                 From = new FromClause
                                 {
                                     From = new FieldExpression(new List<StringSegment> { sq.Collection }),
                                 },
                                 Where = sq.Filter
                             }, alias);
                        }
                    }
                    _synteticWithQueries.Clear();
                }
            }

            if (Scanner.TryScan("ORDER BY"))
                q.OrderBy = OrderBy();

            if (Scanner.TryScan("LOAD"))
                q.Load = SelectClauseExpressions("LOAD", false);

            switch (queryType)
            {
                case QueryType.Select:
                    if (Scanner.TryScan("SELECT"))
                        q.Select = SelectClause("SELECT", q);
                    if (Scanner.TryScan("INCLUDE"))
                        q.Include = IncludeClause();
                    break;
                case QueryType.Update:
                    if (Scanner.TryScan("UPDATE") == false)
                        ThrowParseException("Update operations must end with UPDATE clause");

                    var functionStart = Scanner.Position;
                    if (Scanner.FunctionBody() == false)
                        ThrowParseException("Update clause must have a single function body");

                    q.UpdateBody = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
                    try
                    {
                        // validate the js code
                        ValidateScript("function test()" + q.UpdateBody);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidQueryException("Update clause contains invalid script", Scanner.Input, null, e);
                    }
                    break;
                default:
                    ThrowUnknownQueryType(queryType);
                    break;
            }

            if (recursive == false && Scanner.AtEndOfInput() == false)
                ThrowParseException("Expected end of query");

            return q;
        }

        private void WithClause(Query q)
        {
            if (Scanner.TryScan("EDGES"))
            {
                Scanner.Identifier();

                var (with, alias) = WithEdges();
                q.TryAddWithEdgePredicates(with, alias);
            }
            else
            {
                var (expr, alias) = With();
                q.TryAddWithClause(expr, alias);
            }
        }

        private StringSegment WithEdgeType()
        {
            StringSegment edgeType = null;
            if (Scanner.TryScan('('))
            {
                if (Scanner.Identifier() == true)
                {
                    edgeType = new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength);
                }
                if (Scanner.TryScan(')') == false)
                    ThrowParseException("With edges(<identifier>) was not closed with ')'");
            }

            return edgeType;
        }

        private (WithEdgesExpression Expression, StringSegment Allias) WithEdges()
        {
            StringSegment edgeType = WithEdgeType();

            if (Scanner.TryScan('{') == false)
            {
                if (Alias(true, out var shortAlias) && shortAlias.HasValue)
                {
                    var shortWithEdges = new WithEdgesExpression(null, edgeType, null);
                    return (shortWithEdges, shortAlias.Value);
                }

                throw new InvalidQueryException("With edges should be followed with '{' ", Scanner.Input, null);
            }

            QueryExpression qe = null;
            if (Scanner.TryScan("WHERE") && (Expression(out qe) == false|| qe == null))
                ThrowParseException("Unable to parse WHERE clause of an 'With Edges' clause");

            List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)> orderBy = null;
            if (Scanner.TryScan("ORDER BY"))
            {
                orderBy = OrderBy();
            }

            if (Scanner.TryScan('}') == false)
                throw new InvalidQueryException("With clause contains invalid body", Scanner.Input, null);

            if (Alias(true, out var alias) == false || alias.HasValue == false)
                throw new InvalidQueryException("With clause must contain allias but none was provided", Scanner.Input, null);

            var wee = new WithEdgesExpression(qe, edgeType, orderBy);
            return (wee, alias.Value);
        }

        public static readonly string[] EdgeOps = new[] { "<-", ">", "[", "-", "<", "(" };

        private bool GraphAlias(bool isEdge, out StringSegment alias)
        {
            var start = Scanner.Position;

            if (Scanner.Identifier() == false)
            {
                if (Scanner.TryPeek(':') == false)
                {
                    alias = default;
                    return false;
                }

                alias = "__alias" + (++_counter);
            }
            else
            {
                alias = new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength);
            }


            if (Scanner.TryScan(':'))
            {
                if (Scanner.Identifier() == false)
                    throw new InvalidQueryException("Failed to read collection after: " + alias, Scanner.Input, null);

                QueryExpression filter = null;
                var collection = new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength);
                var path = new List<StringSegment>();

                //filter expressions come before path (ex. -[:Lines(ProductName = 'Chang').Product]->)
                if (Scanner.TryScan('('))
                {
                    if(Expression(out filter) == false)
                        throw new InvalidQueryException("Failed to parse filter expression for: " + alias, Scanner.Input, null);

                    if (Scanner.TryScan(')') == false)
                        throw new InvalidQueryException("Failed to find closing ')' after filter expression for: " + alias, Scanner.Input, null);
                }

                //for cases like -[:Lines[].Product]->
                //the '[]' is essentially a syntax sugar, so simply skip it...
                if (isEdge && Scanner.TryPeek('['))
                {
                    Scanner.TryScan('[');
                    Scanner.TryScan(']');
                }

                if (isEdge && Scanner.TryScan('.')) //perhaps we have edge as a field of embedded object?
                                                    // Example : this is for cases like -[:Lines.Product.Foo.Bar]->
                                                    // TODO : refactor this to use object pool for List<StringSegment> or something similar
                {
                    while (Scanner.Identifier())
                    {
                        path.Add(new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength));
                        if (Scanner.TryPeek('(') || Scanner.TryPeek(']'))
                        {
                            break;
                        }

                        if (Scanner.TryScan('.'))
                            Scanner.NextPathSegment();
                    }
                }



                AddWithQuery(collection, alias, filter, isEdge, start, path);
            }
            return true;
        }

        private void AddWithQuery(StringSegment collection, StringSegment alias, QueryExpression filter, bool isEdge, int start, List<StringSegment> path = null)
        {
            if (_synteticWithQueries == null)
                _synteticWithQueries = new Dictionary<StringSegment, (StringSegment Collection, QueryExpression Filter, bool IsEdge, List<StringSegment> path)>(StringSegmentEqualityComparer.Instance);

            if(_synteticWithQueries.TryGetValue(alias, out var existing))
            {
                if (existing.IsEdge != isEdge)
                    ThrowDuplicateAliasWithoutSameBody(start);

                if (collection != existing.Collection)
                    ThrowDuplicateAliasWithoutSameBody(start);

                if((filter != null ) != (existing.Filter != null))
                    ThrowDuplicateAliasWithoutSameBody(start);

                if(filter != null && filter.Equals(existing.Filter) == false)
                    ThrowDuplicateAliasWithoutSameBody(start);

                return;
            }

            _synteticWithQueries.Add(alias, (collection, filter, isEdge, path));
        }

        private void ThrowDuplicateAliasWithoutSameBody(int start)
        {
            throw new InvalidQueryException("Duplicated alias  was found with a different definition than previous defined: " + 
                new StringSegment(Scanner.Input, start, Scanner.Position - start),
                 Scanner.Input, null);
        }


        private bool BinaryGraph(out QueryExpression op)
        {
            if (GraphOperation(out op) == false)
                return false;

            if (Scanner.TryScan(BinaryOperators, out var found) == false)
                return true; // found simple

            var negate = Scanner.TryScan("NOT");
            var type = found == "OR"
                ? OperatorType.Or
                : OperatorType.And;

            _state = NextTokenOptions.Parenthesis;

            var parenthesis = Scanner.TryPeek('(');

            if (BinaryGraph(out var right) == false)
                ThrowParseException($"Failed to find second part of {type} expression");

            return TrySimplifyBinaryExpression(right, type, negate, parenthesis, ref op);
        }

        private bool GraphOperation(out QueryExpression op)
        {
            if(Scanner.TryScan('(') == false)
                throw new InvalidQueryException("MATCH operator expected a '(', but didn't get it.", Scanner.Input, null);

            if(GraphAlias(isEdge: false, out var alias) == false)
            {
                if (BinaryGraph(out op) == false)
                {
                    throw new InvalidQueryException("Invalid expression in MATCH", Scanner.Input, null);
                }

                if (Scanner.TryScan(')') == false)
                    throw new InvalidQueryException("Missing ')' in MATCH", Scanner.Input, null);
                return true;
            }

            if (Scanner.TryScan(')') == false)
                throw new InvalidQueryException("MATCH operator expected a ')' after reading: " + alias, Scanner.Input, null);

            var list = new List<MatchPath>();

            list.Add(new MatchPath
            {
                Alias = alias,
                EdgeType = EdgeType.Right
            });


            bool foundDash = false;
            bool expectNode = false;
            MatchPath last;
            while (true)
            {
                if (Scanner.TryScan(EdgeOps, out var found))
                {
                    switch (found)
                    {
                        case "-":
                            foundDash = true;
                            continue;
                        case "[":
                            if (foundDash == false)
                                throw new InvalidQueryException("Got '[' when expected '-', did you forget to add '-[' ?", Scanner.Input, null);

                            if (GraphAlias(isEdge: true, out alias) == false)
                                throw new InvalidQueryException("MATCH identifider after '-['", Scanner.Input, null);
                            if (Scanner.TryScan(']') == false)
                                throw new InvalidQueryException("MATCH operator expected a ']' after reading: " + alias, Scanner.Input, null);

                            list.Add(new MatchPath
                            {
                                Alias = alias,
                                EdgeType = list[list.Count - 1].EdgeType,
                                IsEdge = true
                            });
                            break;
                        case "<-":
                            last = list[list.Count - 1];
                            list[list.Count - 1] = new MatchPath
                            {
                                Alias = last.Alias,
                                IsEdge = last.IsEdge,
                                EdgeType = EdgeType.Left
                            };
                            foundDash = true;
                            expectNode = true;
                            continue;
                        case "<":
                            throw new InvalidQueryException("Got unexpceted '<', did you forget to add '->' ?", Scanner.Input, null);
                        case ">":
                            if (foundDash == false)
                                throw new InvalidQueryException("Got '>' when expected '-', did you forget to add '->' ?", Scanner.Input, null);
                            last = list[list.Count - 1];
                            list[list.Count - 1] = new MatchPath
                            {
                                Alias = last.Alias,
                                IsEdge = last.IsEdge,
                                EdgeType = EdgeType.Right
                            };
                            if (Scanner.TryScan('(') == false)
                                throw new InvalidQueryException("MATCH operator expected a '(', but didn't get it.", Scanner.Input, null);
                            expectNode = true;

                            goto case "(";

                        case "(":
                            if (expectNode == false && list[list.Count - 1].EdgeType != EdgeType.Left)
                                throw new InvalidQueryException("Got '(', but it wasn't expected", Scanner.Input, null);

                            if (GraphAlias(isEdge: false, out alias) == false)
                                throw new InvalidQueryException("Couldn't get node's alias", Scanner.Input, null);

                            if (Scanner.TryScan(')') == false)
                                throw new InvalidQueryException("MATCH operator expected a ')' after reading: " + alias, Scanner.Input, null);

                            list.Add(new MatchPath
                            {
                                Alias = alias,
                                EdgeType = list[list.Count - 1].EdgeType
                            });
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Unknown edge type: " + found);
                    }

                    foundDash = false;
                    expectNode = false;
                }
                else
                {
                    op = FinalProcessingOfMatchExpression(list);

                    return true;
                }
            }
        }

        private static QueryExpression FinalProcessingOfMatchExpression(List<MatchPath> list)
        {
            bool hasIncoming = false, hasOutgoing = false;

            for (int i = 0; i < list.Count; i++)
            {
                hasIncoming |= list[i].EdgeType == EdgeType.Left;
                hasOutgoing |= list[i].EdgeType == EdgeType.Right;
            }

            QueryExpression op;
            if (hasIncoming)
            {
                if(hasOutgoing == false)
                {
                    ReverseIncomingChain(list);
                }
                else
                {
                    return BreakPatternChainToAndClasues(list, out op);
                }

            }

            op = new PatternMatchElementExpression
            {
                Path = list.ToArray(),
                Type = ExpressionType.Pattern
            };
            return op;
        }

        private static QueryExpression BreakPatternChainToAndClasues(List<MatchPath> list, out QueryExpression op)
        {
            // need to split the expression based on the direction of the arrows
            var clauses = new List<PatternMatchElementExpression>();
            for (int i = list.Count - 1; i >= 1; i--)
            {
                if (list[i].EdgeType != list[i - 1].EdgeType)
                {
                    var part = list.Skip(i ).ToList();
                    if (part.Last().EdgeType == EdgeType.Left)
                    {
                        ReverseIncomingChain(part);
                    }
                    else
                    {
                        part[0] = new MatchPath
                        {
                            Alias = part[0].Alias,
                            EdgeType = EdgeType.Left,
                            IsEdge = part[0].IsEdge
                        };
                    }
                    list[i] = new MatchPath
                    {
                        Alias = list[i].Alias,
                        EdgeType = list[i-1].EdgeType,
                        IsEdge = list[i].IsEdge
                    };
                    clauses.Add(new PatternMatchElementExpression
                    {
                        Path = part.ToArray(),
                        Type = ExpressionType.Pattern
                    });
                    list.RemoveRange(i+1, list.Count - i -1);
                }
            }

            if(list[0].EdgeType == EdgeType.Left)
            {
                ReverseIncomingChain(list);
            }
            clauses.Add(new PatternMatchElementExpression
            {
                Path = list.ToArray(),
                Type = ExpressionType.Pattern
            });

            op = clauses.Last();
            for (int i = clauses.Count - 2; i >= 0; i--)
            {
                op = new BinaryExpression(op, clauses[i], OperatorType.And);
            }
            return op;
        }

        private static void ReverseIncomingChain(List<MatchPath> list)
        {
            // reverse the path so it is always rightward skips
            list.Reverse();
            for (int i = 0; i < list.Count; i++)
            {
                var cur = list[i];
                list[i] = new MatchPath
                {
                    Alias = cur.Alias,
                    EdgeType = EdgeType.Right,
                    IsEdge = cur.IsEdge
                };
            }
        }

        private (Query Query, StringSegment Allias) With()
        {
            if (Scanner.TryScan('{') == false)
                throw new InvalidQueryException("With keyword should be followed with either 'edges' or '{' ", Scanner.Input, null);

            var query = Parse(recursive:true);

            if (Scanner.TryScan('}') == false)
                throw new InvalidQueryException("With clause contains invalid body", Scanner.Input, null);

            if (Alias(true, out var allias) == false || allias.HasValue == false)
                throw new InvalidQueryException("With clause must contain allias but none was provided", Scanner.Input, null);

            return (query, allias.Value);
        }

        private static Esprima.Ast.Program ValidateScript(string script)
        {
            var javaScriptParser = new JavaScriptParser(script);
            return javaScriptParser.ParseProgram();
        }

        private static void ThrowUnknownQueryType(QueryType queryType)
        {
            throw new ArgumentOutOfRangeException(nameof(queryType), queryType, "Unknown query type");
        }

        private List<QueryExpression> IncludeClause()
        {
            List<QueryExpression> includes = new List<QueryExpression>();

            do
            {
                if (Value(out var val))
                {
                    includes.Add(val);
                }
                else if (Field(out var field))
                {
                    QueryExpression expr;
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException("Expected method call in " + field);
                        expr = method;
                    }
                    else
                    {
                        expr = field;
                    }

                    includes.Add(expr);
                }
                else
                {
                    ThrowParseException("Unable to understand include clause expression");
                }
            } while (Scanner.TryScan(","));
            return includes;
        }

        private (StringSegment Name, (string FunctionText, Esprima.Ast.Program Program)) DeclaredFunction()
        {
            // because of how we are processing them, we don't actually care for
            // parsing the function directly. We have implemented a minimal parser
            // here that find the _boundary_ of the function call, and then we hand
            // all of that code directly to the js code. 

            var functionStart = Scanner.Position;

            if (Scanner.TryScan("function") == false)
                ThrowParseException("DECLARE clause found but missing 'function' keyword");

            if (Scanner.Identifier() == false)
                ThrowParseException("DECLARE functions require a name and cannot be anonymous");

            var name = new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength);

            // this reads the signature of the method: (a,b,c), etc.
            // we are technically allow more complex stuff there that isn't
            // allowed by JS, but that is fine, since the JS parser will break 
            // when it try it, so we are good with false positives here

            if (Scanner.TryScan('(') == false)
                ThrowParseException("Unable to parse function " + name + " signature");

            ReadMethodArguments();

            if (Scanner.FunctionBody() == false)
                ThrowParseException("Unable to get function body for " + name);

            var functionText = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
            // validate this function
            try
            {
                var program = ValidateScript(functionText);
                return (name, (functionText, program));
            }
            catch (Exception e)
            {
                throw new InvalidQueryException("Invalid script inside function " + name, Scanner.Input, null, e);
            }
        }

        private List<(QueryExpression Expression, StringSegment? Alias)> GroupBy()
        {
            var fields = new List<(QueryExpression Expression, StringSegment? Alias)>();
            do
            {
                QueryExpression op = null;
                if (Field(out var field))
                {
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException($"Unable to parse method call {field} for GROUP BY");
                        op = method;
                    }
                    else
                    {
                        op = field;
                    }
                }
                else if (Value(out var value))
                {
                    op = value;
                }
                else
                {
                    ThrowParseException("Unable to get field for GROUP BY");
                }

                Alias(false, out var alias);

                fields.Add((op, alias));

                if (Scanner.TryScan(",") == false)
                    break;

            } while (true);

            return fields;
        }

        private List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)> OrderBy()
        {
            var orderBy = new List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)>();
            do
            {
                if (Field(out var field) == false)
                    ThrowParseException("Unable to get field for ORDER BY");

                var type = OrderByFieldType.Implicit;

                QueryExpression op;
                if (Scanner.TryScan('('))
                {
                    if (Method(field, out var method) == false)
                        ThrowParseException($"Unable to parse method call {field} for ORDER BY");
                    op = method;
                }
                else
                {
                    op = field;
                }

                if (Scanner.TryScan("AS") && Scanner.TryScan(OrderByAsOptions, out var asMatch))
                {
                    switch (asMatch)
                    {
                        case "string":
                            type = OrderByFieldType.String;
                            break;
                        case "long":
                            type = OrderByFieldType.Long;
                            break;
                        case "double":
                            type = OrderByFieldType.Double;
                            break;
                        case "alphaNumeric":
                            type = OrderByFieldType.AlphaNumeric;
                            break;
                    }
                }

                var asc = true;

                if (Scanner.TryScan(OrderByOptions, out var match))
                {
                    if (match == "DESC" || match == "DESCENDING")
                        asc = false;
                }

                orderBy.Add((op, type, asc));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return orderBy;
        }

        private List<(QueryExpression, StringSegment?)> SelectClause(string clause, Query query)
        {
            query.IsDistinct = Scanner.TryScan("DISTINCT");

            if (Scanner.TryScan("*"))
                return null;

            var functionStart = Scanner.Position;
            if (Scanner.FunctionBody())
            {
                var functionText = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);

                // validate that this is valid JS code
                try
                {
                    var program = ValidateScript("return " + functionText);
                    query.SelectFunctionBody = (functionText, program);
                }
                catch (Exception e)
                {
                    throw new InvalidQueryException("Select clause contains invalid script", Scanner.Input, null, e);
                }

                return new List<(QueryExpression, StringSegment?)>();
            }

            return SelectClauseExpressions(clause, true);
        }

        private List<(QueryExpression, StringSegment?)> SelectClauseExpressions(string clause, bool aliasAsRequired)
        {
            var select = new List<(QueryExpression Expr, StringSegment? Alias)>();

            do
            {
                QueryExpression expr;
                if (Field(out var field))
                {
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException("Expected method call in " + clause);
                        expr = method;
                    }
                    else
                    {
                        expr = field;
                    }
                }
                else if (Value(out var v))
                {
                    expr = v;
                }
                else
                {
                    ThrowParseException("Unable to get field for " + clause);
                    return null; // never called
                }

                if (Alias(aliasAsRequired, out var alias) == false && expr is ValueExpression ve)
                {
                    alias = ve.Token;
                }

                @select.Add((expr, alias));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return @select;
        }


        private FromClause FromClause()
        {
            if (Scanner.TryScan("FROM") == false)
                ThrowParseException("Expected FROM clause");

            FieldExpression field;
            QueryExpression filter = null;
            bool index = false;
            if (Scanner.TryScan("INDEX"))
            {
                if (Field(out field) == false)
                    ThrowParseException("Expected FROM INDEX source");

                index = true;
            }
            else
            {

                if (Field(out field) == false)
                    ThrowParseException("Expected FROM source");

                if (Scanner.TryScan('(')) // FROM  Collection ( filter )
                {
                    if (Expression(out filter) == false)
                        ThrowParseException("Expected filter in filtered FORM clause");

                    if (Scanner.TryScan(')') == false)
                        ThrowParseException("Expected closing parenthesis in filtered FORM clause after filter");
                }
            }


            Alias(false, out var alias);

            return new FromClause
            {
                From = field,
                Alias = alias,
                Filter = filter,
                Index = index
            };
        }

        private static readonly string[] AliasKeywords =
        {
            "AS",
            "SELECT",
            "WHERE",
            "LOAD",
            "GROUP",
            "ORDER",
            "INCLUDE",
            "UPDATE"
        };

        private bool Alias(bool aliasAsRequired, out StringSegment? alias)
        {
            bool required = false;
            if (Scanner.TryScan(AliasKeywords, out var match))
            {
                required = true;
                if (match != "AS")
                {
                    // found a keyword
                    Scanner.GoBack(match.Length);
                    alias = null;
                    return false;
                }
            }
            if (aliasAsRequired && required == false)
            {
                alias = null;
                return false;
            }

            if (Field(out var token))
            {
                alias = token.FieldValue;
                return true;
            }

            if (required)
                ThrowParseException("Expected field alias after AS in SELECT");

            alias = null;
            return false;
        }

        internal bool Parameter(out int tokenStart, out int tokenLength)
        {
            if (Scanner.TryScan('$') == false)
            {
                tokenStart = 0;
                tokenLength = 0;
                return false;
            }

            Scanner.TokenStart = Scanner.Position;

            tokenStart = Scanner.TokenStart;

            if (Scanner.Identifier(false) == false)
                ThrowParseException("Expected parameter name");

            tokenLength = Scanner.TokenLength;
            return true;
        }

        internal bool Expression(out QueryExpression op)
        {
            if (++_depth > 128)
                ThrowQueryException("Query is too complex, over 128 nested clauses are not allowed");
            if (Scanner.Position != _statePos)
            {
                _statePos = Scanner.Position;
                _state = NextTokenOptions.Parenthesis;
            }
            var result = Binary(out op);
            _depth--;
            return result;
        }

        private bool Binary(out QueryExpression op)
        {
            switch (_state)
            {
                case NextTokenOptions.Parenthesis:
                    if (Parenthesis(out op) == false)
                        return false;
                    break;
                case NextTokenOptions.BinaryOp:
                    _state = NextTokenOptions.Parenthesis;
                    if (Operator(true, out op) == false)
                        return false;
                    break;
                default:
                    op = null;
                    return false;
            }


            if (Scanner.TryScan(BinaryOperators, out var found) == false)
                return true; // found simple

            var negate = Scanner.TryScan("NOT");
            var type = found == "OR"
                ? OperatorType.Or
                : OperatorType.And;

            _state = NextTokenOptions.Parenthesis;

            var parenthesis = Scanner.TryPeek('(');

            if (Binary(out var right) == false)
                ThrowParseException($"Failed to find second part of {type} expression");

            return TrySimplifyBinaryExpression(right, type, negate, parenthesis, ref op);
        }

        private bool TrySimplifyBinaryExpression(
            QueryExpression right,
            OperatorType type,
            bool negate, 
            bool parenthesis, 
            ref QueryExpression op)
        {
            if (parenthesis == false)
            {
                if (negate)
                {
                    right = NegateExpressionWithoutParenthesis(right);
                }

                // if the other arg isn't parenthesis, use operator precedence rules
                // to re-write the query
                switch (type)
                {
                    case OperatorType.And:
                        if (right is BinaryExpression rightOp)
                        {
                            switch (rightOp.Operator)
                            {
                                case OperatorType.Or:
                                case OperatorType.And:

                                    rightOp.Left = new BinaryExpression(op, rightOp.Left, type);
                                    op = right;
                                    return true;
                            }
                        }
                        break;
                }
            }
            else if (negate)
            {
                right = new NegatedExpression(right);
            }

            op = new BinaryExpression(op, right, type)
            {
                Parenthesis = parenthesis
            };

            return true;
        }

        private QueryExpression NegateExpressionWithoutParenthesis(QueryExpression expr)
        {
            bool ShouldRecurse(BinaryExpression e)
            {
                if (e.Parenthesis)
                    return false;

                return e.Operator == OperatorType.And ||
                       e.Operator == OperatorType.Or;
            }

            if (expr is BinaryExpression be && ShouldRecurse(be))
            {
                var result = be;

                while (be.Left is BinaryExpression nested && ShouldRecurse(nested))
                {
                    be = nested;
                }
                be.Left = new NegatedExpression(be.Left);

                return result;
            }
            return new NegatedExpression(expr);
        }

        private bool Parenthesis(out QueryExpression op)
        {
            if (Scanner.TryScan('(') == false)
            {
                _state = NextTokenOptions.BinaryOp;
                return Binary(out op);
            }

            if (Expression(out op) == false)
                return false;

            if (Scanner.TryScan(')') == false)
                ThrowParseException("Unmatched parenthesis, expected ')'");
            return true;
        }

        private bool Operator(bool fieldRequired, out QueryExpression op)
        {
            OperatorType type;
            FieldExpression field = null;

            if (Scanner.TryScan("true"))
            {
                op = new TrueExpression();
                return true;
            }
            else
            {
                if (fieldRequired && Field(out field) == false)
                {
                    op = null;
                    return false;
                }

                if (Scanner.TryScan(OperatorStartMatches, out var match) == false)
                {
                    if (fieldRequired == false)
                    {
                        op = null;
                        return false;
                    }
                    ThrowParseException("Invalid operator expected any of (In, Between, =, <, >, <=, >=)");
                }


                switch (match)
                {
                    case "<":
                        type = OperatorType.LessThan;
                        break;
                    case ">":
                        type = OperatorType.GreaterThan;
                        break;
                    case "<=":
                        type = OperatorType.LessThanEqual;
                        break;
                    case ">=":
                        type = OperatorType.GreaterThanEqual;
                        break;
                    case "=":
                    case "==":
                        type = OperatorType.Equal;
                        break;
                    case "!=":
                    case "<>":
                        type = OperatorType.NotEqual;
                        break;
                    case "BETWEEN":
                        if (Value(out var fst) == false)
                            ThrowParseException("parsing Between, expected value (1st)");
                        if (Scanner.TryScan("AND") == false)
                            ThrowParseException("parsing Between, expected AND");
                        if (Value(out var snd) == false)
                            ThrowParseException("parsing Between, expected value (2nd)");

                        if (fst.Type != snd.Type)
                            ThrowQueryException(
                                $"Invalid Between expression, values must have the same type but got {fst.Type} and {snd.Type}");

                        op = new BetweenExpression(field, fst, snd);
                        return true;
                    case "IN":
                    case "ALL IN":
                        if (Scanner.TryScan('(') == false)
                            ThrowParseException("parsing In, expected '('");

                        var list = new List<QueryExpression>();
                        do
                        {
                            if (Scanner.TryScan(')'))
                                break;

                            if (list.Count != 0)
                                if (Scanner.TryScan(',') == false)
                                    ThrowParseException("parsing In expression, expected ','");

                            if (Value(out var inVal) == false)
                                ThrowParseException("parsing In, expected a value");

                            if (list.Count > 0)
                                if (list[0].Type != inVal.Type)
                                    ThrowQueryException(
                                        $"Invalid In expression, all values must have the same type, expected {list[0].Type} but got {inVal.Type}");
                            list.Add(inVal);
                        } while (true);

                        op = new InExpression(field, list, match == "ALL IN");

                        return true;
                    case "(":
                        var isMethod = Method(field, out var method);
                        op = method;

                        if (isMethod && Operator(false, out var methodOperator))
                        {
                            if (methodOperator is BinaryExpression be)
                            {
                                be.Left = method;
                                op = be;
                                return true;
                            }

                            if (methodOperator is InExpression ie)
                            {
                                ie.Source = method;
                                op = ie;
                                return true;
                            }
                            if (methodOperator is BetweenExpression between)
                            {
                                between.Source = method;
                                op = between;
                                return true;
                            }
                            if (methodOperator is MethodExpression me)
                            {
                                op = me;
                                return true;
                            }
                            ThrowParseException("Unexpected operator after method call: " + methodOperator);
                        }

                        return isMethod;
                    default:
                        op = null;
                        return false;
                }
            }

            if (Value(out var val))
            {
                op = new BinaryExpression(field, val, type);
                return true;
            }
            if (Operator(true, out var op2))
            {
                op = new BinaryExpression(field, op2, type);
                return true;
            }
            op = null;
            return false;
        }

        private bool Method(FieldExpression field, out MethodExpression op)
        {
            var args = ReadMethodArguments();

            op = new MethodExpression(field.FieldValue, args);
            return true;
        }

        private List<QueryExpression> ReadMethodArguments()
        {
            var args = new List<QueryExpression>();
            do
            {
                if (Scanner.TryScan(')'))
                    break;

                if (args.Count != 0)
                    if (Scanner.TryScan(',') == false)
                        ThrowParseException("parsing method expression, expected ','");

                var maybeExpression = false;
                if (Value(out var argVal))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(argVal.Token.Offset - 1); // if this was a value then it had to be in ''
                        maybeExpression = true;
                    }
                    else
                    {
                        args.Add(argVal);
                        continue;
                    }
                }

                if (maybeExpression == false && Field(out var fieldRef))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(fieldRef.Compound[0].Offset);
                    }
                    else
                    {
                        args.Add(fieldRef);
                        continue;
                    }
                }

                if (Expression(out var expr))
                    args.Add(expr);
                else
                    ThrowParseException("parsing method, expected an argument");
            } while (true);
            return args;
        }

        private void ThrowParseException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Line)
                .Append(":")
                .Append(Scanner.Column)
                .Append(" ")
                .Append(msg)
                .Append(" but got");

            if (Scanner.NextToken())
                sb.Append(": ")
                    .Append(Scanner.CurrentToken);
            else
                sb.Append(" to the end of the query");


            sb.AppendLine();
            sb.AppendLine("Query: ");
            sb.Append(Scanner.Input);

            throw new ParseException(sb.ToString());
        }

        private void ThrowQueryException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Column)
                .Append(":")
                .Append(Scanner.Line)
                .Append(" ")
                .Append(msg);

            throw new ParseException(sb.ToString());
        }

        private bool Value(out ValueExpression val)
        {
            var numberToken = Scanner.TryNumber();
            if (numberToken != null)
            {
                val = new ValueExpression(
                    new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength),
                    numberToken.Value == NumberToken.Long ? ValueTokenType.Long : ValueTokenType.Double
                );
                return true;
            }
            if (Scanner.String(out var token))
            {
                val = new ValueExpression(
                    token,
                    ValueTokenType.String
                );
                return true;
            }
            if (Scanner.TryScan(StaticValues, out var match))
            {
                ValueTokenType type;
                switch (match)
                {
                    case "true":
                        type = ValueTokenType.True;
                        break;
                    case "false":
                        type = ValueTokenType.False;
                        break;
                    case "null":
                        type = ValueTokenType.Null;
                        break;
                    default:
                        type = ValueTokenType.String;
                        break;
                }

                val = new ValueExpression(
                    new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength),
                    type);
                return true;
            }

            if (Parameter(out int tokenStart, out int tokenLength))
            {
                val = new ValueExpression(
                    new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength),
                    ValueTokenType.Parameter
                );
                return true;
            }
            val = null;
            return false;
        }

        internal bool Field(out FieldExpression token)
        {
            var part = 0;

            var parts = new List<StringSegment>(1);
            bool quoted = false;
            while (true)
            {
                if (Scanner.Identifier(beginning: part++ == 0) == false)
                {
                    if (Scanner.String(out var str))
                    {
                        if (part == 1)
                            quoted = true;
                        parts.Add(str);
                    }
                    else
                    {
                        token = null;
                        return false;
                    }
                }
                else
                {
                    parts.Add(new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength));
                }
                if (part == 1)
                {
                    // need to ensure that this isn't a keyword
                    if (Scanner.CurrentTokenMatchesAnyOf(AliasKeywords))
                    {
                        Scanner.GoBack(Scanner.TokenLength);
                        token = null;
                        return false;
                    }
                }

                bool? hasNextPart = null;

                while (Scanner.TryScan('['))
                {
                    switch (Scanner.TryNumber())
                    {
                        case NumberToken.Long:
                            if (Scanner.TryScan(']') == false)
                                ThrowParseException("Expected to find closing ]");
                            parts.Add(new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength));
                            break;

                        case null:
                            if (Scanner.TryScan(']') == false)
                                ThrowParseException("Expected to find closing ]");
                            parts.Add("[]");

                            break;
                        case NumberToken.Double:
                            ThrowParseException("Array indexer must be integer, but got double");
                            break;
                    }

                    hasNextPart = Scanner.TryScan('.');
                }

                if (hasNextPart == true)
                    continue;

                if (Scanner.TryScan('.') == false)
                    break;
            }


            token = new FieldExpression(parts)
            {
                IsQuoted = quoted
            };

            return true;
        }

        private enum NextTokenOptions
        {
            Parenthesis,
            BinaryOp
        }

        public class ParseException : Exception
        {
            public ParseException(string msg) : base(msg)
            {
            }
        }
    }
}
