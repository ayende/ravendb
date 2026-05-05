using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Server;
using Voron;
using Voron.Data.PostingLists;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public enum SearchQueryOptions
    {
        Legacy,
        PhraseQuery,
        PhraseQueryWithWildcardAdjustments
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch SearchQuery(in FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, SearchQueryOptions searchQueryOptions = SearchQueryOptions.PhraseQueryWithWildcardAdjustments, in CancellationToken cancellationToken = default)
    {
        return searchQueryOptions switch
        {
            SearchQueryOptions.Legacy => SearchQueryLegacy(field, values, @operator, cancellationToken),
            SearchQueryOptions.PhraseQueryWithWildcardAdjustments =>
                SearchQueryWithPhraseQueryWithWildcardQueriesAdjustments(field, values, @operator, cancellationToken),
            SearchQueryOptions.PhraseQuery => SearchQueryWithPhraseQuery(field, values, @operator, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(searchQueryOptions))
        };
    }

    private IQueryMatch SearchQueryLegacy(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken)
    {
        AssertFieldIsSearched();
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString())
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);

        Analyzer wildcardAnalyzer = null;
        IQueryMatch searchQuery = null;
        // BitmapMatch holds a struct RoaringBitmap with internal NativeList<>'s. Using
        // Nullable<BitmapMatch> would route .Value through a temporary copy, severing
        // mutations to the bitmap's count/index fields. Keep an eagerly-allocated
        // instance plus a flag tracking whether anything was accumulated.
        var searchBitmap = new BitmapMatch(Allocator);
        bool searchBitmapHasValue = false;
        Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData = new(Allocator);

        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);
        foreach (var word in values)
        {
            foreach (var token in GetTokens(word))
            {
                var value = word.AsSpan(token.Offset, (int)token.Length);
                var termType = GetTermType(value);
                (int startIncrement, int lengthIncrement, Analyzer analyzer) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0, searchAnalyzer),
                    Constants.Search.SearchMatchOptions.Exists => (0, 0, searchAnalyzer),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                var termReadyToAnalyze = value.Slice(startIncrement, value.Length - startIncrement + lengthIncrement);

                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= new();

                    terms.Clear(); // Clear the terms list.
                    EncodeAndApplyAnalyzerForMultipleTerms(field, termReadyToAnalyze, ref terms);
                    foreach (var term in terms.GetEnumerator())
                    {
                        if (term.Size == 0)
                            continue; //skip empty results

                        termMatches.Add(term);
                    }
                    continue;
                }

                Slice analyzedTerm = default;

                if (termType is not Constants.Search.SearchMatchOptions.Exists)
                {
                    analyzedTerm = EncodeAndApplyAnalyzer(field, analyzer, termReadyToAnalyze);
                    if (analyzedTerm.Size == 0)
                        continue; //skip empty results
                }

                var query = termType switch
                {
                    Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException(
                        $"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
                    Constants.Search.SearchMatchOptions.Exists => ExistsQuery(field, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, analyzedTerm, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, analyzedTerm, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, analyzedTerm, token: cancellationToken),
                    _ => throw new ArgumentOutOfRangeException(nameof(termType), termType.ToString())
                };

                searchBitmapHasValue = true;
                if (@operator == Constants.Search.Operator.Or)
                    Primitives.QueryPrimitives.FillFromMatch(query, ref searchBitmap.BitmapState, Allocator);
                else
                    Primitives.QueryPrimitives.AndWithMatch(query, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);
            }
        }

        if (termMatches?.Count > 0)
        {
            // Build bitmap directly from term posting lists instead of calling AllInQuery/InQuery
            var termBitmap = new BitmapMatch(Allocator);
            var tempTermBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);

            if (@operator == Constants.Search.Operator.And)
            {
                // AND all terms together
                bool first = true;
                foreach (var term in termMatches)
                {
                    var termQuery = TermQuery(field, term);
                    if (first)
                    {
                        Primitives.QueryPrimitives.FillFromMatch(termQuery, ref termBitmap.BitmapState, Allocator);
                        first = false;
                    }
                    else
                    {
                        Primitives.QueryPrimitives.AndWithMatch(termQuery, ref termBitmap.BitmapState, ref tempTermBitmapData, Allocator);
                    }
                }
            }
            else
            {
                // OR all terms together
                foreach (var term in termMatches)
                {
                    var termQuery = TermQuery(field, term);
                    Primitives.QueryPrimitives.FillFromMatch(termQuery, ref termBitmap.BitmapState, Allocator);
                }
            }

            searchBitmapHasValue = true;
            if (@operator == Constants.Search.Operator.Or)
                Primitives.QueryPrimitives.FillFromMatch((IQueryMatch)termBitmap, ref searchBitmap.BitmapState, Allocator);
            else
                Primitives.QueryPrimitives.AndWithMatch((IQueryMatch)termBitmap, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);

            tempTermBitmapData.Dispose();
        }

        if (searchBitmapHasValue)
            searchQuery = searchBitmap;


        void AssertFieldIsSearched()
        {
            if (field.Analyzer == null && field.IsDynamic == false)
                throw new InvalidOperationException($"{nameof(SearchQuery)} requires analyzer.");
        }

        wildcardAnalyzer?.Dispose();
        tempBitmapData.Dispose();

        return searchQuery ?? TermMatch.CreateEmpty(this, Allocator);

        IEnumerable<Token> GetTokens(string source)
        {
            if (string.IsNullOrEmpty(source))
            {
                yield return new Token() {Offset = 0, Length = 0};
                yield break;
            }
            
            //TODO This code in from `WhitespaceTokenizer`. We can optimize it later but for now it should be OK.
            int i = 0;

            while (i < source.Length)
            {
                while (i < source.Length && source[i] == ' ')
                    i++;

                int start = i;
                while (i < source.Length && source[i] != ' ')
                    i++;

                if (start != i)
                {
                    yield return new Token() {Offset = start, Length = (uint)(i - start), Type = TokenType.Word};
                }
            } 
        }
        
    }

    
    private IQueryMatch SearchQueryWithPhraseQuery(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken = default)
    {
        AssertFieldIsSearched();
        var searchAnalyzer = field.IsDynamic
            ? _fieldMapping.SearchAnalyzer(field.FieldName.ToString()) 
            : field.Analyzer;

        field = field.ChangeAnalyzer(field.Mode, searchAnalyzer);

        Analyzer wildcardAnalyzer = null;
        IQueryMatch searchQuery = null;
        // BitmapMatch holds a struct RoaringBitmap with internal NativeList<>'s. Using
        // Nullable<BitmapMatch> would route .Value through a temporary copy, severing
        // mutations to the bitmap's count/index fields. Keep an eagerly-allocated
        // instance plus a flag tracking whether anything was accumulated.
        var searchBitmap = new BitmapMatch(Allocator);
        bool searchBitmapHasValue = false;
        Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData = new(Allocator);

        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);
        foreach (var word in values)
        {
            var tokensInWord = CountTokens(word, out var token);

            if (tokensInWord == 0)
                continue;

            //Single word
            if (tokensInWord == 1)
            {
                var value = word.AsSpan(token.Offset, (int)token.Length);
                var termType = GetTermType(value);
                (int startIncrement, int lengthIncrement, Analyzer analyzer) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1, CreateWildcardAnalyzer(field, ref wildcardAnalyzer)),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0, searchAnalyzer),
                    Constants.Search.SearchMatchOptions.Exists => (0, 0, searchAnalyzer),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };

                var termReadyToAnalyze = value.Slice(startIncrement, value.Length - startIncrement + lengthIncrement);

                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= new();
                    terms.Clear(); // Clear the terms list.
                    EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);

                    //When single term outputs multiple terms we've to jump into phraseQuery
                    if (terms.Count > 1)
                        goto PhraseQuery;

                    foreach (var term in terms.GetEnumerator())
                    {
                        if (term.Size == 0)
                            continue; //skip empty results

                        termMatches.Add(term);
                    }
                    continue;
                }

                Slice analyzedTerm = default;

                if (termType is not Constants.Search.SearchMatchOptions.Exists)
                {
                    analyzedTerm = EncodeAndApplyAnalyzer(field, analyzer, termReadyToAnalyze);
                    if (analyzedTerm.Size == 0)
                        continue; //skip empty results
                }

                var query = termType switch
                {
                    Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException(
                        $"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
                    Constants.Search.SearchMatchOptions.Exists => ExistsQuery(field, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, analyzedTerm, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, analyzedTerm, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, analyzedTerm, token: cancellationToken),
                    _ => throw new ArgumentOutOfRangeException(nameof(termType), termType.ToString())
                };

                searchBitmapHasValue = true;
                if (@operator == Constants.Search.Operator.Or)
                    Primitives.QueryPrimitives.FillFromMatch(query, ref searchBitmap.BitmapState, Allocator);
                else
                    Primitives.QueryPrimitives.AndWithMatch(query, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);

                continue;
            }

            //Phrase query
            terms.Clear();
            EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);

            if (terms.Count == 0)
                continue; //sentence contained only stop-words
            PhraseQuery:
            // Build bitmap directly from term posting lists for phrase query
            var phraseBitmap = new BitmapMatch(Allocator);
            var tempPhraseBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);
            bool firstPhraseTerm = true;
            foreach (var term in terms.GetEnumerator())
            {
                var termQuery = TermQuery(field, term);
                if (firstPhraseTerm)
                {
                    Primitives.QueryPrimitives.FillFromMatch(termQuery, ref phraseBitmap.BitmapState, Allocator);
                    firstPhraseTerm = false;
                }
                else
                {
                    Primitives.QueryPrimitives.AndWithMatch(termQuery, ref phraseBitmap.BitmapState, ref tempPhraseBitmapData, Allocator);
                }
            }

            var phraseMatch = PhraseQuery(phraseBitmap, field, terms.ToSpan());
            tempPhraseBitmapData.Dispose();

            searchBitmapHasValue = true;
            if (@operator == Constants.Search.Operator.Or)
                Primitives.QueryPrimitives.FillFromMatch(phraseMatch, ref searchBitmap.BitmapState, Allocator);
            else
                Primitives.QueryPrimitives.AndWithMatch(phraseMatch, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);
        }

        if (termMatches?.Count > 0)
        {
            // Build bitmap directly from term posting lists instead of calling AllInQuery/InQuery
            var termBitmap = new BitmapMatch(Allocator);
            var tempTermBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);

            if (@operator == Constants.Search.Operator.And)
            {
                // AND all terms together
                bool first = true;
                foreach (var term in termMatches)
                {
                    var termQuery = TermQuery(field, term);
                    if (first)
                    {
                        Primitives.QueryPrimitives.FillFromMatch(termQuery, ref termBitmap.BitmapState, Allocator);
                        first = false;
                    }
                    else
                    {
                        Primitives.QueryPrimitives.AndWithMatch(termQuery, ref termBitmap.BitmapState, ref tempTermBitmapData, Allocator);
                    }
                }
            }
            else
            {
                // OR all terms together
                foreach (var term in termMatches)
                {
                    var termQuery = TermQuery(field, term);
                    Primitives.QueryPrimitives.FillFromMatch(termQuery, ref termBitmap.BitmapState, Allocator);
                }
            }

            searchBitmapHasValue = true;
            if (@operator == Constants.Search.Operator.Or)
                Primitives.QueryPrimitives.FillFromMatch((IQueryMatch)termBitmap, ref searchBitmap.BitmapState, Allocator);
            else
                Primitives.QueryPrimitives.AndWithMatch((IQueryMatch)termBitmap, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);

            tempTermBitmapData.Dispose();
        }

        if (searchBitmapHasValue)
            searchQuery = searchBitmap;


        void AssertFieldIsSearched()
        {
            if (field.Analyzer == null && field.IsDynamic == false)
                throw new InvalidOperationException($"{nameof(SearchQueryWithPhraseQuery)} requires analyzer.");
        }

        wildcardAnalyzer?.Dispose();
        tempBitmapData.Dispose();

        return searchQuery ?? TermMatch.CreateEmpty(this, Allocator);
        
        //In pharse query we expect to have multiple tokens, for most cases 
        int CountTokens(in string source, out Token termToken)
        {
            int count = 0;
            termToken = default;

            if (string.IsNullOrEmpty(source))
                return count;
            
            var i = 0;
            while (i < source.Length)
            {
                while (i < source.Length && source[i] == ' ')
                    i++;

                int start = i;
                while (i < source.Length && source[i] != ' ')
                    i++;

                if (start != i)
                {
                    termToken = new Token() {Length = (uint)(i - start), Offset = start, Type = TokenType.Word};
                    count++;
                }
            }

            return count;
        }
    }
    
    private IQueryMatch SearchQueryWithPhraseQueryWithWildcardQueriesAdjustments(FieldMetadata field, IEnumerable<string> values, Constants.Search.Operator @operator, in CancellationToken cancellationToken = default)
    {
        AssertFieldIsSearched();
        IQueryMatch searchQuery = null;
        // BitmapMatch holds a struct RoaringBitmap with internal NativeList<>'s. Using
        // Nullable<BitmapMatch> would route .Value through a temporary copy, severing
        // mutations to the bitmap's count/index fields. Keep an eagerly-allocated
        // instance plus a flag tracking whether anything was accumulated.
        var searchBitmap = new BitmapMatch(Allocator);
        bool searchBitmapHasValue = false;
        Voron.Data.RoaringBitmaps.RoaringBitmap tempBitmapData = new(Allocator);
        List<Slice> termMatches = null;
        var terms = new ContextBoundNativeList<Slice>(Allocator);

        foreach (var word in values)
        {
            terms.Clear();
            var termType = GetTermType(word);
            EncodeAndApplyAnalyzerForMultipleTerms(field, word, ref terms);
            var tokensInWord = terms.Count;
            
            if (tokensInWord == 0)
                continue;

            //single word
            if (tokensInWord is 1 || termType is Constants.Search.SearchMatchOptions.StartsWith)
            {
                var value = terms[0];
                var valueAsSpan = value.AsSpan();

                //Adjustment to Lucene builder.
                if (termType is not Constants.Search.SearchMatchOptions.StartsWith)
                    termType = GetTermType(valueAsSpan);
                    
                (int startIncrement, int lengthIncrement) = termType switch
                {
                    Constants.Search.SearchMatchOptions.StartsWith when valueAsSpan[^1] != '*' => (0, 0),
                    Constants.Search.SearchMatchOptions.StartsWith => (0, -1),
                    Constants.Search.SearchMatchOptions.EndsWith => (1, 0),
                    Constants.Search.SearchMatchOptions.Contains => (1, -1),
                    Constants.Search.SearchMatchOptions.TermMatch => (0, 0),
                    Constants.Search.SearchMatchOptions.Exists => (0, 0),
                    _ => throw new InvalidExpressionException("Unknown flag inside Search match.")
                };
                
                //Rewrite term without asterisks.
                if (termType is not (Constants.Search.SearchMatchOptions.Exists or Constants.Search.SearchMatchOptions.TermMatch))
                {
                    Slice.From(Allocator, valueAsSpan.Slice(startIncrement, valueAsSpan.Length - startIncrement + lengthIncrement), ByteStringType.Immutable, out value);
                }
                
                if (termType is Constants.Search.SearchMatchOptions.TermMatch)
                {
                    termMatches ??= new();
                    termMatches.Add(value);
                    continue;
                }
                
                var query = termType switch
                {
                    Constants.Search.SearchMatchOptions.TermMatch => throw new InvalidDataException(
                        $"{nameof(TermMatch)} is handled in different part of evaluator. This is a bug."),
                    Constants.Search.SearchMatchOptions.Exists => ExistsQuery(field, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.StartsWith => StartWithQuery(field, value, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.EndsWith => EndsWithQuery(field, value, token: cancellationToken),
                    Constants.Search.SearchMatchOptions.Contains => ContainsQuery(field, value, token: cancellationToken),
                    _ => throw new ArgumentOutOfRangeException(nameof(termType), termType.ToString())
                };

                searchBitmapHasValue = true;
                if (@operator == Constants.Search.Operator.Or)
                    Primitives.QueryPrimitives.FillFromMatch(query, ref searchBitmap.BitmapState, Allocator);
                else
                    Primitives.QueryPrimitives.AndWithMatch(query, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);

                continue;
            }

            // Phrase query part (wildcards are not supported in phrase queries).
            // Build bitmap directly from term posting lists for phrase query
            var phraseBitmap = new BitmapMatch(Allocator);
            var tempPhraseBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);
            bool firstPhraseTerm = true;
            foreach (var term in terms.GetEnumerator())
            {
                var termQuery = TermQuery(field, term);
                if (firstPhraseTerm)
                {
                    Primitives.QueryPrimitives.FillFromMatch(termQuery, ref phraseBitmap.BitmapState, Allocator);
                    firstPhraseTerm = false;
                }
                else
                {
                    Primitives.QueryPrimitives.AndWithMatch(termQuery, ref phraseBitmap.BitmapState, ref tempPhraseBitmapData, Allocator);
                }
            }

            var phraseMatch = PhraseQuery(phraseBitmap, field, terms.ToSpan());
            tempPhraseBitmapData.Dispose();

            searchBitmapHasValue = true;
            if (@operator == Constants.Search.Operator.Or)
                Primitives.QueryPrimitives.FillFromMatch(phraseMatch, ref searchBitmap.BitmapState, Allocator);
            else
                Primitives.QueryPrimitives.AndWithMatch(phraseMatch, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);
        }

        if (termMatches?.Count > 0)
        {
            // Build bitmap directly from term posting lists instead of calling AllInQuery/InQuery
            var termBitmap = new BitmapMatch(Allocator);
            var tempTermBitmapData = new Voron.Data.RoaringBitmaps.RoaringBitmap(Allocator);

            if (@operator == Constants.Search.Operator.And)
            {
                // AND all terms together
                bool first = true;
                foreach (var term in termMatches)
                {
                    var termQuery = TermQuery(field, term);
                    if (first)
                    {
                        Primitives.QueryPrimitives.FillFromMatch(termQuery, ref termBitmap.BitmapState, Allocator);
                        first = false;
                    }
                    else
                    {
                        Primitives.QueryPrimitives.AndWithMatch(termQuery, ref termBitmap.BitmapState, ref tempTermBitmapData, Allocator);
                    }
                }
            }
            else
            {
                // OR all terms together
                foreach (var term in termMatches)
                {
                    var termQuery = TermQuery(field, term);
                    Primitives.QueryPrimitives.FillFromMatch(termQuery, ref termBitmap.BitmapState, Allocator);
                }
            }

            searchBitmapHasValue = true;
            if (@operator == Constants.Search.Operator.Or)
                Primitives.QueryPrimitives.FillFromMatch((IQueryMatch)termBitmap, ref searchBitmap.BitmapState, Allocator);
            else
                Primitives.QueryPrimitives.AndWithMatch((IQueryMatch)termBitmap, ref searchBitmap.BitmapState, ref tempBitmapData, Allocator);

            tempTermBitmapData.Dispose();
        }

        if (searchBitmapHasValue)
            searchQuery = searchBitmap;

        tempBitmapData.Dispose();
        return searchQuery ?? TermMatch.CreateEmpty(this, Allocator);

        void AssertFieldIsSearched()
        {
            if (field.Analyzer == null && field.IsDynamic == false)
                throw new InvalidOperationException($"{nameof(SearchQueryWithPhraseQuery)} requires analyzer.");
        }
    }
    
    private Constants.Search.SearchMatchOptions GetTermType(ReadOnlySpan<char> termValue)
    {
        if (termValue.IsEmpty)
            return Constants.Search.SearchMatchOptions.TermMatch;
            
        Constants.Search.SearchMatchOptions mode = default;
            
        if (termValue[0] == '*')
            mode |= Constants.Search.SearchMatchOptions.EndsWith;

        if (termValue[^1] == '*')
        {
            if (termValue.Length <= 2 || termValue[^2] != '\\')
                mode |= Constants.Search.SearchMatchOptions.StartsWith;
        }
            
        if (mode == Constants.Search.SearchMatchOptions.Contains && termValue.Count('*') == termValue.Length)
            return Constants.Search.SearchMatchOptions.Exists;

        return mode;
    }
    
    private Constants.Search.SearchMatchOptions GetTermType(ReadOnlySpan<byte> termValue)
    {
        if (termValue.IsEmpty)
            return Constants.Search.SearchMatchOptions.TermMatch;
            
        Constants.Search.SearchMatchOptions mode = default;
            
        if (termValue[0] == '*')
            mode |= Constants.Search.SearchMatchOptions.EndsWith;

        if (termValue[^1] == '*')
        {
            if (termValue.Length <= 2 || termValue[^2] != '\\')
                mode |= Constants.Search.SearchMatchOptions.StartsWith;
        }
            
        if (mode == Constants.Search.SearchMatchOptions.Contains && termValue.Count((byte)'*') == termValue.Length)
            return Constants.Search.SearchMatchOptions.Exists;

        return mode;
    }
    
    private Analyzer CreateWildcardAnalyzer(in FieldMetadata field, ref Analyzer analyzer)
    {
        if (analyzer != null)
            return analyzer;
        var a = field.Analyzer.IsExactAnalyzer ? Analyzer.CreateDefaultAnalyzer(Allocator) : Analyzer.CreateLowercaseAnalyzer(Allocator);
        analyzer = a;
        return a;
    }
}
