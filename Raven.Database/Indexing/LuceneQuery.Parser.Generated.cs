// This code was generated by the Gardens Point Parser Generator
// Copyright (c) Wayne Kelly, John Gough, QUT 2005-2014
// (see accompanying GPPGcopyright.rtf)

// GPPG version 1.5.2
// Machine:  TAL-PC
// DateTime: 6/19/2016 4:05:53 PM
// UserName: Tal
// Input file <Indexing\LuceneQuery.Language.grammar.y - 2/8/2016 2:21:18 PM>

// options: no-lines gplex

using System;
using System.Collections.Generic;
using System.CodeDom.Compiler;
using System.Globalization;
using System.Text;
using QUT.Gppg;

namespace Raven.Database.Indexing
{
internal enum Token {error=2,EOF=3,NOT=4,OR=5,AND=6,
    INTERSECT=7,PLUS=8,MINUS=9,OPEN_CURLY_BRACKET=10,CLOSE_CURLY_BRACKET=11,OPEN_SQUARE_BRACKET=12,
    CLOSE_SQUARE_BRACKET=13,TILDA=14,BOOST=15,QUOTE=16,TO=17,COLON=18,
    OPEN_PAREN=19,CLOSE_PAREN=20,COMMA=21,ALL_DOC=22,UNANALIZED_TERM=23,METHOD=24,
    UNQUOTED_TERM=25,QUOTED_TERM=26,QUOTED_WILDCARD_TERM=27,FLOAT_NUMBER=28,INT_NUMBER=29,DOUBLE_NUMBER=30,
    LONG_NUMBER=31,DATETIME=32,NULL=33,PREFIX_TERM=34,WILDCARD_TERM=35,HEX_NUMBER=36};

internal partial struct ValueType
{ 
            public string s; 
            public FieldLuceneASTNode fn;
            public ParenthesistLuceneASTNode pn;
            public PostfixModifiers pm;
            public LuceneASTNodeBase nb;
            public OperatorLuceneASTNode.Operator o;
            public RangeLuceneASTNode rn;
            public TermLuceneASTNode tn;
            public MethodLuceneASTNode mn;
            public List<TermLuceneASTNode> ltn;
            public LuceneASTNodeBase.PrefixOperator npo;
       }
// Abstract base class for GPLEX scanners
[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.2")]
internal abstract class ScanBase : AbstractScanner<ValueType,LexLocation> {
  private LexLocation __yylloc = new LexLocation();
  public override LexLocation yylloc { get { return __yylloc; } set { __yylloc = value; } }
  protected virtual bool yywrap() { return true; }
}

// Utility class for encapsulating token information
[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.2")]
internal class ScanObj {
  public int token;
  public ValueType yylval;
  public LexLocation yylloc;
  public ScanObj( int t, ValueType val, LexLocation loc ) {
    this.token = t; this.yylval = val; this.yylloc = loc;
  }
}

[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.2")]
internal partial class LuceneQueryParser: ShiftReduceParser<ValueType, LexLocation>
{
  // Verbatim content from Indexing\LuceneQuery.Language.grammar.y - 2/8/2016 2:21:18 PM
    public LuceneASTNodeBase LuceneAST {get; set;}
  // End verbatim content from Indexing\LuceneQuery.Language.grammar.y - 2/8/2016 2:21:18 PM

#pragma warning disable 649
  private static Dictionary<int, string> aliases;
#pragma warning restore 649
  private static Rule[] rules = new Rule[65];
  private static State[] states = new State[92];
  private static string[] nonTerms = new string[] {
      "main", "prefix_operator", "methodName", "fieldname", "fuzzy_modifier", 
      "boost_modifier", "proximity_modifier", "operator", "term_exp", "term", 
      "postfix_modifier", "paren_exp", "node", "field_exp", "range_operator_exp", 
      "method_exp", "term_match_list", "$accept", };

  static LuceneQueryParser() {
    states[0] = new State(new int[]{4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91},new int[]{-1,1,-13,3,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[1] = new State(new int[]{3,2});
    states[2] = new State(-1);
    states[3] = new State(new int[]{3,4,5,8,6,9,7,10,4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91},new int[]{-8,5,-13,7,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[4] = new State(-2);
    states[5] = new State(new int[]{4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91},new int[]{-13,6,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[6] = new State(new int[]{5,8,6,9,7,10,4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91,3,-4,20,-4},new int[]{-8,5,-13,7,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[7] = new State(new int[]{5,8,6,9,7,10,4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91,3,-5,20,-5},new int[]{-8,5,-13,7,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[8] = new State(-60);
    states[9] = new State(-61);
    states[10] = new State(-62);
    states[11] = new State(new int[]{4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91},new int[]{-13,12,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[12] = new State(new int[]{5,8,6,9,7,10,4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91,3,-3,20,-3},new int[]{-8,5,-13,7,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[13] = new State(-6);
    states[14] = new State(new int[]{10,18,12,37,8,57,9,58,26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,19,61},new int[]{-15,15,-9,16,-12,17,-2,43,-10,59});
    states[15] = new State(-16);
    states[16] = new State(-17);
    states[17] = new State(-18);
    states[18] = new State(new int[]{26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-10,19});
    states[19] = new State(new int[]{17,20});
    states[20] = new State(new int[]{26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-10,21});
    states[21] = new State(new int[]{11,22,13,23});
    states[22] = new State(-56);
    states[23] = new State(-58);
    states[24] = new State(-33);
    states[25] = new State(-34);
    states[26] = new State(-35);
    states[27] = new State(-36);
    states[28] = new State(-37);
    states[29] = new State(-38);
    states[30] = new State(-39);
    states[31] = new State(-40);
    states[32] = new State(-41);
    states[33] = new State(-42);
    states[34] = new State(-43);
    states[35] = new State(-44);
    states[36] = new State(-45);
    states[37] = new State(new int[]{26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-10,38});
    states[38] = new State(new int[]{17,39});
    states[39] = new State(new int[]{26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-10,40});
    states[40] = new State(new int[]{11,41,13,42});
    states[41] = new State(-57);
    states[42] = new State(-59);
    states[43] = new State(new int[]{26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-10,44});
    states[44] = new State(new int[]{14,51,15,48,3,-31,5,-31,6,-31,7,-31,4,-31,25,-31,19,-31,8,-31,9,-31,26,-31,29,-31,28,-31,36,-31,31,-31,30,-31,23,-31,32,-31,33,-31,27,-31,35,-31,34,-31,24,-31,22,-31,20,-31,21,-31},new int[]{-11,45,-7,46,-5,54,-6,56});
    states[45] = new State(-29);
    states[46] = new State(new int[]{15,48,3,-50,5,-50,6,-50,7,-50,4,-50,25,-50,19,-50,8,-50,9,-50,26,-50,29,-50,28,-50,36,-50,31,-50,30,-50,23,-50,32,-50,33,-50,27,-50,35,-50,34,-50,24,-50,22,-50,20,-50,21,-50},new int[]{-6,47});
    states[47] = new State(-46);
    states[48] = new State(new int[]{29,49,28,50});
    states[49] = new State(-52);
    states[50] = new State(-53);
    states[51] = new State(new int[]{29,52,28,53,15,-55,3,-55,5,-55,6,-55,7,-55,4,-55,25,-55,19,-55,8,-55,9,-55,26,-55,36,-55,31,-55,30,-55,23,-55,32,-55,33,-55,27,-55,35,-55,34,-55,24,-55,22,-55,20,-55,21,-55});
    states[52] = new State(-51);
    states[53] = new State(-54);
    states[54] = new State(new int[]{15,48,3,-49,5,-49,6,-49,7,-49,4,-49,25,-49,19,-49,8,-49,9,-49,26,-49,29,-49,28,-49,36,-49,31,-49,30,-49,23,-49,32,-49,33,-49,27,-49,35,-49,34,-49,24,-49,22,-49,20,-49,21,-49},new int[]{-6,55});
    states[55] = new State(-47);
    states[56] = new State(-48);
    states[57] = new State(-63);
    states[58] = new State(-64);
    states[59] = new State(new int[]{14,51,15,48,3,-32,5,-32,6,-32,7,-32,4,-32,25,-32,19,-32,8,-32,9,-32,26,-32,29,-32,28,-32,36,-32,31,-32,30,-32,23,-32,32,-32,33,-32,27,-32,35,-32,34,-32,24,-32,22,-32,20,-32,21,-32},new int[]{-11,60,-7,46,-5,54,-6,56});
    states[60] = new State(-30);
    states[61] = new State(new int[]{4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91},new int[]{-13,62,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[62] = new State(new int[]{20,63,5,8,6,9,7,10,4,11,25,65,19,61,8,57,9,58,26,24,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,24,88,22,91},new int[]{-8,5,-13,7,-14,13,-4,14,-12,67,-9,68,-2,69,-10,59,-16,90,-3,77});
    states[63] = new State(new int[]{15,48,3,-25,5,-25,6,-25,7,-25,4,-25,25,-25,19,-25,8,-25,9,-25,26,-25,29,-25,28,-25,36,-25,31,-25,30,-25,23,-25,32,-25,33,-25,27,-25,35,-25,34,-25,24,-25,22,-25,20,-25},new int[]{-6,64});
    states[64] = new State(-26);
    states[65] = new State(new int[]{18,66,14,-34,15,-34,3,-34,5,-34,6,-34,7,-34,4,-34,25,-34,19,-34,8,-34,9,-34,26,-34,29,-34,28,-34,36,-34,31,-34,30,-34,23,-34,32,-34,33,-34,27,-34,35,-34,34,-34,24,-34,22,-34,20,-34});
    states[66] = new State(-28);
    states[67] = new State(-7);
    states[68] = new State(-8);
    states[69] = new State(new int[]{22,76,26,24,25,65,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,19,61,8,57,9,58,24,88},new int[]{-10,70,-14,72,-12,73,-9,74,-16,75,-4,14,-2,43,-3,77});
    states[70] = new State(new int[]{14,51,15,48,3,-31,5,-31,6,-31,7,-31,4,-31,25,-31,19,-31,8,-31,9,-31,26,-31,29,-31,28,-31,36,-31,31,-31,30,-31,23,-31,32,-31,33,-31,27,-31,35,-31,34,-31,24,-31,22,-31,20,-31},new int[]{-11,71,-7,46,-5,54,-6,56});
    states[71] = new State(-29);
    states[72] = new State(-10);
    states[73] = new State(-11);
    states[74] = new State(-12);
    states[75] = new State(-13);
    states[76] = new State(-14);
    states[77] = new State(new int[]{19,78});
    states[78] = new State(new int[]{8,57,9,58,26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-17,79,-9,81,-2,43,-10,59});
    states[79] = new State(new int[]{20,80});
    states[80] = new State(-19);
    states[81] = new State(new int[]{20,82,21,83,8,57,9,58,26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-9,85,-17,86,-2,43,-10,59});
    states[82] = new State(-20);
    states[83] = new State(new int[]{8,57,9,58,26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36},new int[]{-9,84,-17,87,-2,43,-10,59});
    states[84] = new State(new int[]{21,83,8,57,9,58,26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,20,-21},new int[]{-9,85,-17,86,-2,43,-10,59});
    states[85] = new State(new int[]{21,83,8,57,9,58,26,24,25,25,29,26,28,27,36,28,31,29,30,30,23,31,32,32,33,33,27,34,35,35,34,36,20,-23},new int[]{-9,85,-17,86,-2,43,-10,59});
    states[86] = new State(-24);
    states[87] = new State(-22);
    states[88] = new State(new int[]{18,89});
    states[89] = new State(-27);
    states[90] = new State(-9);
    states[91] = new State(-15);

    for (int sNo = 0; sNo < states.Length; sNo++) states[sNo].number = sNo;

    rules[1] = new Rule(-18, new int[]{-1,3});
    rules[2] = new Rule(-1, new int[]{-13,3});
    rules[3] = new Rule(-13, new int[]{4,-13});
    rules[4] = new Rule(-13, new int[]{-13,-8,-13});
    rules[5] = new Rule(-13, new int[]{-13,-13});
    rules[6] = new Rule(-13, new int[]{-14});
    rules[7] = new Rule(-13, new int[]{-12});
    rules[8] = new Rule(-13, new int[]{-9});
    rules[9] = new Rule(-13, new int[]{-16});
    rules[10] = new Rule(-13, new int[]{-2,-14});
    rules[11] = new Rule(-13, new int[]{-2,-12});
    rules[12] = new Rule(-13, new int[]{-2,-9});
    rules[13] = new Rule(-13, new int[]{-2,-16});
    rules[14] = new Rule(-13, new int[]{-2,22});
    rules[15] = new Rule(-13, new int[]{22});
    rules[16] = new Rule(-14, new int[]{-4,-15});
    rules[17] = new Rule(-14, new int[]{-4,-9});
    rules[18] = new Rule(-14, new int[]{-4,-12});
    rules[19] = new Rule(-16, new int[]{-3,19,-17,20});
    rules[20] = new Rule(-16, new int[]{-3,19,-9,20});
    rules[21] = new Rule(-17, new int[]{-9,21,-9});
    rules[22] = new Rule(-17, new int[]{-9,21,-17});
    rules[23] = new Rule(-17, new int[]{-9,-9});
    rules[24] = new Rule(-17, new int[]{-9,-17});
    rules[25] = new Rule(-12, new int[]{19,-13,20});
    rules[26] = new Rule(-12, new int[]{19,-13,20,-6});
    rules[27] = new Rule(-3, new int[]{24,18});
    rules[28] = new Rule(-4, new int[]{25,18});
    rules[29] = new Rule(-9, new int[]{-2,-10,-11});
    rules[30] = new Rule(-9, new int[]{-10,-11});
    rules[31] = new Rule(-9, new int[]{-2,-10});
    rules[32] = new Rule(-9, new int[]{-10});
    rules[33] = new Rule(-10, new int[]{26});
    rules[34] = new Rule(-10, new int[]{25});
    rules[35] = new Rule(-10, new int[]{29});
    rules[36] = new Rule(-10, new int[]{28});
    rules[37] = new Rule(-10, new int[]{36});
    rules[38] = new Rule(-10, new int[]{31});
    rules[39] = new Rule(-10, new int[]{30});
    rules[40] = new Rule(-10, new int[]{23});
    rules[41] = new Rule(-10, new int[]{32});
    rules[42] = new Rule(-10, new int[]{33});
    rules[43] = new Rule(-10, new int[]{27});
    rules[44] = new Rule(-10, new int[]{35});
    rules[45] = new Rule(-10, new int[]{34});
    rules[46] = new Rule(-11, new int[]{-7,-6});
    rules[47] = new Rule(-11, new int[]{-5,-6});
    rules[48] = new Rule(-11, new int[]{-6});
    rules[49] = new Rule(-11, new int[]{-5});
    rules[50] = new Rule(-11, new int[]{-7});
    rules[51] = new Rule(-7, new int[]{14,29});
    rules[52] = new Rule(-6, new int[]{15,29});
    rules[53] = new Rule(-6, new int[]{15,28});
    rules[54] = new Rule(-5, new int[]{14,28});
    rules[55] = new Rule(-5, new int[]{14});
    rules[56] = new Rule(-15, new int[]{10,-10,17,-10,11});
    rules[57] = new Rule(-15, new int[]{12,-10,17,-10,11});
    rules[58] = new Rule(-15, new int[]{10,-10,17,-10,13});
    rules[59] = new Rule(-15, new int[]{12,-10,17,-10,13});
    rules[60] = new Rule(-8, new int[]{5});
    rules[61] = new Rule(-8, new int[]{6});
    rules[62] = new Rule(-8, new int[]{7});
    rules[63] = new Rule(-2, new int[]{8});
    rules[64] = new Rule(-2, new int[]{9});
  }

  protected override void Initialize() {
    this.InitSpecialTokens((int)Token.error, (int)Token.EOF);
    this.InitStates(states);
    this.InitRules(rules);
    this.InitNonTerminals(nonTerms);
  }

  protected override void DoAction(int action)
  {
#pragma warning disable 162, 1522
    switch (action)
    {
      case 2: // main -> node, EOF
{
    //Console.WriteLine("Found rule main -> node EOF");
    CurrentSemanticValue.nb = ValueStack[ValueStack.Depth-2].nb;
    LuceneAST = CurrentSemanticValue.nb;
    }
        break;
      case 3: // node -> NOT, node
{
        //Console.WriteLine("Found rule node -> NOT node");
        CurrentSemanticValue.nb = new OperatorLuceneASTNode(ValueStack[ValueStack.Depth-1].nb,null,OperatorLuceneASTNode.Operator.NOT);
    }
        break;
      case 4: // node -> node, operator, node
{
        //Console.WriteLine("Found rule node -> node operator node");
        var res =  new OperatorLuceneASTNode(ValueStack[ValueStack.Depth-3].nb,ValueStack[ValueStack.Depth-1].nb,ValueStack[ValueStack.Depth-2].o);
        CurrentSemanticValue.nb = res;
    }
        break;
      case 5: // node -> node, node
{
        //Console.WriteLine("Found rule node -> node node");
        CurrentSemanticValue.nb = new OperatorLuceneASTNode(ValueStack[ValueStack.Depth-2].nb,ValueStack[ValueStack.Depth-1].nb,OperatorLuceneASTNode.Operator.Implicit);
    }
        break;
      case 6: // node -> field_exp
{
        //Console.WriteLine("Found rule node -> field_exp");
        CurrentSemanticValue.nb =ValueStack[ValueStack.Depth-1].fn;
    }
        break;
      case 7: // node -> paren_exp
{
        //Console.WriteLine("Found rule node -> paren_exp");
        CurrentSemanticValue.nb =ValueStack[ValueStack.Depth-1].pn;
    }
        break;
      case 8: // node -> term_exp
{
    //Console.WriteLine("Found rule node -> term_exp");
        CurrentSemanticValue.nb = ValueStack[ValueStack.Depth-1].tn;
    }
        break;
      case 9: // node -> method_exp
{
        //Console.WriteLine("Found rule node -> method_exp");
        CurrentSemanticValue.nb = ValueStack[ValueStack.Depth-1].mn;
    }
        break;
      case 10: // node -> prefix_operator, field_exp
{
        //Console.WriteLine("Found rule node -> prefix_operator field_exp");
        CurrentSemanticValue.nb =ValueStack[ValueStack.Depth-1].fn;
        CurrentSemanticValue.nb.Prefix = ValueStack[ValueStack.Depth-2].npo;
    }
        break;
      case 11: // node -> prefix_operator, paren_exp
{
        //Console.WriteLine("Found rule node -> prefix_operator paren_exp");
        CurrentSemanticValue.nb =ValueStack[ValueStack.Depth-1].pn;
        CurrentSemanticValue.nb.Prefix = ValueStack[ValueStack.Depth-2].npo;
    }
        break;
      case 12: // node -> prefix_operator, term_exp
{
    //Console.WriteLine("Found rule node -> prefix_operator term_exp");
        CurrentSemanticValue.nb = ValueStack[ValueStack.Depth-1].tn;
        CurrentSemanticValue.nb.Prefix = ValueStack[ValueStack.Depth-2].npo;
    }
        break;
      case 13: // node -> prefix_operator, method_exp
{
        //Console.WriteLine("Found rule node -> prefix_operator method_exp");
        CurrentSemanticValue.nb = ValueStack[ValueStack.Depth-1].mn;
        CurrentSemanticValue.nb.Prefix = ValueStack[ValueStack.Depth-2].npo;
    }
        break;
      case 14: // node -> prefix_operator, ALL_DOC
{
        //Console.WriteLine("Found rule node -> prefix_operator ALL_DOC");
        CurrentSemanticValue.nb = new AllDocumentsLuceneASTNode();
        CurrentSemanticValue.nb.Prefix = ValueStack[ValueStack.Depth-2].npo;
    }
        break;
      case 15: // node -> ALL_DOC
{
        CurrentSemanticValue.nb = new AllDocumentsLuceneASTNode();
    }
        break;
      case 16: // field_exp -> fieldname, range_operator_exp
{
        //Console.WriteLine("Found rule field_exp -> fieldname range_operator_exp");		
        CurrentSemanticValue.fn = new FieldLuceneASTNode(){FieldName = ValueStack[ValueStack.Depth-2].s, Node = ValueStack[ValueStack.Depth-1].rn};
        }
        break;
      case 17: // field_exp -> fieldname, term_exp
{
        //Console.WriteLine("Found rule field_exp -> fieldname term_exp");
        CurrentSemanticValue.fn = new FieldLuceneASTNode(){FieldName = ValueStack[ValueStack.Depth-2].s, Node = ValueStack[ValueStack.Depth-1].tn};
        }
        break;
      case 18: // field_exp -> fieldname, paren_exp
{
        //Console.WriteLine("Found rule field_exp -> fieldname paren_exp");
        CurrentSemanticValue.fn = new FieldLuceneASTNode(){FieldName = ValueStack[ValueStack.Depth-2].s, Node = ValueStack[ValueStack.Depth-1].pn};
    }
        break;
      case 19: // method_exp -> methodName, OPEN_PAREN, term_match_list, CLOSE_PAREN
{
        //Console.WriteLine("Found rule method_exp -> methodName OPEN_PAREN term_match_list CLOSE_PAREN");
        CurrentSemanticValue.mn = new MethodLuceneASTNode(ValueStack[ValueStack.Depth-4].s,ValueStack[ValueStack.Depth-2].ltn);
        InMethod = false;
}
        break;
      case 20: // method_exp -> methodName, OPEN_PAREN, term_exp, CLOSE_PAREN
{
        //Console.WriteLine("Found rule method_exp -> methodName OPEN_PAREN term_exp CLOSE_PAREN");
        CurrentSemanticValue.mn = new MethodLuceneASTNode(ValueStack[ValueStack.Depth-4].s,ValueStack[ValueStack.Depth-2].tn);
        InMethod = false;
}
        break;
      case 21: // term_match_list -> term_exp, COMMA, term_exp
{
    //Console.WriteLine("Found rule term_match_list -> term_exp COMMA term_exp");
    CurrentSemanticValue.ltn = new List<TermLuceneASTNode>(){ValueStack[ValueStack.Depth-3].tn,ValueStack[ValueStack.Depth-1].tn};
}
        break;
      case 22: // term_match_list -> term_exp, COMMA, term_match_list
{
    //Console.WriteLine("Found rule term_match_list -> term_exp COMMA term_match_list");
    ValueStack[ValueStack.Depth-1].ltn.Add(ValueStack[ValueStack.Depth-3].tn);
    CurrentSemanticValue.ltn = ValueStack[ValueStack.Depth-1].ltn;
}
        break;
      case 23: // term_match_list -> term_exp, term_exp
{
    //Console.WriteLine("Found rule term_match_list -> term_exp term_exp");
    CurrentSemanticValue.ltn = new List<TermLuceneASTNode>(){ValueStack[ValueStack.Depth-2].tn,ValueStack[ValueStack.Depth-1].tn};
}
        break;
      case 24: // term_match_list -> term_exp, term_match_list
{
    //Console.WriteLine("Found rule term_match_list -> term_exp term_match_list");
    ValueStack[ValueStack.Depth-1].ltn.Add(ValueStack[ValueStack.Depth-2].tn);
    CurrentSemanticValue.ltn = ValueStack[ValueStack.Depth-1].ltn;
}
        break;
      case 25: // paren_exp -> OPEN_PAREN, node, CLOSE_PAREN
{
        //Console.WriteLine("Found rule paren_exp -> OPEN_PAREN node CLOSE_PAREN");
        CurrentSemanticValue.pn = new ParenthesistLuceneASTNode();
        CurrentSemanticValue.pn.Node = ValueStack[ValueStack.Depth-2].nb;
        }
        break;
      case 26: // paren_exp -> OPEN_PAREN, node, CLOSE_PAREN, boost_modifier
{
        //Console.WriteLine("Found rule paren_exp -> OPEN_PAREN node CLOSE_PAREN boost_modifier");
        CurrentSemanticValue.pn = new ParenthesistLuceneASTNode();
        CurrentSemanticValue.pn.Node = ValueStack[ValueStack.Depth-3].nb;
        CurrentSemanticValue.pn.Boost = ValueStack[ValueStack.Depth-1].s;
        }
        break;
      case 27: // methodName -> METHOD, COLON
{
        //Console.WriteLine("Found rule methodName -> METHOD COLON");
        CurrentSemanticValue.s = ValueStack[ValueStack.Depth-2].s;
        InMethod = true;
}
        break;
      case 28: // fieldname -> UNQUOTED_TERM, COLON
{
        //Console.WriteLine("Found rule fieldname -> UNQUOTED_TERM COLON");
        CurrentSemanticValue.s = ValueStack[ValueStack.Depth-2].s;
    }
        break;
      case 29: // term_exp -> prefix_operator, term, postfix_modifier
{
        //Console.WriteLine("Found rule term_exp -> prefix_operator term postfix_modifier");
        CurrentSemanticValue.tn = ValueStack[ValueStack.Depth-2].tn;
        CurrentSemanticValue.tn.Prefix =ValueStack[ValueStack.Depth-3].npo;
        CurrentSemanticValue.tn.SetPostfixOperators(ValueStack[ValueStack.Depth-1].pm);
    }
        break;
      case 30: // term_exp -> term, postfix_modifier
{
        //Console.WriteLine("Found rule term_exp -> postfix_modifier");
        CurrentSemanticValue.tn = ValueStack[ValueStack.Depth-2].tn;
        CurrentSemanticValue.tn.SetPostfixOperators(ValueStack[ValueStack.Depth-1].pm);
    }
        break;
      case 31: // term_exp -> prefix_operator, term
{
        //Console.WriteLine("Found rule term_exp -> prefix_operator term");
        CurrentSemanticValue.tn = ValueStack[ValueStack.Depth-1].tn;
        CurrentSemanticValue.tn.Prefix = ValueStack[ValueStack.Depth-2].npo;
    }
        break;
      case 32: // term_exp -> term
{
        //Console.WriteLine("Found rule term_exp -> term");
        CurrentSemanticValue.tn = ValueStack[ValueStack.Depth-1].tn;
    }
        break;
      case 33: // term -> QUOTED_TERM
{
        //Console.WriteLine("Found rule term -> QUOTED_TERM");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s.Substring(1,ValueStack[ValueStack.Depth-1].s.Length-2), Type=TermLuceneASTNode.TermType.Quoted};
    }
        break;
      case 34: // term -> UNQUOTED_TERM
{
        //Console.WriteLine("Found rule term -> UNQUOTED_TERM");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s,Type=TermLuceneASTNode.TermType.UnQuoted};
        }
        break;
      case 35: // term -> INT_NUMBER
{
        //Console.WriteLine("Found rule term -> INT_NUMBER");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.Int};
        }
        break;
      case 36: // term -> FLOAT_NUMBER
{
        //Console.WriteLine("Found rule term -> FLOAT_NUMBER");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.Float};
    }
        break;
      case 37: // term -> HEX_NUMBER
{
        //Console.WriteLine("Found rule term -> HEX_NUMBER");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.Hex};
    }
        break;
      case 38: // term -> LONG_NUMBER
{
        //Console.WriteLine("Found rule term -> INT_NUMBER");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.Long};
        }
        break;
      case 39: // term -> DOUBLE_NUMBER
{
        //Console.WriteLine("Found rule term -> FLOAT_NUMBER");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.Double};
    }
        break;
      case 40: // term -> UNANALIZED_TERM
{
        //Console.WriteLine("Found rule term -> UNANALIZED_TERM");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.UnAnalyzed};
    }
        break;
      case 41: // term -> DATETIME
{
        //Console.WriteLine("Found rule term -> DATETIME");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.DateTime};
    }
        break;
      case 42: // term -> NULL
{
        //Console.WriteLine("Found rule term -> NULL");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.Null};
    }
        break;
      case 43: // term -> QUOTED_WILDCARD_TERM
{
        //Console.WriteLine("Found rule term -> QUOTED_WILDCARD_TERM");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.QuotedWildcard};
    }
        break;
      case 44: // term -> WILDCARD_TERM
{
        //Console.WriteLine("Found rule term -> WILDCARD_TERM");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.WildCardTerm};
    }
        break;
      case 45: // term -> PREFIX_TERM
{
        //Console.WriteLine("Found rule term -> PREFIX_TERM");
        CurrentSemanticValue.tn = new TermLuceneASTNode(){Term=ValueStack[ValueStack.Depth-1].s, Type=TermLuceneASTNode.TermType.PrefixTerm};
    }
        break;
      case 46: // postfix_modifier -> proximity_modifier, boost_modifier
{
        CurrentSemanticValue.pm = new PostfixModifiers(){Boost = ValueStack[ValueStack.Depth-1].s, Similerity = null, Proximity = ValueStack[ValueStack.Depth-2].s};
    }
        break;
      case 47: // postfix_modifier -> fuzzy_modifier, boost_modifier
{
        CurrentSemanticValue.pm = new PostfixModifiers(){Boost = ValueStack[ValueStack.Depth-1].s, Similerity = ValueStack[ValueStack.Depth-2].s, Proximity = null};
    }
        break;
      case 48: // postfix_modifier -> boost_modifier
{
        CurrentSemanticValue.pm = new PostfixModifiers(){Boost = ValueStack[ValueStack.Depth-1].s,Similerity = null, Proximity = null};
    }
        break;
      case 49: // postfix_modifier -> fuzzy_modifier
{
        CurrentSemanticValue.pm = new PostfixModifiers(){Boost = null, Similerity = ValueStack[ValueStack.Depth-1].s, Proximity = null};
    }
        break;
      case 50: // postfix_modifier -> proximity_modifier
{
        CurrentSemanticValue.pm = new PostfixModifiers(){Boost = null, Similerity = null, Proximity = ValueStack[ValueStack.Depth-1].s};
    }
        break;
      case 51: // proximity_modifier -> TILDA, INT_NUMBER
{
    //Console.WriteLine("Found rule proximity_modifier -> TILDA INT_NUMBER");
    CurrentSemanticValue.s = ValueStack[ValueStack.Depth-1].s;
    }
        break;
      case 52: // boost_modifier -> BOOST, INT_NUMBER
{
    //Console.WriteLine("Found rule boost_modifier -> BOOST INT_NUMBER");
    CurrentSemanticValue.s = ValueStack[ValueStack.Depth-1].s;
    }
        break;
      case 53: // boost_modifier -> BOOST, FLOAT_NUMBER
{
    //Console.WriteLine("Found rule boost_modifier -> BOOST FLOAT_NUMBER");
    CurrentSemanticValue.s = ValueStack[ValueStack.Depth-1].s;
    }
        break;
      case 54: // fuzzy_modifier -> TILDA, FLOAT_NUMBER
{
    //Console.WriteLine("Found rule fuzzy_modifier ->  TILDA FLOAT_NUMBER");
    CurrentSemanticValue.s = ValueStack[ValueStack.Depth-1].s;
    }
        break;
      case 55: // fuzzy_modifier -> TILDA
{
        //Console.WriteLine("Found rule fuzzy_modifier ->  TILDA");
        CurrentSemanticValue.s = "0.5";
    }
        break;
      case 56: // range_operator_exp -> OPEN_CURLY_BRACKET, term, TO, term, CLOSE_CURLY_BRACKET
{
        //Console.WriteLine("Found rule range_operator_exp -> OPEN_CURLY_BRACKET term TO term CLOSE_CURLY_BRACKET");
        CurrentSemanticValue.rn = new RangeLuceneASTNode(){RangeMin = ValueStack[ValueStack.Depth-4].tn, RangeMax = ValueStack[ValueStack.Depth-2].tn, InclusiveMin = false, InclusiveMax = false};
        }
        break;
      case 57: // range_operator_exp -> OPEN_SQUARE_BRACKET, term, TO, term, CLOSE_CURLY_BRACKET
{
        //Console.WriteLine("Found rule range_operator_exp -> OPEN_SQUARE_BRACKET term TO term CLOSE_CURLY_BRACKET");
        CurrentSemanticValue.rn = new RangeLuceneASTNode(){RangeMin = ValueStack[ValueStack.Depth-4].tn, RangeMax = ValueStack[ValueStack.Depth-2].tn, InclusiveMin = true, InclusiveMax = false};
        }
        break;
      case 58: // range_operator_exp -> OPEN_CURLY_BRACKET, term, TO, term, CLOSE_SQUARE_BRACKET
{
        //Console.WriteLine("Found rule range_operator_exp -> OPEN_CURLY_BRACKET term TO term CLOSE_SQUARE_BRACKET");
        CurrentSemanticValue.rn = new RangeLuceneASTNode(){RangeMin = ValueStack[ValueStack.Depth-4].tn, RangeMax = ValueStack[ValueStack.Depth-2].tn, InclusiveMin = false, InclusiveMax = true};
        }
        break;
      case 59: // range_operator_exp -> OPEN_SQUARE_BRACKET, term, TO, term, CLOSE_SQUARE_BRACKET
{
        //Console.WriteLine("Found rule range_operator_exp -> OPEN_SQUARE_BRACKET term TO term CLOSE_SQUARE_BRACKET");
        CurrentSemanticValue.rn = new RangeLuceneASTNode(){RangeMin = ValueStack[ValueStack.Depth-4].tn, RangeMax = ValueStack[ValueStack.Depth-2].tn, InclusiveMin = true, InclusiveMax = true};
        }
        break;
      case 60: // operator -> OR
{
        //Console.WriteLine("Found rule operator -> OR");
        CurrentSemanticValue.o = OperatorLuceneASTNode.Operator.OR;
        }
        break;
      case 61: // operator -> AND
{
        //Console.WriteLine("Found rule operator -> AND");
        CurrentSemanticValue.o = OperatorLuceneASTNode.Operator.AND;
        }
        break;
      case 62: // operator -> INTERSECT
{
        //Console.WriteLine("Found rule operator -> INTERSECT");
        CurrentSemanticValue.o = OperatorLuceneASTNode.Operator.INTERSECT;
    }
        break;
      case 63: // prefix_operator -> PLUS
{
        //Console.WriteLine("Found rule prefix_operator -> PLUS");
        CurrentSemanticValue.npo = LuceneASTNodeBase.PrefixOperator.Plus;
        }
        break;
      case 64: // prefix_operator -> MINUS
{
        //Console.WriteLine("Found rule prefix_operator -> MINUS");
        CurrentSemanticValue.npo = LuceneASTNodeBase.PrefixOperator.Minus;
        }
        break;
    }
#pragma warning restore 162, 1522
  }

  protected override string TerminalToString(int terminal)
  {
    if (aliases != null && aliases.ContainsKey(terminal))
        return aliases[terminal];
    else if (((Token)terminal).ToString() != terminal.ToString(CultureInfo.InvariantCulture))
        return ((Token)terminal).ToString();
    else
        return CharToString((char)terminal);
  }

}
}
