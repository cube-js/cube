// Generated from GenericSql.g4 by ANTLR 4.8
// jshint ignore: start
var antlr4 = require('antlr4/index');
var GenericSqlListener = require('./GenericSqlListener').GenericSqlListener;
var grammarFileName = "GenericSql.g4";


var serializedATN = ["\u0003\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964",
    "\u0003\u001d\u00a3\u0004\u0002\t\u0002\u0004\u0003\t\u0003\u0004\u0004",
    "\t\u0004\u0004\u0005\t\u0005\u0004\u0006\t\u0006\u0004\u0007\t\u0007",
    "\u0004\b\t\b\u0004\t\t\t\u0004\n\t\n\u0004\u000b\t\u000b\u0004\f\t\f",
    "\u0004\r\t\r\u0004\u000e\t\u000e\u0003\u0002\u0003\u0002\u0003\u0002",
    "\u0003\u0002\u0003\u0002\u0003\u0002\u0003\u0002\u0003\u0002\u0005\u0002",
    "%\n\u0002\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003",
    "\u0003\u0003\u0005\u0003-\n\u0003\u0003\u0004\u0003\u0004\u0003\u0005",
    "\u0003\u0005\u0003\u0005\u0007\u00054\n\u0005\f\u0005\u000e\u00057\u000b",
    "\u0005\u0003\u0006\u0003\u0006\u0005\u0006;\n\u0006\u0003\u0007\u0003",
    "\u0007\u0005\u0007?\n\u0007\u0003\u0007\u0005\u0007B\n\u0007\u0003\b",
    "\u0003\b\u0003\b\u0003\b\u0003\b\u0003\b\u0003\b\u0003\b\u0003\b\u0003",
    "\b\u0003\b\u0005\bO\n\b\u0003\t\u0003\t\u0003\t\u0003\t\u0003\t\u0003",
    "\t\u0003\t\u0007\tX\n\t\f\t\u000e\t[\u000b\t\u0003\t\u0003\t\u0003\t",
    "\u0003\t\u0003\t\u0003\t\u0003\t\u0003\t\u0003\t\u0003\t\u0003\t\u0003",
    "\t\u0003\t\u0003\t\u0003\t\u0003\t\u0003\t\u0005\tn\n\t\u0003\t\u0003",
    "\t\u0003\t\u0003\t\u0003\t\u0003\t\u0007\tv\n\t\f\t\u000e\ty\u000b\t",
    "\u0003\n\u0006\n|\n\n\r\n\u000e\n}\u0003\n\u0003\n\u0006\n\u0082\n\n",
    "\r\n\u000e\n\u0083\u0005\n\u0086\n\n\u0003\n\u0003\n\u0006\n\u008a\n",
    "\n\r\n\u000e\n\u008b\u0005\n\u008e\n\n\u0003\u000b\u0003\u000b\u0003",
    "\f\u0003\f\u0003\f\u0003\f\u0003\f\u0005\f\u0097\n\f\u0003\r\u0003\r",
    "\u0003\r\u0007\r\u009c\n\r\f\r\u000e\r\u009f\u000b\r\u0003\u000e\u0003",
    "\u000e\u0003\u000e\u0002\u0003\u0010\u000f\u0002\u0004\u0006\b\n\f\u000e",
    "\u0010\u0012\u0014\u0016\u0018\u001a\u0002\u0004\u0003\u0002\u000f\u0014",
    "\u0004\u0002\u0019\u0019\u001b\u001b\u0002\u00af\u0002$\u0003\u0002",
    "\u0002\u0002\u0004&\u0003\u0002\u0002\u0002\u0006.\u0003\u0002\u0002",
    "\u0002\b0\u0003\u0002\u0002\u0002\n:\u0003\u0002\u0002\u0002\f<\u0003",
    "\u0002\u0002\u0002\u000eN\u0003\u0002\u0002\u0002\u0010m\u0003\u0002",
    "\u0002\u0002\u0012\u008d\u0003\u0002\u0002\u0002\u0014\u008f\u0003\u0002",
    "\u0002\u0002\u0016\u0096\u0003\u0002\u0002\u0002\u0018\u0098\u0003\u0002",
    "\u0002\u0002\u001a\u00a0\u0003\u0002\u0002\u0002\u001c\u001d\u0005\u0004",
    "\u0003\u0002\u001d\u001e\u0007\u0002\u0002\u0003\u001e%\u0003\u0002",
    "\u0002\u0002\u001f \u0007\u0003\u0002\u0002 !\u0005\u0004\u0003\u0002",
    "!\"\u0007\u0004\u0002\u0002\"#\u0007\u0002\u0002\u0003#%\u0003\u0002",
    "\u0002\u0002$\u001c\u0003\u0002\u0002\u0002$\u001f\u0003\u0002\u0002",
    "\u0002%\u0003\u0003\u0002\u0002\u0002&\'\u0007\u0007\u0002\u0002\'(",
    "\u0005\b\u0005\u0002()\u0007\t\u0002\u0002),\u0005\u0006\u0004\u0002",
    "*+\u0007\n\u0002\u0002+-\u0005\u000e\b\u0002,*\u0003\u0002\u0002\u0002",
    ",-\u0003\u0002\u0002\u0002-\u0005\u0003\u0002\u0002\u0002./\u0005\f",
    "\u0007\u0002/\u0007\u0003\u0002\u0002\u000205\u0005\n\u0006\u000212",
    "\u0007\u0005\u0002\u000224\u0005\n\u0006\u000231\u0003\u0002\u0002\u0002",
    "47\u0003\u0002\u0002\u000253\u0003\u0002\u0002\u000256\u0003\u0002\u0002",
    "\u00026\t\u0003\u0002\u0002\u000275\u0003\u0002\u0002\u00028;\u0005",
    "\f\u0007\u00029;\u0007\b\u0002\u0002:8\u0003\u0002\u0002\u0002:9\u0003",
    "\u0002\u0002\u0002;\u000b\u0003\u0002\u0002\u0002<A\u0005\u0018\r\u0002",
    "=?\u0007\u000e\u0002\u0002>=\u0003\u0002\u0002\u0002>?\u0003\u0002\u0002",
    "\u0002?@\u0003\u0002\u0002\u0002@B\u0005\u001a\u000e\u0002A>\u0003\u0002",
    "\u0002\u0002AB\u0003\u0002\u0002\u0002B\r\u0003\u0002\u0002\u0002CO",
    "\u0005\u0010\t\u0002DE\u0005\u0010\t\u0002EF\u0007\u000b\u0002\u0002",
    "FG\u0005\u0010\t\u0002GO\u0003\u0002\u0002\u0002HI\u0005\u0010\t\u0002",
    "IJ\u0007\f\u0002\u0002JK\u0005\u0010\t\u0002KO\u0003\u0002\u0002\u0002",
    "LM\u0007\r\u0002\u0002MO\u0005\u0010\t\u0002NC\u0003\u0002\u0002\u0002",
    "ND\u0003\u0002\u0002\u0002NH\u0003\u0002\u0002\u0002NL\u0003\u0002\u0002",
    "\u0002O\u000f\u0003\u0002\u0002\u0002PQ\b\t\u0001\u0002Qn\u0005\u0018",
    "\r\u0002RS\u0005\u001a\u000e\u0002ST\u0007\u0003\u0002\u0002TY\u0005",
    "\u0010\t\u0002UV\u0007\u0005\u0002\u0002VX\u0005\u0010\t\u0002WU\u0003",
    "\u0002\u0002\u0002X[\u0003\u0002\u0002\u0002YW\u0003\u0002\u0002\u0002",
    "YZ\u0003\u0002\u0002\u0002Z\\\u0003\u0002\u0002\u0002[Y\u0003\u0002",
    "\u0002\u0002\\]\u0007\u0004\u0002\u0002]n\u0003\u0002\u0002\u0002^_",
    "\u0007\u0017\u0002\u0002_`\u0007\u0003\u0002\u0002`a\u0005\u0010\t\u0002",
    "ab\u0007\u000e\u0002\u0002bc\u0005\u001a\u000e\u0002cd\u0007\u0004\u0002",
    "\u0002dn\u0003\u0002\u0002\u0002en\u0007\u001c\u0002\u0002fn\u0005\u0012",
    "\n\u0002gn\u0005\u001a\u000e\u0002hn\u0007\u0018\u0002\u0002ij\u0007",
    "\u0003\u0002\u0002jk\u0005\u0010\t\u0002kl\u0007\u0004\u0002\u0002l",
    "n\u0003\u0002\u0002\u0002mP\u0003\u0002\u0002\u0002mR\u0003\u0002\u0002",
    "\u0002m^\u0003\u0002\u0002\u0002me\u0003\u0002\u0002\u0002mf\u0003\u0002",
    "\u0002\u0002mg\u0003\u0002\u0002\u0002mh\u0003\u0002\u0002\u0002mi\u0003",
    "\u0002\u0002\u0002nw\u0003\u0002\u0002\u0002op\f\f\u0002\u0002pq\u0005",
    "\u0014\u000b\u0002qr\u0005\u0010\t\rrv\u0003\u0002\u0002\u0002st\f\u000b",
    "\u0002\u0002tv\u0005\u0016\f\u0002uo\u0003\u0002\u0002\u0002us\u0003",
    "\u0002\u0002\u0002vy\u0003\u0002\u0002\u0002wu\u0003\u0002\u0002\u0002",
    "wx\u0003\u0002\u0002\u0002x\u0011\u0003\u0002\u0002\u0002yw\u0003\u0002",
    "\u0002\u0002z|\u0007\u001a\u0002\u0002{z\u0003\u0002\u0002\u0002|}\u0003",
    "\u0002\u0002\u0002}{\u0003\u0002\u0002\u0002}~\u0003\u0002\u0002\u0002",
    "~\u0085\u0003\u0002\u0002\u0002\u007f\u0081\u0007\u0006\u0002\u0002",
    "\u0080\u0082\u0007\u001a\u0002\u0002\u0081\u0080\u0003\u0002\u0002\u0002",
    "\u0082\u0083\u0003\u0002\u0002\u0002\u0083\u0081\u0003\u0002\u0002\u0002",
    "\u0083\u0084\u0003\u0002\u0002\u0002\u0084\u0086\u0003\u0002\u0002\u0002",
    "\u0085\u007f\u0003\u0002\u0002\u0002\u0085\u0086\u0003\u0002\u0002\u0002",
    "\u0086\u008e\u0003\u0002\u0002\u0002\u0087\u0089\u0007\u0006\u0002\u0002",
    "\u0088\u008a\u0007\u001a\u0002\u0002\u0089\u0088\u0003\u0002\u0002\u0002",
    "\u008a\u008b\u0003\u0002\u0002\u0002\u008b\u0089\u0003\u0002\u0002\u0002",
    "\u008b\u008c\u0003\u0002\u0002\u0002\u008c\u008e\u0003\u0002\u0002\u0002",
    "\u008d{\u0003\u0002\u0002\u0002\u008d\u0087\u0003\u0002\u0002\u0002",
    "\u008e\u0013\u0003\u0002\u0002\u0002\u008f\u0090\t\u0002\u0002\u0002",
    "\u0090\u0015\u0003\u0002\u0002\u0002\u0091\u0092\u0007\u0015\u0002\u0002",
    "\u0092\u0097\u0007\u0016\u0002\u0002\u0093\u0094\u0007\u0015\u0002\u0002",
    "\u0094\u0095\u0007\r\u0002\u0002\u0095\u0097\u0007\u0016\u0002\u0002",
    "\u0096\u0091\u0003\u0002\u0002\u0002\u0096\u0093\u0003\u0002\u0002\u0002",
    "\u0097\u0017\u0003\u0002\u0002\u0002\u0098\u009d\u0005\u001a\u000e\u0002",
    "\u0099\u009a\u0007\u0006\u0002\u0002\u009a\u009c\u0005\u001a\u000e\u0002",
    "\u009b\u0099\u0003\u0002\u0002\u0002\u009c\u009f\u0003\u0002\u0002\u0002",
    "\u009d\u009b\u0003\u0002\u0002\u0002\u009d\u009e\u0003\u0002\u0002\u0002",
    "\u009e\u0019\u0003\u0002\u0002\u0002\u009f\u009d\u0003\u0002\u0002\u0002",
    "\u00a0\u00a1\t\u0003\u0002\u0002\u00a1\u001b\u0003\u0002\u0002\u0002",
    "\u0014$,5:>ANYmuw}\u0083\u0085\u008b\u008d\u0096\u009d"].join("");


var atn = new antlr4.atn.ATNDeserializer().deserialize(serializedATN);

var decisionsToDFA = atn.decisionToState.map( function(ds, index) { return new antlr4.dfa.DFA(ds, index); });

var sharedContextCache = new antlr4.PredictionContextCache();

var literalNames = [ null, "'('", "')'", "','", "'.'", "'SELECT'", "'*'", 
                     "'FROM'", "'WHERE'", "'AND'", "'OR'", "'NOT'", "'AS'", 
                     "'<'", "'<='", "'>'", "'>='", "'='", null, "'IS'", 
                     "'NULL'", "'CAST'" ];

var symbolicNames = [ null, null, null, null, null, "SELECT", "ASTERISK", 
                      "FROM", "WHERE", "AND", "OR", "NOT", "AS", "LT", "LTE", 
                      "GT", "GTE", "EQUALS", "NOT_EQUALS", "IS", "NULL", 
                      "CAST", "INDEXED_PARAM", "ID", "DIGIT", "QUOTED_ID", 
                      "STRING", "WHITESPACE" ];

var ruleNames =  [ "statement", "query", "fromTables", "selectFields", "field", 
                   "aliasField", "boolExp", "exp", "numeric", "binaryOperator", 
                   "unaryOperator", "idPath", "identifier" ];

function GenericSqlParser (input) {
	antlr4.Parser.call(this, input);
    this._interp = new antlr4.atn.ParserATNSimulator(this, atn, decisionsToDFA, sharedContextCache);
    this.ruleNames = ruleNames;
    this.literalNames = literalNames;
    this.symbolicNames = symbolicNames;
    return this;
}

GenericSqlParser.prototype = Object.create(antlr4.Parser.prototype);
GenericSqlParser.prototype.constructor = GenericSqlParser;

Object.defineProperty(GenericSqlParser.prototype, "atn", {
	get : function() {
		return atn;
	}
});

GenericSqlParser.EOF = antlr4.Token.EOF;
GenericSqlParser.T__0 = 1;
GenericSqlParser.T__1 = 2;
GenericSqlParser.T__2 = 3;
GenericSqlParser.T__3 = 4;
GenericSqlParser.SELECT = 5;
GenericSqlParser.ASTERISK = 6;
GenericSqlParser.FROM = 7;
GenericSqlParser.WHERE = 8;
GenericSqlParser.AND = 9;
GenericSqlParser.OR = 10;
GenericSqlParser.NOT = 11;
GenericSqlParser.AS = 12;
GenericSqlParser.LT = 13;
GenericSqlParser.LTE = 14;
GenericSqlParser.GT = 15;
GenericSqlParser.GTE = 16;
GenericSqlParser.EQUALS = 17;
GenericSqlParser.NOT_EQUALS = 18;
GenericSqlParser.IS = 19;
GenericSqlParser.NULL = 20;
GenericSqlParser.CAST = 21;
GenericSqlParser.INDEXED_PARAM = 22;
GenericSqlParser.ID = 23;
GenericSqlParser.DIGIT = 24;
GenericSqlParser.QUOTED_ID = 25;
GenericSqlParser.STRING = 26;
GenericSqlParser.WHITESPACE = 27;

GenericSqlParser.RULE_statement = 0;
GenericSqlParser.RULE_query = 1;
GenericSqlParser.RULE_fromTables = 2;
GenericSqlParser.RULE_selectFields = 3;
GenericSqlParser.RULE_field = 4;
GenericSqlParser.RULE_aliasField = 5;
GenericSqlParser.RULE_boolExp = 6;
GenericSqlParser.RULE_exp = 7;
GenericSqlParser.RULE_numeric = 8;
GenericSqlParser.RULE_binaryOperator = 9;
GenericSqlParser.RULE_unaryOperator = 10;
GenericSqlParser.RULE_idPath = 11;
GenericSqlParser.RULE_identifier = 12;


function StatementContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_statement;
    return this;
}

StatementContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
StatementContext.prototype.constructor = StatementContext;

StatementContext.prototype.query = function() {
    return this.getTypedRuleContext(QueryContext,0);
};

StatementContext.prototype.EOF = function() {
    return this.getToken(GenericSqlParser.EOF, 0);
};

StatementContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterStatement(this);
	}
};

StatementContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitStatement(this);
	}
};




GenericSqlParser.StatementContext = StatementContext;

GenericSqlParser.prototype.statement = function() {

    var localctx = new StatementContext(this, this._ctx, this.state);
    this.enterRule(localctx, 0, GenericSqlParser.RULE_statement);
    try {
        this.state = 34;
        this._errHandler.sync(this);
        switch(this._input.LA(1)) {
        case GenericSqlParser.SELECT:
            this.enterOuterAlt(localctx, 1);
            this.state = 26;
            this.query();
            this.state = 27;
            this.match(GenericSqlParser.EOF);
            break;
        case GenericSqlParser.T__0:
            this.enterOuterAlt(localctx, 2);
            this.state = 29;
            this.match(GenericSqlParser.T__0);
            this.state = 30;
            this.query();
            this.state = 31;
            this.match(GenericSqlParser.T__1);
            this.state = 32;
            this.match(GenericSqlParser.EOF);
            break;
        default:
            throw new antlr4.error.NoViableAltException(this);
        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function QueryContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_query;
    this.from = null; // FromTablesContext
    this.where = null; // BoolExpContext
    return this;
}

QueryContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
QueryContext.prototype.constructor = QueryContext;

QueryContext.prototype.SELECT = function() {
    return this.getToken(GenericSqlParser.SELECT, 0);
};

QueryContext.prototype.selectFields = function() {
    return this.getTypedRuleContext(SelectFieldsContext,0);
};

QueryContext.prototype.FROM = function() {
    return this.getToken(GenericSqlParser.FROM, 0);
};

QueryContext.prototype.fromTables = function() {
    return this.getTypedRuleContext(FromTablesContext,0);
};

QueryContext.prototype.WHERE = function() {
    return this.getToken(GenericSqlParser.WHERE, 0);
};

QueryContext.prototype.boolExp = function() {
    return this.getTypedRuleContext(BoolExpContext,0);
};

QueryContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterQuery(this);
	}
};

QueryContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitQuery(this);
	}
};




GenericSqlParser.QueryContext = QueryContext;

GenericSqlParser.prototype.query = function() {

    var localctx = new QueryContext(this, this._ctx, this.state);
    this.enterRule(localctx, 2, GenericSqlParser.RULE_query);
    var _la = 0; // Token type
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 36;
        this.match(GenericSqlParser.SELECT);
        this.state = 37;
        this.selectFields();
        this.state = 38;
        this.match(GenericSqlParser.FROM);
        this.state = 39;
        localctx.from = this.fromTables();
        this.state = 42;
        this._errHandler.sync(this);
        _la = this._input.LA(1);
        if(_la===GenericSqlParser.WHERE) {
            this.state = 40;
            this.match(GenericSqlParser.WHERE);
            this.state = 41;
            localctx.where = this.boolExp();
        }

    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function FromTablesContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_fromTables;
    return this;
}

FromTablesContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
FromTablesContext.prototype.constructor = FromTablesContext;

FromTablesContext.prototype.aliasField = function() {
    return this.getTypedRuleContext(AliasFieldContext,0);
};

FromTablesContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterFromTables(this);
	}
};

FromTablesContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitFromTables(this);
	}
};




GenericSqlParser.FromTablesContext = FromTablesContext;

GenericSqlParser.prototype.fromTables = function() {

    var localctx = new FromTablesContext(this, this._ctx, this.state);
    this.enterRule(localctx, 4, GenericSqlParser.RULE_fromTables);
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 44;
        this.aliasField();
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function SelectFieldsContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_selectFields;
    return this;
}

SelectFieldsContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
SelectFieldsContext.prototype.constructor = SelectFieldsContext;

SelectFieldsContext.prototype.field = function(i) {
    if(i===undefined) {
        i = null;
    }
    if(i===null) {
        return this.getTypedRuleContexts(FieldContext);
    } else {
        return this.getTypedRuleContext(FieldContext,i);
    }
};

SelectFieldsContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterSelectFields(this);
	}
};

SelectFieldsContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitSelectFields(this);
	}
};




GenericSqlParser.SelectFieldsContext = SelectFieldsContext;

GenericSqlParser.prototype.selectFields = function() {

    var localctx = new SelectFieldsContext(this, this._ctx, this.state);
    this.enterRule(localctx, 6, GenericSqlParser.RULE_selectFields);
    var _la = 0; // Token type
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 46;
        this.field();
        this.state = 51;
        this._errHandler.sync(this);
        _la = this._input.LA(1);
        while(_la===GenericSqlParser.T__2) {
            this.state = 47;
            this.match(GenericSqlParser.T__2);
            this.state = 48;
            this.field();
            this.state = 53;
            this._errHandler.sync(this);
            _la = this._input.LA(1);
        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function FieldContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_field;
    return this;
}

FieldContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
FieldContext.prototype.constructor = FieldContext;

FieldContext.prototype.aliasField = function() {
    return this.getTypedRuleContext(AliasFieldContext,0);
};

FieldContext.prototype.ASTERISK = function() {
    return this.getToken(GenericSqlParser.ASTERISK, 0);
};

FieldContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterField(this);
	}
};

FieldContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitField(this);
	}
};




GenericSqlParser.FieldContext = FieldContext;

GenericSqlParser.prototype.field = function() {

    var localctx = new FieldContext(this, this._ctx, this.state);
    this.enterRule(localctx, 8, GenericSqlParser.RULE_field);
    try {
        this.state = 56;
        this._errHandler.sync(this);
        switch(this._input.LA(1)) {
        case GenericSqlParser.ID:
        case GenericSqlParser.QUOTED_ID:
            this.enterOuterAlt(localctx, 1);
            this.state = 54;
            this.aliasField();
            break;
        case GenericSqlParser.ASTERISK:
            this.enterOuterAlt(localctx, 2);
            this.state = 55;
            this.match(GenericSqlParser.ASTERISK);
            break;
        default:
            throw new antlr4.error.NoViableAltException(this);
        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function AliasFieldContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_aliasField;
    return this;
}

AliasFieldContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
AliasFieldContext.prototype.constructor = AliasFieldContext;

AliasFieldContext.prototype.idPath = function() {
    return this.getTypedRuleContext(IdPathContext,0);
};

AliasFieldContext.prototype.identifier = function() {
    return this.getTypedRuleContext(IdentifierContext,0);
};

AliasFieldContext.prototype.AS = function() {
    return this.getToken(GenericSqlParser.AS, 0);
};

AliasFieldContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterAliasField(this);
	}
};

AliasFieldContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitAliasField(this);
	}
};




GenericSqlParser.AliasFieldContext = AliasFieldContext;

GenericSqlParser.prototype.aliasField = function() {

    var localctx = new AliasFieldContext(this, this._ctx, this.state);
    this.enterRule(localctx, 10, GenericSqlParser.RULE_aliasField);
    var _la = 0; // Token type
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 58;
        this.idPath();
        this.state = 63;
        this._errHandler.sync(this);
        _la = this._input.LA(1);
        if((((_la) & ~0x1f) == 0 && ((1 << _la) & ((1 << GenericSqlParser.AS) | (1 << GenericSqlParser.ID) | (1 << GenericSqlParser.QUOTED_ID))) !== 0)) {
            this.state = 60;
            this._errHandler.sync(this);
            _la = this._input.LA(1);
            if(_la===GenericSqlParser.AS) {
                this.state = 59;
                this.match(GenericSqlParser.AS);
            }

            this.state = 62;
            this.identifier();
        }

    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function BoolExpContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_boolExp;
    return this;
}

BoolExpContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
BoolExpContext.prototype.constructor = BoolExpContext;

BoolExpContext.prototype.exp = function(i) {
    if(i===undefined) {
        i = null;
    }
    if(i===null) {
        return this.getTypedRuleContexts(ExpContext);
    } else {
        return this.getTypedRuleContext(ExpContext,i);
    }
};

BoolExpContext.prototype.AND = function() {
    return this.getToken(GenericSqlParser.AND, 0);
};

BoolExpContext.prototype.OR = function() {
    return this.getToken(GenericSqlParser.OR, 0);
};

BoolExpContext.prototype.NOT = function() {
    return this.getToken(GenericSqlParser.NOT, 0);
};

BoolExpContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterBoolExp(this);
	}
};

BoolExpContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitBoolExp(this);
	}
};




GenericSqlParser.BoolExpContext = BoolExpContext;

GenericSqlParser.prototype.boolExp = function() {

    var localctx = new BoolExpContext(this, this._ctx, this.state);
    this.enterRule(localctx, 12, GenericSqlParser.RULE_boolExp);
    try {
        this.state = 76;
        this._errHandler.sync(this);
        var la_ = this._interp.adaptivePredict(this._input,6,this._ctx);
        switch(la_) {
        case 1:
            this.enterOuterAlt(localctx, 1);
            this.state = 65;
            this.exp(0);
            break;

        case 2:
            this.enterOuterAlt(localctx, 2);
            this.state = 66;
            this.exp(0);
            this.state = 67;
            this.match(GenericSqlParser.AND);
            this.state = 68;
            this.exp(0);
            break;

        case 3:
            this.enterOuterAlt(localctx, 3);
            this.state = 70;
            this.exp(0);
            this.state = 71;
            this.match(GenericSqlParser.OR);
            this.state = 72;
            this.exp(0);
            break;

        case 4:
            this.enterOuterAlt(localctx, 4);
            this.state = 74;
            this.match(GenericSqlParser.NOT);
            this.state = 75;
            this.exp(0);
            break;

        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function ExpContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_exp;
    return this;
}

ExpContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
ExpContext.prototype.constructor = ExpContext;

ExpContext.prototype.idPath = function() {
    return this.getTypedRuleContext(IdPathContext,0);
};

ExpContext.prototype.identifier = function() {
    return this.getTypedRuleContext(IdentifierContext,0);
};

ExpContext.prototype.exp = function(i) {
    if(i===undefined) {
        i = null;
    }
    if(i===null) {
        return this.getTypedRuleContexts(ExpContext);
    } else {
        return this.getTypedRuleContext(ExpContext,i);
    }
};

ExpContext.prototype.CAST = function() {
    return this.getToken(GenericSqlParser.CAST, 0);
};

ExpContext.prototype.AS = function() {
    return this.getToken(GenericSqlParser.AS, 0);
};

ExpContext.prototype.STRING = function() {
    return this.getToken(GenericSqlParser.STRING, 0);
};

ExpContext.prototype.numeric = function() {
    return this.getTypedRuleContext(NumericContext,0);
};

ExpContext.prototype.INDEXED_PARAM = function() {
    return this.getToken(GenericSqlParser.INDEXED_PARAM, 0);
};

ExpContext.prototype.binaryOperator = function() {
    return this.getTypedRuleContext(BinaryOperatorContext,0);
};

ExpContext.prototype.unaryOperator = function() {
    return this.getTypedRuleContext(UnaryOperatorContext,0);
};

ExpContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterExp(this);
	}
};

ExpContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitExp(this);
	}
};



GenericSqlParser.prototype.exp = function(_p) {
	if(_p===undefined) {
	    _p = 0;
	}
    var _parentctx = this._ctx;
    var _parentState = this.state;
    var localctx = new ExpContext(this, this._ctx, _parentState);
    var _prevctx = localctx;
    var _startState = 14;
    this.enterRecursionRule(localctx, 14, GenericSqlParser.RULE_exp, _p);
    var _la = 0; // Token type
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 107;
        this._errHandler.sync(this);
        var la_ = this._interp.adaptivePredict(this._input,8,this._ctx);
        switch(la_) {
        case 1:
            this.state = 79;
            this.idPath();
            break;

        case 2:
            this.state = 80;
            this.identifier();
            this.state = 81;
            this.match(GenericSqlParser.T__0);

            this.state = 82;
            this.exp(0);
            this.state = 87;
            this._errHandler.sync(this);
            _la = this._input.LA(1);
            while(_la===GenericSqlParser.T__2) {
                this.state = 83;
                this.match(GenericSqlParser.T__2);
                this.state = 84;
                this.exp(0);
                this.state = 89;
                this._errHandler.sync(this);
                _la = this._input.LA(1);
            }
            this.state = 90;
            this.match(GenericSqlParser.T__1);
            break;

        case 3:
            this.state = 92;
            this.match(GenericSqlParser.CAST);
            this.state = 93;
            this.match(GenericSqlParser.T__0);
            this.state = 94;
            this.exp(0);
            this.state = 95;
            this.match(GenericSqlParser.AS);
            this.state = 96;
            this.identifier();
            this.state = 97;
            this.match(GenericSqlParser.T__1);
            break;

        case 4:
            this.state = 99;
            this.match(GenericSqlParser.STRING);
            break;

        case 5:
            this.state = 100;
            this.numeric();
            break;

        case 6:
            this.state = 101;
            this.identifier();
            break;

        case 7:
            this.state = 102;
            this.match(GenericSqlParser.INDEXED_PARAM);
            break;

        case 8:
            this.state = 103;
            this.match(GenericSqlParser.T__0);
            this.state = 104;
            this.exp(0);
            this.state = 105;
            this.match(GenericSqlParser.T__1);
            break;

        }
        this._ctx.stop = this._input.LT(-1);
        this.state = 117;
        this._errHandler.sync(this);
        var _alt = this._interp.adaptivePredict(this._input,10,this._ctx)
        while(_alt!=2 && _alt!=antlr4.atn.ATN.INVALID_ALT_NUMBER) {
            if(_alt===1) {
                if(this._parseListeners!==null) {
                    this.triggerExitRuleEvent();
                }
                _prevctx = localctx;
                this.state = 115;
                this._errHandler.sync(this);
                var la_ = this._interp.adaptivePredict(this._input,9,this._ctx);
                switch(la_) {
                case 1:
                    localctx = new ExpContext(this, _parentctx, _parentState);
                    this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_exp);
                    this.state = 109;
                    if (!( this.precpred(this._ctx, 10))) {
                        throw new antlr4.error.FailedPredicateException(this, "this.precpred(this._ctx, 10)");
                    }
                    this.state = 110;
                    this.binaryOperator();
                    this.state = 111;
                    this.exp(11);
                    break;

                case 2:
                    localctx = new ExpContext(this, _parentctx, _parentState);
                    this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_exp);
                    this.state = 113;
                    if (!( this.precpred(this._ctx, 9))) {
                        throw new antlr4.error.FailedPredicateException(this, "this.precpred(this._ctx, 9)");
                    }
                    this.state = 114;
                    this.unaryOperator();
                    break;

                } 
            }
            this.state = 119;
            this._errHandler.sync(this);
            _alt = this._interp.adaptivePredict(this._input,10,this._ctx);
        }

    } catch( error) {
        if(error instanceof antlr4.error.RecognitionException) {
	        localctx.exception = error;
	        this._errHandler.reportError(this, error);
	        this._errHandler.recover(this, error);
	    } else {
	    	throw error;
	    }
    } finally {
        this.unrollRecursionContexts(_parentctx)
    }
    return localctx;
};


function NumericContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_numeric;
    return this;
}

NumericContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
NumericContext.prototype.constructor = NumericContext;

NumericContext.prototype.DIGIT = function(i) {
	if(i===undefined) {
		i = null;
	}
    if(i===null) {
        return this.getTokens(GenericSqlParser.DIGIT);
    } else {
        return this.getToken(GenericSqlParser.DIGIT, i);
    }
};


NumericContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterNumeric(this);
	}
};

NumericContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitNumeric(this);
	}
};




GenericSqlParser.NumericContext = NumericContext;

GenericSqlParser.prototype.numeric = function() {

    var localctx = new NumericContext(this, this._ctx, this.state);
    this.enterRule(localctx, 16, GenericSqlParser.RULE_numeric);
    try {
        this.state = 139;
        this._errHandler.sync(this);
        switch(this._input.LA(1)) {
        case GenericSqlParser.DIGIT:
            this.enterOuterAlt(localctx, 1);
            this.state = 121; 
            this._errHandler.sync(this);
            var _alt = 1;
            do {
            	switch (_alt) {
            	case 1:
            		this.state = 120;
            		this.match(GenericSqlParser.DIGIT);
            		break;
            	default:
            		throw new antlr4.error.NoViableAltException(this);
            	}
            	this.state = 123; 
            	this._errHandler.sync(this);
            	_alt = this._interp.adaptivePredict(this._input,11, this._ctx);
            } while ( _alt!=2 && _alt!=antlr4.atn.ATN.INVALID_ALT_NUMBER );
            this.state = 131;
            this._errHandler.sync(this);
            var la_ = this._interp.adaptivePredict(this._input,13,this._ctx);
            if(la_===1) {
                this.state = 125;
                this.match(GenericSqlParser.T__3);
                this.state = 127; 
                this._errHandler.sync(this);
                var _alt = 1;
                do {
                	switch (_alt) {
                	case 1:
                		this.state = 126;
                		this.match(GenericSqlParser.DIGIT);
                		break;
                	default:
                		throw new antlr4.error.NoViableAltException(this);
                	}
                	this.state = 129; 
                	this._errHandler.sync(this);
                	_alt = this._interp.adaptivePredict(this._input,12, this._ctx);
                } while ( _alt!=2 && _alt!=antlr4.atn.ATN.INVALID_ALT_NUMBER );

            }
            break;
        case GenericSqlParser.T__3:
            this.enterOuterAlt(localctx, 2);
            this.state = 133;
            this.match(GenericSqlParser.T__3);
            this.state = 135; 
            this._errHandler.sync(this);
            var _alt = 1;
            do {
            	switch (_alt) {
            	case 1:
            		this.state = 134;
            		this.match(GenericSqlParser.DIGIT);
            		break;
            	default:
            		throw new antlr4.error.NoViableAltException(this);
            	}
            	this.state = 137; 
            	this._errHandler.sync(this);
            	_alt = this._interp.adaptivePredict(this._input,14, this._ctx);
            } while ( _alt!=2 && _alt!=antlr4.atn.ATN.INVALID_ALT_NUMBER );
            break;
        default:
            throw new antlr4.error.NoViableAltException(this);
        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function BinaryOperatorContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_binaryOperator;
    return this;
}

BinaryOperatorContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
BinaryOperatorContext.prototype.constructor = BinaryOperatorContext;

BinaryOperatorContext.prototype.LT = function() {
    return this.getToken(GenericSqlParser.LT, 0);
};

BinaryOperatorContext.prototype.LTE = function() {
    return this.getToken(GenericSqlParser.LTE, 0);
};

BinaryOperatorContext.prototype.GT = function() {
    return this.getToken(GenericSqlParser.GT, 0);
};

BinaryOperatorContext.prototype.GTE = function() {
    return this.getToken(GenericSqlParser.GTE, 0);
};

BinaryOperatorContext.prototype.EQUALS = function() {
    return this.getToken(GenericSqlParser.EQUALS, 0);
};

BinaryOperatorContext.prototype.NOT_EQUALS = function() {
    return this.getToken(GenericSqlParser.NOT_EQUALS, 0);
};

BinaryOperatorContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterBinaryOperator(this);
	}
};

BinaryOperatorContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitBinaryOperator(this);
	}
};




GenericSqlParser.BinaryOperatorContext = BinaryOperatorContext;

GenericSqlParser.prototype.binaryOperator = function() {

    var localctx = new BinaryOperatorContext(this, this._ctx, this.state);
    this.enterRule(localctx, 18, GenericSqlParser.RULE_binaryOperator);
    var _la = 0; // Token type
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 141;
        _la = this._input.LA(1);
        if(!((((_la) & ~0x1f) == 0 && ((1 << _la) & ((1 << GenericSqlParser.LT) | (1 << GenericSqlParser.LTE) | (1 << GenericSqlParser.GT) | (1 << GenericSqlParser.GTE) | (1 << GenericSqlParser.EQUALS) | (1 << GenericSqlParser.NOT_EQUALS))) !== 0))) {
        this._errHandler.recoverInline(this);
        }
        else {
        	this._errHandler.reportMatch(this);
            this.consume();
        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function UnaryOperatorContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_unaryOperator;
    return this;
}

UnaryOperatorContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
UnaryOperatorContext.prototype.constructor = UnaryOperatorContext;

UnaryOperatorContext.prototype.IS = function() {
    return this.getToken(GenericSqlParser.IS, 0);
};

UnaryOperatorContext.prototype.NULL = function() {
    return this.getToken(GenericSqlParser.NULL, 0);
};

UnaryOperatorContext.prototype.NOT = function() {
    return this.getToken(GenericSqlParser.NOT, 0);
};

UnaryOperatorContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterUnaryOperator(this);
	}
};

UnaryOperatorContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitUnaryOperator(this);
	}
};




GenericSqlParser.UnaryOperatorContext = UnaryOperatorContext;

GenericSqlParser.prototype.unaryOperator = function() {

    var localctx = new UnaryOperatorContext(this, this._ctx, this.state);
    this.enterRule(localctx, 20, GenericSqlParser.RULE_unaryOperator);
    try {
        this.state = 148;
        this._errHandler.sync(this);
        var la_ = this._interp.adaptivePredict(this._input,16,this._ctx);
        switch(la_) {
        case 1:
            this.enterOuterAlt(localctx, 1);
            this.state = 143;
            this.match(GenericSqlParser.IS);
            this.state = 144;
            this.match(GenericSqlParser.NULL);
            break;

        case 2:
            this.enterOuterAlt(localctx, 2);
            this.state = 145;
            this.match(GenericSqlParser.IS);
            this.state = 146;
            this.match(GenericSqlParser.NOT);
            this.state = 147;
            this.match(GenericSqlParser.NULL);
            break;

        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function IdPathContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_idPath;
    return this;
}

IdPathContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
IdPathContext.prototype.constructor = IdPathContext;

IdPathContext.prototype.identifier = function(i) {
    if(i===undefined) {
        i = null;
    }
    if(i===null) {
        return this.getTypedRuleContexts(IdentifierContext);
    } else {
        return this.getTypedRuleContext(IdentifierContext,i);
    }
};

IdPathContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterIdPath(this);
	}
};

IdPathContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitIdPath(this);
	}
};




GenericSqlParser.IdPathContext = IdPathContext;

GenericSqlParser.prototype.idPath = function() {

    var localctx = new IdPathContext(this, this._ctx, this.state);
    this.enterRule(localctx, 22, GenericSqlParser.RULE_idPath);
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 150;
        this.identifier();
        this.state = 155;
        this._errHandler.sync(this);
        var _alt = this._interp.adaptivePredict(this._input,17,this._ctx)
        while(_alt!=2 && _alt!=antlr4.atn.ATN.INVALID_ALT_NUMBER) {
            if(_alt===1) {
                this.state = 151;
                this.match(GenericSqlParser.T__3);
                this.state = 152;
                this.identifier(); 
            }
            this.state = 157;
            this._errHandler.sync(this);
            _alt = this._interp.adaptivePredict(this._input,17,this._ctx);
        }

    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


function IdentifierContext(parser, parent, invokingState) {
	if(parent===undefined) {
	    parent = null;
	}
	if(invokingState===undefined || invokingState===null) {
		invokingState = -1;
	}
	antlr4.ParserRuleContext.call(this, parent, invokingState);
    this.parser = parser;
    this.ruleIndex = GenericSqlParser.RULE_identifier;
    return this;
}

IdentifierContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
IdentifierContext.prototype.constructor = IdentifierContext;

IdentifierContext.prototype.ID = function() {
    return this.getToken(GenericSqlParser.ID, 0);
};

IdentifierContext.prototype.QUOTED_ID = function() {
    return this.getToken(GenericSqlParser.QUOTED_ID, 0);
};

IdentifierContext.prototype.enterRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.enterIdentifier(this);
	}
};

IdentifierContext.prototype.exitRule = function(listener) {
    if(listener instanceof GenericSqlListener ) {
        listener.exitIdentifier(this);
	}
};




GenericSqlParser.IdentifierContext = IdentifierContext;

GenericSqlParser.prototype.identifier = function() {

    var localctx = new IdentifierContext(this, this._ctx, this.state);
    this.enterRule(localctx, 24, GenericSqlParser.RULE_identifier);
    var _la = 0; // Token type
    try {
        this.enterOuterAlt(localctx, 1);
        this.state = 158;
        _la = this._input.LA(1);
        if(!(_la===GenericSqlParser.ID || _la===GenericSqlParser.QUOTED_ID)) {
        this._errHandler.recoverInline(this);
        }
        else {
        	this._errHandler.reportMatch(this);
            this.consume();
        }
    } catch (re) {
    	if(re instanceof antlr4.error.RecognitionException) {
	        localctx.exception = re;
	        this._errHandler.reportError(this, re);
	        this._errHandler.recover(this, re);
	    } else {
	    	throw re;
	    }
    } finally {
        this.exitRule();
    }
    return localctx;
};


GenericSqlParser.prototype.sempred = function(localctx, ruleIndex, predIndex) {
	switch(ruleIndex) {
	case 7:
			return this.exp_sempred(localctx, predIndex);
    default:
        throw "No predicate with index:" + ruleIndex;
   }
};

GenericSqlParser.prototype.exp_sempred = function(localctx, predIndex) {
	switch(predIndex) {
		case 0:
			return this.precpred(this._ctx, 10);
		case 1:
			return this.precpred(this._ctx, 9);
		default:
			throw "No predicate with index:" + predIndex;
	}
};


exports.GenericSqlParser = GenericSqlParser;
