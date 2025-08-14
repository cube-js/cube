// Generated from src/parser/Python3Lexer.g4 by ANTLR 4.13.2
// noinspection ES6UnusedImports,JSUnusedGlobalSymbols,JSUnusedLocalSymbols
// @ts-nocheck
import {
	ATN,
	ATNDeserializer,
	CharStream,
	DecisionState, DFA,
	Lexer,
	LexerATNSimulator,
	RuleContext,
	PredictionContextCache,
	Token
} from "antlr4";

import Python3Parser from './Python3Parser';

export default class Python3Lexer extends Lexer {
	public static readonly INDENT = 1;
	public static readonly DEDENT = 2;
	public static readonly SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START = 3;
	public static readonly DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START = 4;
	public static readonly SINGLE_QUOTE_LONG_TEMPLATE_STRING_START = 5;
	public static readonly DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START = 6;
	public static readonly STRING = 7;
	public static readonly NUMBER = 8;
	public static readonly INTEGER = 9;
	public static readonly DEF = 10;
	public static readonly RETURN = 11;
	public static readonly RAISE = 12;
	public static readonly FROM = 13;
	public static readonly IMPORT = 14;
	public static readonly AS = 15;
	public static readonly GLOBAL = 16;
	public static readonly NONLOCAL = 17;
	public static readonly ASSERT = 18;
	public static readonly IF = 19;
	public static readonly ELIF = 20;
	public static readonly ELSE = 21;
	public static readonly WHILE = 22;
	public static readonly FOR = 23;
	public static readonly IN = 24;
	public static readonly TRY = 25;
	public static readonly FINALLY = 26;
	public static readonly WITH = 27;
	public static readonly EXCEPT = 28;
	public static readonly LAMBDA = 29;
	public static readonly OR = 30;
	public static readonly AND = 31;
	public static readonly NOT = 32;
	public static readonly IS = 33;
	public static readonly NONE = 34;
	public static readonly TRUE = 35;
	public static readonly FALSE = 36;
	public static readonly CLASS = 37;
	public static readonly YIELD = 38;
	public static readonly DEL = 39;
	public static readonly PASS = 40;
	public static readonly CONTINUE = 41;
	public static readonly BREAK = 42;
	public static readonly ASYNC = 43;
	public static readonly AWAIT = 44;
	public static readonly NEWLINE = 45;
	public static readonly NAME = 46;
	public static readonly STRING_LITERAL = 47;
	public static readonly BYTES_LITERAL = 48;
	public static readonly DECIMAL_INTEGER = 49;
	public static readonly OCT_INTEGER = 50;
	public static readonly HEX_INTEGER = 51;
	public static readonly BIN_INTEGER = 52;
	public static readonly FLOAT_NUMBER = 53;
	public static readonly IMAG_NUMBER = 54;
	public static readonly DOT = 55;
	public static readonly ELLIPSIS = 56;
	public static readonly STAR = 57;
	public static readonly OPEN_PAREN = 58;
	public static readonly CLOSE_PAREN = 59;
	public static readonly COMMA = 60;
	public static readonly COLON = 61;
	public static readonly SEMI_COLON = 62;
	public static readonly POWER = 63;
	public static readonly ASSIGN = 64;
	public static readonly OPEN_BRACK = 65;
	public static readonly CLOSE_BRACK = 66;
	public static readonly OR_OP = 67;
	public static readonly XOR = 68;
	public static readonly AND_OP = 69;
	public static readonly LEFT_SHIFT = 70;
	public static readonly RIGHT_SHIFT = 71;
	public static readonly ADD = 72;
	public static readonly MINUS = 73;
	public static readonly DIV = 74;
	public static readonly MOD = 75;
	public static readonly IDIV = 76;
	public static readonly NOT_OP = 77;
	public static readonly OPEN_BRACE = 78;
	public static readonly TEMPLATE_CLOSE_BRACE = 79;
	public static readonly CLOSE_BRACE = 80;
	public static readonly LESS_THAN = 81;
	public static readonly GREATER_THAN = 82;
	public static readonly EQUALS = 83;
	public static readonly GT_EQ = 84;
	public static readonly LT_EQ = 85;
	public static readonly NOT_EQ_1 = 86;
	public static readonly NOT_EQ_2 = 87;
	public static readonly AT = 88;
	public static readonly ARROW = 89;
	public static readonly ADD_ASSIGN = 90;
	public static readonly SUB_ASSIGN = 91;
	public static readonly MULT_ASSIGN = 92;
	public static readonly AT_ASSIGN = 93;
	public static readonly DIV_ASSIGN = 94;
	public static readonly MOD_ASSIGN = 95;
	public static readonly AND_ASSIGN = 96;
	public static readonly OR_ASSIGN = 97;
	public static readonly XOR_ASSIGN = 98;
	public static readonly LEFT_SHIFT_ASSIGN = 99;
	public static readonly RIGHT_SHIFT_ASSIGN = 100;
	public static readonly POWER_ASSIGN = 101;
	public static readonly IDIV_ASSIGN = 102;
	public static readonly QUOTE = 103;
	public static readonly DOUBLE_QUOTE = 104;
	public static readonly SKIP_ = 105;
	public static readonly UNKNOWN_CHAR = 106;
	public static readonly SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END = 107;
	public static readonly SINGLE_QUOTE_LONG_TEMPLATE_STRING_END = 108;
	public static readonly SINGLE_QUOTE_STRING_ATOM = 109;
	public static readonly DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END = 110;
	public static readonly DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END = 111;
	public static readonly DOUBLE_QUOTE_STRING_ATOM = 112;
	public static readonly EOF = Token.EOF;
	public static readonly SINGLE_TEMPLATE = 1;
	public static readonly DOUBLE_TEMPLATE = 2;

	public static readonly channelNames: string[] = [ "DEFAULT_TOKEN_CHANNEL", "HIDDEN" ];
	public static readonly literalNames: (string | null)[] = [ null, null,
                                                            null, null,
                                                            null, null,
                                                            null, null,
                                                            null, null,
                                                            "'def'", "'return'",
                                                            "'raise'", "'from'",
                                                            "'import'",
                                                            "'as'", "'global'",
                                                            "'nonlocal'",
                                                            "'assert'",
                                                            "'if'", "'elif'",
                                                            "'else'", "'while'",
                                                            "'for'", "'in'",
                                                            "'try'", "'finally'",
                                                            "'with'", "'except'",
                                                            "'lambda'",
                                                            "'or'", "'and'",
                                                            "'not'", "'is'",
                                                            "'None'", "'True'",
                                                            "'False'", "'class'",
                                                            "'yield'", "'del'",
                                                            "'pass'", "'continue'",
                                                            "'break'", "'async'",
                                                            "'await'", null,
                                                            null, null,
                                                            null, null,
                                                            null, null,
                                                            null, null,
                                                            null, "'.'",
                                                            "'...'", "'*'",
                                                            "'('", "')'",
                                                            "','", "':'",
                                                            "';'", "'**'",
                                                            "'='", "'['",
                                                            "']'", "'|'",
                                                            "'^'", "'&'",
                                                            "'<<'", "'>>'",
                                                            "'+'", "'-'",
                                                            "'/'", "'%'",
                                                            "'//'", "'~'",
                                                            "'{'", null,
                                                            "'}'", "'<'",
                                                            "'>'", "'=='",
                                                            "'>='", "'<='",
                                                            "'<>'", "'!='",
                                                            "'@'", "'->'",
                                                            "'+='", "'-='",
                                                            "'*='", "'@='",
                                                            "'/='", "'%='",
                                                            "'&='", "'|='",
                                                            "'^='", "'<<='",
                                                            "'>>='", "'**='",
                                                            "'//='", "'''",
                                                            "'\"'" ];
	public static readonly symbolicNames: (string | null)[] = [ null, "INDENT",
                                                             "DEDENT", "SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START",
                                                             "DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START",
                                                             "SINGLE_QUOTE_LONG_TEMPLATE_STRING_START",
                                                             "DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START",
                                                             "STRING", "NUMBER",
                                                             "INTEGER",
                                                             "DEF", "RETURN",
                                                             "RAISE", "FROM",
                                                             "IMPORT", "AS",
                                                             "GLOBAL", "NONLOCAL",
                                                             "ASSERT", "IF",
                                                             "ELIF", "ELSE",
                                                             "WHILE", "FOR",
                                                             "IN", "TRY",
                                                             "FINALLY",
                                                             "WITH", "EXCEPT",
                                                             "LAMBDA", "OR",
                                                             "AND", "NOT",
                                                             "IS", "NONE",
                                                             "TRUE", "FALSE",
                                                             "CLASS", "YIELD",
                                                             "DEL", "PASS",
                                                             "CONTINUE",
                                                             "BREAK", "ASYNC",
                                                             "AWAIT", "NEWLINE",
                                                             "NAME", "STRING_LITERAL",
                                                             "BYTES_LITERAL",
                                                             "DECIMAL_INTEGER",
                                                             "OCT_INTEGER",
                                                             "HEX_INTEGER",
                                                             "BIN_INTEGER",
                                                             "FLOAT_NUMBER",
                                                             "IMAG_NUMBER",
                                                             "DOT", "ELLIPSIS",
                                                             "STAR", "OPEN_PAREN",
                                                             "CLOSE_PAREN",
                                                             "COMMA", "COLON",
                                                             "SEMI_COLON",
                                                             "POWER", "ASSIGN",
                                                             "OPEN_BRACK",
                                                             "CLOSE_BRACK",
                                                             "OR_OP", "XOR",
                                                             "AND_OP", "LEFT_SHIFT",
                                                             "RIGHT_SHIFT",
                                                             "ADD", "MINUS",
                                                             "DIV", "MOD",
                                                             "IDIV", "NOT_OP",
                                                             "OPEN_BRACE",
                                                             "TEMPLATE_CLOSE_BRACE",
                                                             "CLOSE_BRACE",
                                                             "LESS_THAN",
                                                             "GREATER_THAN",
                                                             "EQUALS", "GT_EQ",
                                                             "LT_EQ", "NOT_EQ_1",
                                                             "NOT_EQ_2",
                                                             "AT", "ARROW",
                                                             "ADD_ASSIGN",
                                                             "SUB_ASSIGN",
                                                             "MULT_ASSIGN",
                                                             "AT_ASSIGN",
                                                             "DIV_ASSIGN",
                                                             "MOD_ASSIGN",
                                                             "AND_ASSIGN",
                                                             "OR_ASSIGN",
                                                             "XOR_ASSIGN",
                                                             "LEFT_SHIFT_ASSIGN",
                                                             "RIGHT_SHIFT_ASSIGN",
                                                             "POWER_ASSIGN",
                                                             "IDIV_ASSIGN",
                                                             "QUOTE", "DOUBLE_QUOTE",
                                                             "SKIP_", "UNKNOWN_CHAR",
                                                             "SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END",
                                                             "SINGLE_QUOTE_LONG_TEMPLATE_STRING_END",
                                                             "SINGLE_QUOTE_STRING_ATOM",
                                                             "DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END",
                                                             "DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END",
                                                             "DOUBLE_QUOTE_STRING_ATOM" ];
	public static readonly modeNames: string[] = [ "DEFAULT_MODE", "SINGLE_TEMPLATE",
                                                "DOUBLE_TEMPLATE", ];

	public static readonly ruleNames: string[] = [
		"SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START", "DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START",
		"SINGLE_QUOTE_LONG_TEMPLATE_STRING_START", "DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START",
		"STRING", "NUMBER", "INTEGER", "DEF", "RETURN", "RAISE", "FROM", "IMPORT",
		"AS", "GLOBAL", "NONLOCAL", "ASSERT", "IF", "ELIF", "ELSE", "WHILE", "FOR",
		"IN", "TRY", "FINALLY", "WITH", "EXCEPT", "LAMBDA", "OR", "AND", "NOT",
		"IS", "NONE", "TRUE", "FALSE", "CLASS", "YIELD", "DEL", "PASS", "CONTINUE",
		"BREAK", "ASYNC", "AWAIT", "NEWLINE", "NAME", "STRING_LITERAL", "BYTES_LITERAL",
		"DECIMAL_INTEGER", "OCT_INTEGER", "HEX_INTEGER", "BIN_INTEGER", "FLOAT_NUMBER",
		"IMAG_NUMBER", "DOT", "ELLIPSIS", "STAR", "OPEN_PAREN", "CLOSE_PAREN",
		"COMMA", "COLON", "SEMI_COLON", "POWER", "ASSIGN", "OPEN_BRACK", "CLOSE_BRACK",
		"OR_OP", "XOR", "AND_OP", "LEFT_SHIFT", "RIGHT_SHIFT", "ADD", "MINUS",
		"DIV", "MOD", "IDIV", "NOT_OP", "OPEN_BRACE", "TEMPLATE_CLOSE_BRACE",
		"CLOSE_BRACE", "LESS_THAN", "GREATER_THAN", "EQUALS", "GT_EQ", "LT_EQ",
		"NOT_EQ_1", "NOT_EQ_2", "AT", "ARROW", "ADD_ASSIGN", "SUB_ASSIGN", "MULT_ASSIGN",
		"AT_ASSIGN", "DIV_ASSIGN", "MOD_ASSIGN", "AND_ASSIGN", "OR_ASSIGN", "XOR_ASSIGN",
		"LEFT_SHIFT_ASSIGN", "RIGHT_SHIFT_ASSIGN", "POWER_ASSIGN", "IDIV_ASSIGN",
		"QUOTE", "DOUBLE_QUOTE", "SKIP_", "UNKNOWN_CHAR", "SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END",
		"SINGLE_QUOTE_LONG_TEMPLATE_STRING_END", "SINGLE_TEMPLATE_START", "SINGLE_QUOTE_STRING_ATOM",
		"DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END", "DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END",
		"DOUBLE_TEMPLATE_START", "DOUBLE_QUOTE_STRING_ATOM", "TEMPLATE_FORMAT_PREFIX",
		"SHORT_STRING_TEMPLATE", "LONG_STRING_TEMPLATE", "SHORT_STRING", "LONG_STRING",
		"LONG_STRING_ITEM", "LONG_STRING_CHAR", "STRING_ESCAPE_SEQ", "NON_ZERO_DIGIT",
		"DIGIT", "OCT_DIGIT", "HEX_DIGIT", "BIN_DIGIT", "POINT_FLOAT", "EXPONENT_FLOAT",
		"INT_PART", "FRACTION", "EXPONENT", "SHORT_BYTES", "LONG_BYTES", "LONG_BYTES_ITEM",
		"SHORT_BYTES_CHAR_NO_SINGLE_QUOTE", "SHORT_BYTES_CHAR_NO_DOUBLE_QUOTE",
		"LONG_BYTES_CHAR", "BYTES_ESCAPE_SEQ", "SPACES", "COMMENT", "LINE_JOINING",
		"ID_START", "ID_CONTINUE",
	];


	  private token_queue: Token[] = [];
	  private indents: number[] = [];
	  private opened: number = 0;
	  private templateDepth: number = 0;
	  private last_token: Token|undefined = undefined;

	  // @Override
	  public reset(): void {
	    // A queue where extra tokens are pushed on (see the NEWLINE lexer rule).
	    this.token_queue = [];

	    // The stack that keeps track of the indentation level.
	    this.indents = [];

	    // The amount of opened braces, brackets and parenthesis.
	    this.opened = 0;

	    this.templateDepth = 0;

	    super.reset();
	  };

	  // @Override
	  public emit(token?: Token): Token {
	    if (token) {
	      token = super.emit(token);
	    } else {
	      token = super.emit();
	    }
	    this.token_queue.push(token);
	    return token;
	  };

	  /**
	   * Return the next token from the character stream and records this last
	   * token in case it resides on the default channel. This recorded token
	   * is used to determine when the lexer could possibly match a regex
	   * literal.
	   *
	   */
	  // @Override
	  public nextToken(): Token {
	    // Check if the end-of-file is ahead and there are still some DEDENTS expected.
	    if (this.inputStream.LA(1) === Python3Parser.EOF && this.indents.length) {

	      // Remove any trailing EOF tokens from our buffer.
	      this.token_queue = this.token_queue.filter(function(val) {
	        return val.type !== Python3Parser.EOF;
	      });

	      // First emit an extra line break that serves as the end of the statement.
	      this.emit(this.commonToken(Python3Parser.NEWLINE, "\n"));

	      // Now emit as much DEDENT tokens as needed.
	      while (this.indents.length) {
	        this.emit(this.createDedent());
	        this.indents.pop();
	      }

	      // Put the EOF back on the token stream.
	      this.emit(this.commonToken(Python3Parser.EOF, "<EOF>"));
	    }

	    let next = super.nextToken();

	    if (next.channel == Token.DEFAULT_CHANNEL) {
	      // Keep track of the last token on the default channel.
	      this.last_token = next;
	    }

	    return this.token_queue.shift() || next;
	  }

	  private createDedent(): Token {
	    let dedent = this.commonToken(Python3Parser.DEDENT, "");
	    if (this.last_token) {
	      dedent.line = this.last_token.line;
	    }
	    return dedent;
	  }

	  private commonToken(type: number, text: string): CommonToken {
	    let stop: number = this.charIndex - 1;
	    let start: number = text.length ? stop - text.length + 1 : stop;
	    return new CommonToken(type, text, this._tokenFactorySourcePair, Lexer.DEFAULT_TOKEN_CHANNEL, start, stop);
	  }

	  // Calculates the indentation of the provided spaces, taking the
	  // following rules into account:
	  //
	  // "Tabs are replaced (from left to right) by one to eight spaces
	  //  such that the total number of characters up to and including
	  //  the replacement is a multiple of eight [...]"
	  //
	  //  -- https://docs.python.org/3.1/reference/lexical_analysis.html#indentation
	  private getIndentationCount(whitespace: string): number {
	    let count = 0;
	    for (let i = 0; i < whitespace.length; i++) {
	      if (whitespace[i] === '\t') {
	        count += 8 - count % 8;
	      } else {
	        count++;
	      }
	    }
	    return count;
	  }

	  private atStartOfInput(): boolean {
	    return this.charIndex === 0;
	  }


	constructor(input: CharStream) {
		super(input);
		this._interp = new LexerATNSimulator(this, Python3Lexer._ATN, Python3Lexer.DecisionsToDFA, new PredictionContextCache());
	}

	public get grammarFileName(): string { return "Python3Lexer.g4"; }

	public get literalNames(): (string | null)[] { return Python3Lexer.literalNames; }
	public get symbolicNames(): (string | null)[] { return Python3Lexer.symbolicNames; }
	public get ruleNames(): string[] { return Python3Lexer.ruleNames; }

	public get serializedATN(): number[] { return Python3Lexer._serializedATN; }

	public get channelNames(): string[] { return Python3Lexer.channelNames; }

	public get modeNames(): string[] { return Python3Lexer.modeNames; }

	// @Override
	public action(localctx: RuleContext, ruleIndex: number, actionIndex: number): void {
		switch (ruleIndex) {
		case 0:
			this.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START_action(localctx, actionIndex);
			break;
		case 1:
			this.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START_action(localctx, actionIndex);
			break;
		case 2:
			this.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START_action(localctx, actionIndex);
			break;
		case 3:
			this.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START_action(localctx, actionIndex);
			break;
		case 42:
			this.NEWLINE_action(localctx, actionIndex);
			break;
		case 55:
			this.OPEN_PAREN_action(localctx, actionIndex);
			break;
		case 56:
			this.CLOSE_PAREN_action(localctx, actionIndex);
			break;
		case 62:
			this.OPEN_BRACK_action(localctx, actionIndex);
			break;
		case 63:
			this.CLOSE_BRACK_action(localctx, actionIndex);
			break;
		case 75:
			this.OPEN_BRACE_action(localctx, actionIndex);
			break;
		case 77:
			this.CLOSE_BRACE_action(localctx, actionIndex);
			break;
		case 104:
			this.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END_action(localctx, actionIndex);
			break;
		case 105:
			this.SINGLE_QUOTE_LONG_TEMPLATE_STRING_END_action(localctx, actionIndex);
			break;
		case 108:
			this.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END_action(localctx, actionIndex);
			break;
		case 109:
			this.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END_action(localctx, actionIndex);
			break;
		}
	}
	private SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 0:
			this.templateDepth++
			break;
		}
	}
	private DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 1:
			this.templateDepth++
			break;
		}
	}
	private SINGLE_QUOTE_LONG_TEMPLATE_STRING_START_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 2:
			this.templateDepth++
			break;
		}
	}
	private DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 3:
			this.templateDepth++
			break;
		}
	}
	private NEWLINE_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 4:

			     let newLine = this.text.replace(/[^\r\n]+/g, '');
			     let spaces = this.text.replace(/[\r\n]+/g, '');

			     // Strip newlines inside open clauses except if we are near EOF. We keep NEWLINEs near EOF to
			     // satisfy the final newline needed by the single_put rule used by the REPL.
			     let next = this.inputStream.LA(1);
			     let nextnext = this.inputStream.LA(2);
			     if (this.opened > 0 || (nextnext != -1 /* EOF */ && (next === 13 /* '\r' */ || next === 10 /* '\n' */ || next === 35 /* '#' */))) {
			       // If we're inside a list or on a blank line, ignore all indents,
			       // dedents and line breaks.
			       this.skip();
			     } else {
			       this.emit(this.commonToken(Python3Parser.NEWLINE, newLine));

			       let indent = this.getIndentationCount(spaces);
			       let previous = this.indents.length ? this.indents[this.indents.length - 1] : 0;

			       if (indent === previous) {
			         // skip indents of the same size as the present indent-size
			         this.skip();
			       } else if (indent > previous) {
			         this.indents.push(indent);
			         this.emit(this.commonToken(Python3Parser.INDENT, spaces));
			       } else {
			         // Possibly emit more than 1 DEDENT token.
			         while (this.indents.length && this.indents[this.indents.length - 1] > indent) {
			           this.emit(this.createDedent());
			           this.indents.pop();
			         }
			       }
			     }

			break;
		}
	}
	private OPEN_PAREN_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 5:
			this.opened++;
			break;
		}
	}
	private CLOSE_PAREN_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 6:
			this.opened--;
			break;
		}
	}
	private OPEN_BRACK_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 7:
			this.opened++;
			break;
		}
	}
	private CLOSE_BRACK_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 8:
			this.opened--;
			break;
		}
	}
	private OPEN_BRACE_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 9:
			this.opened++;
			break;
		}
	}
	private CLOSE_BRACE_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 10:
			this.opened--;
			break;
		}
	}
	private SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 11:
			this.templateDepth--
			break;
		}
	}
	private SINGLE_QUOTE_LONG_TEMPLATE_STRING_END_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 12:
			this.templateDepth--
			break;
		}
	}
	private DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 13:
			this.templateDepth--
			break;
		}
	}
	private DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END_action(localctx: RuleContext, actionIndex: number): void {
		switch (actionIndex) {
		case 14:
			this.templateDepth--
			break;
		}
	}
	// @Override
	public sempred(localctx: RuleContext, ruleIndex: number, predIndex: number): boolean {
		switch (ruleIndex) {
		case 42:
			return this.NEWLINE_sempred(localctx, predIndex);
		case 76:
			return this.TEMPLATE_CLOSE_BRACE_sempred(localctx, predIndex);
		}
		return true;
	}
	private NEWLINE_sempred(localctx: RuleContext, predIndex: number): boolean {
		switch (predIndex) {
		case 0:
			return this.atStartOfInput();
		}
		return true;
	}
	private TEMPLATE_CLOSE_BRACE_sempred(localctx: RuleContext, predIndex: number): boolean {
		switch (predIndex) {
		case 1:
			return this.templateDepth > 0;
		}
		return true;
	}

	public static readonly _serializedATN: number[] = [4,0,112,1079,6,-1,6,
	-1,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,7,7,7,
	2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,14,2,15,
	7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,7,21,2,22,7,
	22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,7,28,2,29,7,29,
	2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,34,7,34,2,35,7,35,2,36,7,36,2,
	37,7,37,2,38,7,38,2,39,7,39,2,40,7,40,2,41,7,41,2,42,7,42,2,43,7,43,2,44,
	7,44,2,45,7,45,2,46,7,46,2,47,7,47,2,48,7,48,2,49,7,49,2,50,7,50,2,51,7,
	51,2,52,7,52,2,53,7,53,2,54,7,54,2,55,7,55,2,56,7,56,2,57,7,57,2,58,7,58,
	2,59,7,59,2,60,7,60,2,61,7,61,2,62,7,62,2,63,7,63,2,64,7,64,2,65,7,65,2,
	66,7,66,2,67,7,67,2,68,7,68,2,69,7,69,2,70,7,70,2,71,7,71,2,72,7,72,2,73,
	7,73,2,74,7,74,2,75,7,75,2,76,7,76,2,77,7,77,2,78,7,78,2,79,7,79,2,80,7,
	80,2,81,7,81,2,82,7,82,2,83,7,83,2,84,7,84,2,85,7,85,2,86,7,86,2,87,7,87,
	2,88,7,88,2,89,7,89,2,90,7,90,2,91,7,91,2,92,7,92,2,93,7,93,2,94,7,94,2,
	95,7,95,2,96,7,96,2,97,7,97,2,98,7,98,2,99,7,99,2,100,7,100,2,101,7,101,
	2,102,7,102,2,103,7,103,2,104,7,104,2,105,7,105,2,106,7,106,2,107,7,107,
	2,108,7,108,2,109,7,109,2,110,7,110,2,111,7,111,2,112,7,112,2,113,7,113,
	2,114,7,114,2,115,7,115,2,116,7,116,2,117,7,117,2,118,7,118,2,119,7,119,
	2,120,7,120,2,121,7,121,2,122,7,122,2,123,7,123,2,124,7,124,2,125,7,125,
	2,126,7,126,2,127,7,127,2,128,7,128,2,129,7,129,2,130,7,130,2,131,7,131,
	2,132,7,132,2,133,7,133,2,134,7,134,2,135,7,135,2,136,7,136,2,137,7,137,
	2,138,7,138,2,139,7,139,2,140,7,140,2,141,7,141,1,0,3,0,289,8,0,1,0,1,0,
	1,0,1,0,1,0,1,0,1,1,3,1,298,8,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,3,2,307,8,2,
	1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,3,3,3,318,8,3,1,3,1,3,1,3,1,3,1,3,1,3,
	1,3,1,3,1,4,1,4,3,4,330,8,4,1,5,1,5,1,5,3,5,335,8,5,1,6,1,6,1,6,1,6,3,6,
	341,8,6,1,7,1,7,1,7,1,7,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,9,1,9,1,9,1,9,1,9,
	1,9,1,10,1,10,1,10,1,10,1,10,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,12,1,
	12,1,12,1,13,1,13,1,13,1,13,1,13,1,13,1,13,1,14,1,14,1,14,1,14,1,14,1,14,
	1,14,1,14,1,14,1,15,1,15,1,15,1,15,1,15,1,15,1,15,1,16,1,16,1,16,1,17,1,
	17,1,17,1,17,1,17,1,18,1,18,1,18,1,18,1,18,1,19,1,19,1,19,1,19,1,19,1,19,
	1,20,1,20,1,20,1,20,1,21,1,21,1,21,1,22,1,22,1,22,1,22,1,23,1,23,1,23,1,
	23,1,23,1,23,1,23,1,23,1,24,1,24,1,24,1,24,1,24,1,25,1,25,1,25,1,25,1,25,
	1,25,1,25,1,26,1,26,1,26,1,26,1,26,1,26,1,26,1,27,1,27,1,27,1,28,1,28,1,
	28,1,28,1,29,1,29,1,29,1,29,1,30,1,30,1,30,1,31,1,31,1,31,1,31,1,31,1,32,
	1,32,1,32,1,32,1,32,1,33,1,33,1,33,1,33,1,33,1,33,1,34,1,34,1,34,1,34,1,
	34,1,34,1,35,1,35,1,35,1,35,1,35,1,35,1,36,1,36,1,36,1,36,1,37,1,37,1,37,
	1,37,1,37,1,38,1,38,1,38,1,38,1,38,1,38,1,38,1,38,1,38,1,39,1,39,1,39,1,
	39,1,39,1,39,1,40,1,40,1,40,1,40,1,40,1,40,1,41,1,41,1,41,1,41,1,41,1,41,
	1,42,1,42,1,42,3,42,536,8,42,1,42,1,42,3,42,540,8,42,1,42,3,42,543,8,42,
	3,42,545,8,42,1,42,1,42,1,43,1,43,5,43,551,8,43,10,43,12,43,554,9,43,1,
	44,3,44,557,8,44,1,44,1,44,3,44,561,8,44,1,45,1,45,1,45,1,45,1,45,3,45,
	568,8,45,1,45,1,45,3,45,572,8,45,1,46,1,46,5,46,576,8,46,10,46,12,46,579,
	9,46,1,46,4,46,582,8,46,11,46,12,46,583,3,46,586,8,46,1,47,1,47,1,47,4,
	47,591,8,47,11,47,12,47,592,1,48,1,48,1,48,4,48,598,8,48,11,48,12,48,599,
	1,49,1,49,1,49,4,49,605,8,49,11,49,12,49,606,1,50,1,50,3,50,611,8,50,1,
	51,1,51,3,51,615,8,51,1,51,1,51,1,52,1,52,1,53,1,53,1,53,1,53,1,54,1,54,
	1,55,1,55,1,55,1,56,1,56,1,56,1,57,1,57,1,58,1,58,1,59,1,59,1,60,1,60,1,
	60,1,61,1,61,1,62,1,62,1,62,1,63,1,63,1,63,1,64,1,64,1,65,1,65,1,66,1,66,
	1,67,1,67,1,67,1,68,1,68,1,68,1,69,1,69,1,70,1,70,1,71,1,71,1,72,1,72,1,
	73,1,73,1,73,1,74,1,74,1,75,1,75,1,75,1,76,1,76,1,76,1,76,1,76,1,77,1,77,
	1,77,1,78,1,78,1,79,1,79,1,80,1,80,1,80,1,81,1,81,1,81,1,82,1,82,1,82,1,
	83,1,83,1,83,1,84,1,84,1,84,1,85,1,85,1,86,1,86,1,86,1,87,1,87,1,87,1,88,
	1,88,1,88,1,89,1,89,1,89,1,90,1,90,1,90,1,91,1,91,1,91,1,92,1,92,1,92,1,
	93,1,93,1,93,1,94,1,94,1,94,1,95,1,95,1,95,1,96,1,96,1,96,1,96,1,97,1,97,
	1,97,1,97,1,98,1,98,1,98,1,98,1,99,1,99,1,99,1,99,1,100,1,100,1,101,1,101,
	1,102,1,102,1,102,3,102,760,8,102,1,102,1,102,1,103,1,103,1,104,3,104,767,
	8,104,1,104,1,104,1,104,1,104,1,104,1,105,3,105,775,8,105,1,105,1,105,1,
	105,1,105,1,105,1,105,1,105,1,105,1,106,1,106,1,106,1,106,1,106,1,107,1,
	107,4,107,792,8,107,11,107,12,107,793,1,108,3,108,797,8,108,1,108,1,108,
	1,108,1,108,1,108,1,109,3,109,805,8,109,1,109,1,109,1,109,1,109,1,109,1,
	109,1,109,1,109,1,110,1,110,1,110,1,110,1,110,1,111,1,111,4,111,822,8,111,
	11,111,12,111,823,1,112,1,112,1,112,1,112,1,112,3,112,831,8,112,1,113,1,
	113,1,113,5,113,836,8,113,10,113,12,113,839,9,113,1,113,1,113,1,113,1,113,
	5,113,845,8,113,10,113,12,113,848,9,113,1,113,3,113,851,8,113,1,114,1,114,
	1,114,1,114,1,114,5,114,858,8,114,10,114,12,114,861,9,114,1,114,1,114,1,
	114,1,114,1,114,1,114,1,114,1,114,5,114,871,8,114,10,114,12,114,874,9,114,
	1,114,1,114,1,114,3,114,879,8,114,1,115,1,115,1,115,5,115,884,8,115,10,
	115,12,115,887,9,115,1,115,1,115,1,115,1,115,5,115,893,8,115,10,115,12,
	115,896,9,115,1,115,3,115,899,8,115,1,116,1,116,1,116,1,116,1,116,5,116,
	906,8,116,10,116,12,116,909,9,116,1,116,1,116,1,116,1,116,1,116,1,116,1,
	116,1,116,5,116,919,8,116,10,116,12,116,922,9,116,1,116,1,116,1,116,3,116,
	927,8,116,1,117,1,117,3,117,931,8,117,1,118,1,118,1,119,1,119,1,119,1,119,
	3,119,939,8,119,1,120,1,120,1,121,1,121,1,122,1,122,1,123,1,123,1,124,1,
	124,1,125,3,125,952,8,125,1,125,1,125,1,125,1,125,3,125,958,8,125,1,126,
	1,126,3,126,962,8,126,1,126,1,126,1,127,4,127,967,8,127,11,127,12,127,968,
	1,128,1,128,4,128,973,8,128,11,128,12,128,974,1,129,1,129,3,129,979,8,129,
	1,129,4,129,982,8,129,11,129,12,129,983,1,130,1,130,1,130,5,130,989,8,130,
	10,130,12,130,992,9,130,1,130,1,130,1,130,1,130,5,130,998,8,130,10,130,
	12,130,1001,9,130,1,130,3,130,1004,8,130,1,131,1,131,1,131,1,131,1,131,
	5,131,1011,8,131,10,131,12,131,1014,9,131,1,131,1,131,1,131,1,131,1,131,
	1,131,1,131,1,131,5,131,1024,8,131,10,131,12,131,1027,9,131,1,131,1,131,
	1,131,3,131,1032,8,131,1,132,1,132,3,132,1036,8,132,1,133,3,133,1039,8,
	133,1,134,3,134,1042,8,134,1,135,3,135,1045,8,135,1,136,1,136,1,136,1,137,
	4,137,1051,8,137,11,137,12,137,1052,1,138,1,138,5,138,1057,8,138,10,138,
	12,138,1060,9,138,1,139,1,139,3,139,1064,8,139,1,139,3,139,1067,8,139,1,
	139,1,139,3,139,1071,8,139,1,140,3,140,1074,8,140,1,141,1,141,3,141,1078,
	8,141,6,859,872,907,920,1012,1025,0,142,3,3,5,4,7,5,9,6,11,7,13,8,15,9,
	17,10,19,11,21,12,23,13,25,14,27,15,29,16,31,17,33,18,35,19,37,20,39,21,
	41,22,43,23,45,24,47,25,49,26,51,27,53,28,55,29,57,30,59,31,61,32,63,33,
	65,34,67,35,69,36,71,37,73,38,75,39,77,40,79,41,81,42,83,43,85,44,87,45,
	89,46,91,47,93,48,95,49,97,50,99,51,101,52,103,53,105,54,107,55,109,56,
	111,57,113,58,115,59,117,60,119,61,121,62,123,63,125,64,127,65,129,66,131,
	67,133,68,135,69,137,70,139,71,141,72,143,73,145,74,147,75,149,76,151,77,
	153,78,155,79,157,80,159,81,161,82,163,83,165,84,167,85,169,86,171,87,173,
	88,175,89,177,90,179,91,181,92,183,93,185,94,187,95,189,96,191,97,193,98,
	195,99,197,100,199,101,201,102,203,103,205,104,207,105,209,106,211,107,
	213,108,215,0,217,109,219,110,221,111,223,0,225,112,227,0,229,0,231,0,233,
	0,235,0,237,0,239,0,241,0,243,0,245,0,247,0,249,0,251,0,253,0,255,0,257,
	0,259,0,261,0,263,0,265,0,267,0,269,0,271,0,273,0,275,0,277,0,279,0,281,
	0,283,0,285,0,3,0,1,2,27,4,0,82,82,85,85,114,114,117,117,2,0,66,66,98,98,
	2,0,82,82,114,114,2,0,79,79,111,111,2,0,88,88,120,120,2,0,74,74,106,106,
	2,0,39,39,123,123,2,0,34,34,123,123,2,0,70,70,102,102,4,0,10,10,12,13,39,
	39,92,92,4,0,10,10,12,13,34,34,92,92,1,0,92,92,1,0,49,57,1,0,48,57,1,0,
	48,55,3,0,48,57,65,70,97,102,1,0,48,49,2,0,69,69,101,101,2,0,43,43,45,45,
	5,0,0,9,11,12,14,38,40,91,93,127,5,0,0,9,11,12,14,33,35,91,93,127,2,0,0,
	91,93,127,1,0,0,127,2,0,9,9,32,32,2,0,10,10,12,13,295,0,65,90,95,95,97,
	122,170,170,181,181,186,186,192,214,216,246,248,577,592,705,710,721,736,
	740,750,750,890,890,902,902,904,906,908,908,910,929,931,974,976,1013,1015,
	1153,1162,1230,1232,1273,1280,1295,1329,1366,1369,1369,1377,1415,1488,1514,
	1520,1522,1569,1594,1600,1610,1646,1647,1649,1747,1749,1749,1765,1766,1774,
	1775,1786,1788,1791,1791,1808,1808,1810,1839,1869,1901,1920,1957,1969,1969,
	2308,2361,2365,2365,2384,2384,2392,2401,2429,2429,2437,2444,2447,2448,2451,
	2472,2474,2480,2482,2482,2486,2489,2493,2493,2510,2510,2524,2525,2527,2529,
	2544,2545,2565,2570,2575,2576,2579,2600,2602,2608,2610,2611,2613,2614,2616,
	2617,2649,2652,2654,2654,2674,2676,2693,2701,2703,2705,2707,2728,2730,2736,
	2738,2739,2741,2745,2749,2749,2768,2768,2784,2785,2821,2828,2831,2832,2835,
	2856,2858,2864,2866,2867,2869,2873,2877,2877,2908,2909,2911,2913,2929,2929,
	2947,2947,2949,2954,2958,2960,2962,2965,2969,2970,2972,2972,2974,2975,2979,
	2980,2984,2986,2990,3001,3077,3084,3086,3088,3090,3112,3114,3123,3125,3129,
	3168,3169,3205,3212,3214,3216,3218,3240,3242,3251,3253,3257,3261,3261,3294,
	3294,3296,3297,3333,3340,3342,3344,3346,3368,3370,3385,3424,3425,3461,3478,
	3482,3505,3507,3515,3517,3517,3520,3526,3585,3632,3634,3635,3648,3654,3713,
	3714,3716,3716,3719,3720,3722,3722,3725,3725,3732,3735,3737,3743,3745,3747,
	3749,3749,3751,3751,3754,3755,3757,3760,3762,3763,3773,3773,3776,3780,3782,
	3782,3804,3805,3840,3840,3904,3911,3913,3946,3976,3979,4096,4129,4131,4135,
	4137,4138,4176,4181,4256,4293,4304,4346,4348,4348,4352,4441,4447,4514,4520,
	4601,4608,4680,4682,4685,4688,4694,4696,4696,4698,4701,4704,4744,4746,4749,
	4752,4784,4786,4789,4792,4798,4800,4800,4802,4805,4808,4822,4824,4880,4882,
	4885,4888,4954,4992,5007,5024,5108,5121,5740,5743,5750,5761,5786,5792,5866,
	5870,5872,5888,5900,5902,5905,5920,5937,5952,5969,5984,5996,5998,6000,6016,
	6067,6103,6103,6108,6108,6176,6263,6272,6312,6400,6428,6480,6509,6512,6516,
	6528,6569,6593,6599,6656,6678,7424,7615,7680,7835,7840,7929,7936,7957,7960,
	7965,7968,8005,8008,8013,8016,8023,8025,8025,8027,8027,8029,8029,8031,8061,
	8064,8116,8118,8124,8126,8126,8130,8132,8134,8140,8144,8147,8150,8155,8160,
	8172,8178,8180,8182,8188,8305,8305,8319,8319,8336,8340,8450,8450,8455,8455,
	8458,8467,8469,8469,8472,8477,8484,8484,8486,8486,8488,8488,8490,8497,8499,
	8505,8508,8511,8517,8521,8544,8579,11264,11310,11312,11358,11392,11492,
	11520,11557,11568,11621,11631,11631,11648,11670,11680,11686,11688,11694,
	11696,11702,11704,11710,11712,11718,11720,11726,11728,11734,11736,11742,
	12293,12295,12321,12329,12337,12341,12344,12348,12353,12438,12443,12447,
	12449,12538,12540,12543,12549,12588,12593,12686,12704,12727,12784,12799,
	13312,19893,19968,40891,40960,42124,43008,43009,43011,43013,43015,43018,
	43020,43042,44032,55203,63744,64045,64048,64106,64112,64217,64256,64262,
	64275,64279,64285,64285,64287,64296,64298,64310,64312,64316,64318,64318,
	64320,64321,64323,64324,64326,64433,64467,64829,64848,64911,64914,64967,
	65008,65019,65136,65140,65142,65276,65313,65338,65345,65370,65382,65470,
	65474,65479,65482,65487,65490,65495,65498,65500,148,0,48,57,768,879,1155,
	1158,1425,1465,1467,1469,1471,1471,1473,1474,1476,1477,1479,1479,1552,1557,
	1611,1630,1632,1641,1648,1648,1750,1756,1759,1764,1767,1768,1770,1773,1776,
	1785,1809,1809,1840,1866,1958,1968,2305,2307,2364,2364,2366,2381,2385,2388,
	2402,2403,2406,2415,2433,2435,2492,2492,2494,2500,2503,2504,2507,2509,2519,
	2519,2530,2531,2534,2543,2561,2563,2620,2620,2622,2626,2631,2632,2635,2637,
	2662,2673,2689,2691,2748,2748,2750,2757,2759,2761,2763,2765,2786,2787,2790,
	2799,2817,2819,2876,2876,2878,2883,2887,2888,2891,2893,2902,2903,2918,2927,
	2946,2946,3006,3010,3014,3016,3018,3021,3031,3031,3046,3055,3073,3075,3134,
	3140,3142,3144,3146,3149,3157,3158,3174,3183,3202,3203,3260,3260,3262,3268,
	3270,3272,3274,3277,3285,3286,3302,3311,3330,3331,3390,3395,3398,3400,3402,
	3405,3415,3415,3430,3439,3458,3459,3530,3530,3535,3540,3542,3542,3544,3551,
	3570,3571,3633,3633,3636,3642,3655,3662,3664,3673,3761,3761,3764,3769,3771,
	3772,3784,3789,3792,3801,3864,3865,3872,3881,3893,3893,3895,3895,3897,3897,
	3902,3903,3953,3972,3974,3975,3984,3991,3993,4028,4038,4038,4140,4146,4150,
	4153,4160,4169,4182,4185,4959,4959,4969,4977,5906,5908,5938,5940,5970,5971,
	6002,6003,6070,6099,6109,6109,6112,6121,6155,6157,6160,6169,6313,6313,6432,
	6443,6448,6459,6470,6479,6576,6592,6600,6601,6608,6617,6679,6683,7616,7619,
	8255,8256,8276,8276,8400,8412,8417,8417,8421,8427,12330,12335,12441,12442,
	43010,43010,43014,43014,43019,43019,43043,43047,64286,64286,65024,65039,
	65056,65059,65075,65076,65101,65103,65296,65305,65343,65343,1126,0,3,1,
	0,0,0,0,5,1,0,0,0,0,7,1,0,0,0,0,9,1,0,0,0,0,11,1,0,0,0,0,13,1,0,0,0,0,15,
	1,0,0,0,0,17,1,0,0,0,0,19,1,0,0,0,0,21,1,0,0,0,0,23,1,0,0,0,0,25,1,0,0,
	0,0,27,1,0,0,0,0,29,1,0,0,0,0,31,1,0,0,0,0,33,1,0,0,0,0,35,1,0,0,0,0,37,
	1,0,0,0,0,39,1,0,0,0,0,41,1,0,0,0,0,43,1,0,0,0,0,45,1,0,0,0,0,47,1,0,0,
	0,0,49,1,0,0,0,0,51,1,0,0,0,0,53,1,0,0,0,0,55,1,0,0,0,0,57,1,0,0,0,0,59,
	1,0,0,0,0,61,1,0,0,0,0,63,1,0,0,0,0,65,1,0,0,0,0,67,1,0,0,0,0,69,1,0,0,
	0,0,71,1,0,0,0,0,73,1,0,0,0,0,75,1,0,0,0,0,77,1,0,0,0,0,79,1,0,0,0,0,81,
	1,0,0,0,0,83,1,0,0,0,0,85,1,0,0,0,0,87,1,0,0,0,0,89,1,0,0,0,0,91,1,0,0,
	0,0,93,1,0,0,0,0,95,1,0,0,0,0,97,1,0,0,0,0,99,1,0,0,0,0,101,1,0,0,0,0,103,
	1,0,0,0,0,105,1,0,0,0,0,107,1,0,0,0,0,109,1,0,0,0,0,111,1,0,0,0,0,113,1,
	0,0,0,0,115,1,0,0,0,0,117,1,0,0,0,0,119,1,0,0,0,0,121,1,0,0,0,0,123,1,0,
	0,0,0,125,1,0,0,0,0,127,1,0,0,0,0,129,1,0,0,0,0,131,1,0,0,0,0,133,1,0,0,
	0,0,135,1,0,0,0,0,137,1,0,0,0,0,139,1,0,0,0,0,141,1,0,0,0,0,143,1,0,0,0,
	0,145,1,0,0,0,0,147,1,0,0,0,0,149,1,0,0,0,0,151,1,0,0,0,0,153,1,0,0,0,0,
	155,1,0,0,0,0,157,1,0,0,0,0,159,1,0,0,0,0,161,1,0,0,0,0,163,1,0,0,0,0,165,
	1,0,0,0,0,167,1,0,0,0,0,169,1,0,0,0,0,171,1,0,0,0,0,173,1,0,0,0,0,175,1,
	0,0,0,0,177,1,0,0,0,0,179,1,0,0,0,0,181,1,0,0,0,0,183,1,0,0,0,0,185,1,0,
	0,0,0,187,1,0,0,0,0,189,1,0,0,0,0,191,1,0,0,0,0,193,1,0,0,0,0,195,1,0,0,
	0,0,197,1,0,0,0,0,199,1,0,0,0,0,201,1,0,0,0,0,203,1,0,0,0,0,205,1,0,0,0,
	0,207,1,0,0,0,0,209,1,0,0,0,1,211,1,0,0,0,1,213,1,0,0,0,1,215,1,0,0,0,1,
	217,1,0,0,0,2,219,1,0,0,0,2,221,1,0,0,0,2,223,1,0,0,0,2,225,1,0,0,0,3,288,
	1,0,0,0,5,297,1,0,0,0,7,306,1,0,0,0,9,317,1,0,0,0,11,329,1,0,0,0,13,334,
	1,0,0,0,15,340,1,0,0,0,17,342,1,0,0,0,19,346,1,0,0,0,21,353,1,0,0,0,23,
	359,1,0,0,0,25,364,1,0,0,0,27,371,1,0,0,0,29,374,1,0,0,0,31,381,1,0,0,0,
	33,390,1,0,0,0,35,397,1,0,0,0,37,400,1,0,0,0,39,405,1,0,0,0,41,410,1,0,
	0,0,43,416,1,0,0,0,45,420,1,0,0,0,47,423,1,0,0,0,49,427,1,0,0,0,51,435,
	1,0,0,0,53,440,1,0,0,0,55,447,1,0,0,0,57,454,1,0,0,0,59,457,1,0,0,0,61,
	461,1,0,0,0,63,465,1,0,0,0,65,468,1,0,0,0,67,473,1,0,0,0,69,478,1,0,0,0,
	71,484,1,0,0,0,73,490,1,0,0,0,75,496,1,0,0,0,77,500,1,0,0,0,79,505,1,0,
	0,0,81,514,1,0,0,0,83,520,1,0,0,0,85,526,1,0,0,0,87,544,1,0,0,0,89,548,
	1,0,0,0,91,556,1,0,0,0,93,567,1,0,0,0,95,585,1,0,0,0,97,587,1,0,0,0,99,
	594,1,0,0,0,101,601,1,0,0,0,103,610,1,0,0,0,105,614,1,0,0,0,107,618,1,0,
	0,0,109,620,1,0,0,0,111,624,1,0,0,0,113,626,1,0,0,0,115,629,1,0,0,0,117,
	632,1,0,0,0,119,634,1,0,0,0,121,636,1,0,0,0,123,638,1,0,0,0,125,641,1,0,
	0,0,127,643,1,0,0,0,129,646,1,0,0,0,131,649,1,0,0,0,133,651,1,0,0,0,135,
	653,1,0,0,0,137,655,1,0,0,0,139,658,1,0,0,0,141,661,1,0,0,0,143,663,1,0,
	0,0,145,665,1,0,0,0,147,667,1,0,0,0,149,669,1,0,0,0,151,672,1,0,0,0,153,
	674,1,0,0,0,155,677,1,0,0,0,157,682,1,0,0,0,159,685,1,0,0,0,161,687,1,0,
	0,0,163,689,1,0,0,0,165,692,1,0,0,0,167,695,1,0,0,0,169,698,1,0,0,0,171,
	701,1,0,0,0,173,704,1,0,0,0,175,706,1,0,0,0,177,709,1,0,0,0,179,712,1,0,
	0,0,181,715,1,0,0,0,183,718,1,0,0,0,185,721,1,0,0,0,187,724,1,0,0,0,189,
	727,1,0,0,0,191,730,1,0,0,0,193,733,1,0,0,0,195,736,1,0,0,0,197,740,1,0,
	0,0,199,744,1,0,0,0,201,748,1,0,0,0,203,752,1,0,0,0,205,754,1,0,0,0,207,
	759,1,0,0,0,209,763,1,0,0,0,211,766,1,0,0,0,213,774,1,0,0,0,215,784,1,0,
	0,0,217,791,1,0,0,0,219,796,1,0,0,0,221,804,1,0,0,0,223,814,1,0,0,0,225,
	821,1,0,0,0,227,830,1,0,0,0,229,850,1,0,0,0,231,878,1,0,0,0,233,898,1,0,
	0,0,235,926,1,0,0,0,237,930,1,0,0,0,239,932,1,0,0,0,241,938,1,0,0,0,243,
	940,1,0,0,0,245,942,1,0,0,0,247,944,1,0,0,0,249,946,1,0,0,0,251,948,1,0,
	0,0,253,957,1,0,0,0,255,961,1,0,0,0,257,966,1,0,0,0,259,970,1,0,0,0,261,
	976,1,0,0,0,263,1003,1,0,0,0,265,1031,1,0,0,0,267,1035,1,0,0,0,269,1038,
	1,0,0,0,271,1041,1,0,0,0,273,1044,1,0,0,0,275,1046,1,0,0,0,277,1050,1,0,
	0,0,279,1054,1,0,0,0,281,1061,1,0,0,0,283,1073,1,0,0,0,285,1077,1,0,0,0,
	287,289,3,227,112,0,288,287,1,0,0,0,288,289,1,0,0,0,289,290,1,0,0,0,290,
	291,5,39,0,0,291,292,1,0,0,0,292,293,6,0,0,0,293,294,1,0,0,0,294,295,6,
	0,1,0,295,4,1,0,0,0,296,298,3,227,112,0,297,296,1,0,0,0,297,298,1,0,0,0,
	298,299,1,0,0,0,299,300,5,34,0,0,300,301,1,0,0,0,301,302,6,1,2,0,302,303,
	1,0,0,0,303,304,6,1,3,0,304,6,1,0,0,0,305,307,3,227,112,0,306,305,1,0,0,
	0,306,307,1,0,0,0,307,308,1,0,0,0,308,309,5,39,0,0,309,310,5,39,0,0,310,
	311,5,39,0,0,311,312,1,0,0,0,312,313,6,2,4,0,313,314,1,0,0,0,314,315,6,
	2,1,0,315,8,1,0,0,0,316,318,3,227,112,0,317,316,1,0,0,0,317,318,1,0,0,0,
	318,319,1,0,0,0,319,320,5,34,0,0,320,321,5,34,0,0,321,322,5,34,0,0,322,
	323,1,0,0,0,323,324,6,3,5,0,324,325,1,0,0,0,325,326,6,3,3,0,326,10,1,0,
	0,0,327,330,3,91,44,0,328,330,3,93,45,0,329,327,1,0,0,0,329,328,1,0,0,0,
	330,12,1,0,0,0,331,335,3,15,6,0,332,335,3,103,50,0,333,335,3,105,51,0,334,
	331,1,0,0,0,334,332,1,0,0,0,334,333,1,0,0,0,335,14,1,0,0,0,336,341,3,95,
	46,0,337,341,3,97,47,0,338,341,3,99,48,0,339,341,3,101,49,0,340,336,1,0,
	0,0,340,337,1,0,0,0,340,338,1,0,0,0,340,339,1,0,0,0,341,16,1,0,0,0,342,
	343,5,100,0,0,343,344,5,101,0,0,344,345,5,102,0,0,345,18,1,0,0,0,346,347,
	5,114,0,0,347,348,5,101,0,0,348,349,5,116,0,0,349,350,5,117,0,0,350,351,
	5,114,0,0,351,352,5,110,0,0,352,20,1,0,0,0,353,354,5,114,0,0,354,355,5,
	97,0,0,355,356,5,105,0,0,356,357,5,115,0,0,357,358,5,101,0,0,358,22,1,0,
	0,0,359,360,5,102,0,0,360,361,5,114,0,0,361,362,5,111,0,0,362,363,5,109,
	0,0,363,24,1,0,0,0,364,365,5,105,0,0,365,366,5,109,0,0,366,367,5,112,0,
	0,367,368,5,111,0,0,368,369,5,114,0,0,369,370,5,116,0,0,370,26,1,0,0,0,
	371,372,5,97,0,0,372,373,5,115,0,0,373,28,1,0,0,0,374,375,5,103,0,0,375,
	376,5,108,0,0,376,377,5,111,0,0,377,378,5,98,0,0,378,379,5,97,0,0,379,380,
	5,108,0,0,380,30,1,0,0,0,381,382,5,110,0,0,382,383,5,111,0,0,383,384,5,
	110,0,0,384,385,5,108,0,0,385,386,5,111,0,0,386,387,5,99,0,0,387,388,5,
	97,0,0,388,389,5,108,0,0,389,32,1,0,0,0,390,391,5,97,0,0,391,392,5,115,
	0,0,392,393,5,115,0,0,393,394,5,101,0,0,394,395,5,114,0,0,395,396,5,116,
	0,0,396,34,1,0,0,0,397,398,5,105,0,0,398,399,5,102,0,0,399,36,1,0,0,0,400,
	401,5,101,0,0,401,402,5,108,0,0,402,403,5,105,0,0,403,404,5,102,0,0,404,
	38,1,0,0,0,405,406,5,101,0,0,406,407,5,108,0,0,407,408,5,115,0,0,408,409,
	5,101,0,0,409,40,1,0,0,0,410,411,5,119,0,0,411,412,5,104,0,0,412,413,5,
	105,0,0,413,414,5,108,0,0,414,415,5,101,0,0,415,42,1,0,0,0,416,417,5,102,
	0,0,417,418,5,111,0,0,418,419,5,114,0,0,419,44,1,0,0,0,420,421,5,105,0,
	0,421,422,5,110,0,0,422,46,1,0,0,0,423,424,5,116,0,0,424,425,5,114,0,0,
	425,426,5,121,0,0,426,48,1,0,0,0,427,428,5,102,0,0,428,429,5,105,0,0,429,
	430,5,110,0,0,430,431,5,97,0,0,431,432,5,108,0,0,432,433,5,108,0,0,433,
	434,5,121,0,0,434,50,1,0,0,0,435,436,5,119,0,0,436,437,5,105,0,0,437,438,
	5,116,0,0,438,439,5,104,0,0,439,52,1,0,0,0,440,441,5,101,0,0,441,442,5,
	120,0,0,442,443,5,99,0,0,443,444,5,101,0,0,444,445,5,112,0,0,445,446,5,
	116,0,0,446,54,1,0,0,0,447,448,5,108,0,0,448,449,5,97,0,0,449,450,5,109,
	0,0,450,451,5,98,0,0,451,452,5,100,0,0,452,453,5,97,0,0,453,56,1,0,0,0,
	454,455,5,111,0,0,455,456,5,114,0,0,456,58,1,0,0,0,457,458,5,97,0,0,458,
	459,5,110,0,0,459,460,5,100,0,0,460,60,1,0,0,0,461,462,5,110,0,0,462,463,
	5,111,0,0,463,464,5,116,0,0,464,62,1,0,0,0,465,466,5,105,0,0,466,467,5,
	115,0,0,467,64,1,0,0,0,468,469,5,78,0,0,469,470,5,111,0,0,470,471,5,110,
	0,0,471,472,5,101,0,0,472,66,1,0,0,0,473,474,5,84,0,0,474,475,5,114,0,0,
	475,476,5,117,0,0,476,477,5,101,0,0,477,68,1,0,0,0,478,479,5,70,0,0,479,
	480,5,97,0,0,480,481,5,108,0,0,481,482,5,115,0,0,482,483,5,101,0,0,483,
	70,1,0,0,0,484,485,5,99,0,0,485,486,5,108,0,0,486,487,5,97,0,0,487,488,
	5,115,0,0,488,489,5,115,0,0,489,72,1,0,0,0,490,491,5,121,0,0,491,492,5,
	105,0,0,492,493,5,101,0,0,493,494,5,108,0,0,494,495,5,100,0,0,495,74,1,
	0,0,0,496,497,5,100,0,0,497,498,5,101,0,0,498,499,5,108,0,0,499,76,1,0,
	0,0,500,501,5,112,0,0,501,502,5,97,0,0,502,503,5,115,0,0,503,504,5,115,
	0,0,504,78,1,0,0,0,505,506,5,99,0,0,506,507,5,111,0,0,507,508,5,110,0,0,
	508,509,5,116,0,0,509,510,5,105,0,0,510,511,5,110,0,0,511,512,5,117,0,0,
	512,513,5,101,0,0,513,80,1,0,0,0,514,515,5,98,0,0,515,516,5,114,0,0,516,
	517,5,101,0,0,517,518,5,97,0,0,518,519,5,107,0,0,519,82,1,0,0,0,520,521,
	5,97,0,0,521,522,5,115,0,0,522,523,5,121,0,0,523,524,5,110,0,0,524,525,
	5,99,0,0,525,84,1,0,0,0,526,527,5,97,0,0,527,528,5,119,0,0,528,529,5,97,
	0,0,529,530,5,105,0,0,530,531,5,116,0,0,531,86,1,0,0,0,532,533,4,42,0,0,
	533,545,3,277,137,0,534,536,5,13,0,0,535,534,1,0,0,0,535,536,1,0,0,0,536,
	537,1,0,0,0,537,540,5,10,0,0,538,540,5,13,0,0,539,535,1,0,0,0,539,538,1,
	0,0,0,540,542,1,0,0,0,541,543,3,277,137,0,542,541,1,0,0,0,542,543,1,0,0,
	0,543,545,1,0,0,0,544,532,1,0,0,0,544,539,1,0,0,0,545,546,1,0,0,0,546,547,
	6,42,6,0,547,88,1,0,0,0,548,552,3,283,140,0,549,551,3,285,141,0,550,549,
	1,0,0,0,551,554,1,0,0,0,552,550,1,0,0,0,552,553,1,0,0,0,553,90,1,0,0,0,
	554,552,1,0,0,0,555,557,7,0,0,0,556,555,1,0,0,0,556,557,1,0,0,0,557,560,
	1,0,0,0,558,561,3,233,115,0,559,561,3,235,116,0,560,558,1,0,0,0,560,559,
	1,0,0,0,561,92,1,0,0,0,562,568,7,1,0,0,563,564,7,1,0,0,564,568,7,2,0,0,
	565,566,7,2,0,0,566,568,7,1,0,0,567,562,1,0,0,0,567,563,1,0,0,0,567,565,
	1,0,0,0,568,571,1,0,0,0,569,572,3,263,130,0,570,572,3,265,131,0,571,569,
	1,0,0,0,571,570,1,0,0,0,572,94,1,0,0,0,573,577,3,243,120,0,574,576,3,245,
	121,0,575,574,1,0,0,0,576,579,1,0,0,0,577,575,1,0,0,0,577,578,1,0,0,0,578,
	586,1,0,0,0,579,577,1,0,0,0,580,582,5,48,0,0,581,580,1,0,0,0,582,583,1,
	0,0,0,583,581,1,0,0,0,583,584,1,0,0,0,584,586,1,0,0,0,585,573,1,0,0,0,585,
	581,1,0,0,0,586,96,1,0,0,0,587,588,5,48,0,0,588,590,7,3,0,0,589,591,3,247,
	122,0,590,589,1,0,0,0,591,592,1,0,0,0,592,590,1,0,0,0,592,593,1,0,0,0,593,
	98,1,0,0,0,594,595,5,48,0,0,595,597,7,4,0,0,596,598,3,249,123,0,597,596,
	1,0,0,0,598,599,1,0,0,0,599,597,1,0,0,0,599,600,1,0,0,0,600,100,1,0,0,0,
	601,602,5,48,0,0,602,604,7,1,0,0,603,605,3,251,124,0,604,603,1,0,0,0,605,
	606,1,0,0,0,606,604,1,0,0,0,606,607,1,0,0,0,607,102,1,0,0,0,608,611,3,253,
	125,0,609,611,3,255,126,0,610,608,1,0,0,0,610,609,1,0,0,0,611,104,1,0,0,
	0,612,615,3,103,50,0,613,615,3,257,127,0,614,612,1,0,0,0,614,613,1,0,0,
	0,615,616,1,0,0,0,616,617,7,5,0,0,617,106,1,0,0,0,618,619,5,46,0,0,619,
	108,1,0,0,0,620,621,5,46,0,0,621,622,5,46,0,0,622,623,5,46,0,0,623,110,
	1,0,0,0,624,625,5,42,0,0,625,112,1,0,0,0,626,627,5,40,0,0,627,628,6,55,
	7,0,628,114,1,0,0,0,629,630,5,41,0,0,630,631,6,56,8,0,631,116,1,0,0,0,632,
	633,5,44,0,0,633,118,1,0,0,0,634,635,5,58,0,0,635,120,1,0,0,0,636,637,5,
	59,0,0,637,122,1,0,0,0,638,639,5,42,0,0,639,640,5,42,0,0,640,124,1,0,0,
	0,641,642,5,61,0,0,642,126,1,0,0,0,643,644,5,91,0,0,644,645,6,62,9,0,645,
	128,1,0,0,0,646,647,5,93,0,0,647,648,6,63,10,0,648,130,1,0,0,0,649,650,
	5,124,0,0,650,132,1,0,0,0,651,652,5,94,0,0,652,134,1,0,0,0,653,654,5,38,
	0,0,654,136,1,0,0,0,655,656,5,60,0,0,656,657,5,60,0,0,657,138,1,0,0,0,658,
	659,5,62,0,0,659,660,5,62,0,0,660,140,1,0,0,0,661,662,5,43,0,0,662,142,
	1,0,0,0,663,664,5,45,0,0,664,144,1,0,0,0,665,666,5,47,0,0,666,146,1,0,0,
	0,667,668,5,37,0,0,668,148,1,0,0,0,669,670,5,47,0,0,670,671,5,47,0,0,671,
	150,1,0,0,0,672,673,5,126,0,0,673,152,1,0,0,0,674,675,5,123,0,0,675,676,
	6,75,11,0,676,154,1,0,0,0,677,678,4,76,1,0,678,679,5,125,0,0,679,680,1,
	0,0,0,680,681,6,76,12,0,681,156,1,0,0,0,682,683,5,125,0,0,683,684,6,77,
	13,0,684,158,1,0,0,0,685,686,5,60,0,0,686,160,1,0,0,0,687,688,5,62,0,0,
	688,162,1,0,0,0,689,690,5,61,0,0,690,691,5,61,0,0,691,164,1,0,0,0,692,693,
	5,62,0,0,693,694,5,61,0,0,694,166,1,0,0,0,695,696,5,60,0,0,696,697,5,61,
	0,0,697,168,1,0,0,0,698,699,5,60,0,0,699,700,5,62,0,0,700,170,1,0,0,0,701,
	702,5,33,0,0,702,703,5,61,0,0,703,172,1,0,0,0,704,705,5,64,0,0,705,174,
	1,0,0,0,706,707,5,45,0,0,707,708,5,62,0,0,708,176,1,0,0,0,709,710,5,43,
	0,0,710,711,5,61,0,0,711,178,1,0,0,0,712,713,5,45,0,0,713,714,5,61,0,0,
	714,180,1,0,0,0,715,716,5,42,0,0,716,717,5,61,0,0,717,182,1,0,0,0,718,719,
	5,64,0,0,719,720,5,61,0,0,720,184,1,0,0,0,721,722,5,47,0,0,722,723,5,61,
	0,0,723,186,1,0,0,0,724,725,5,37,0,0,725,726,5,61,0,0,726,188,1,0,0,0,727,
	728,5,38,0,0,728,729,5,61,0,0,729,190,1,0,0,0,730,731,5,124,0,0,731,732,
	5,61,0,0,732,192,1,0,0,0,733,734,5,94,0,0,734,735,5,61,0,0,735,194,1,0,
	0,0,736,737,5,60,0,0,737,738,5,60,0,0,738,739,5,61,0,0,739,196,1,0,0,0,
	740,741,5,62,0,0,741,742,5,62,0,0,742,743,5,61,0,0,743,198,1,0,0,0,744,
	745,5,42,0,0,745,746,5,42,0,0,746,747,5,61,0,0,747,200,1,0,0,0,748,749,
	5,47,0,0,749,750,5,47,0,0,750,751,5,61,0,0,751,202,1,0,0,0,752,753,5,39,
	0,0,753,204,1,0,0,0,754,755,5,34,0,0,755,206,1,0,0,0,756,760,3,277,137,
	0,757,760,3,279,138,0,758,760,3,281,139,0,759,756,1,0,0,0,759,757,1,0,0,
	0,759,758,1,0,0,0,760,761,1,0,0,0,761,762,6,102,14,0,762,208,1,0,0,0,763,
	764,9,0,0,0,764,210,1,0,0,0,765,767,3,227,112,0,766,765,1,0,0,0,766,767,
	1,0,0,0,767,768,1,0,0,0,768,769,5,39,0,0,769,770,6,104,15,0,770,771,1,0,
	0,0,771,772,6,104,12,0,772,212,1,0,0,0,773,775,3,227,112,0,774,773,1,0,
	0,0,774,775,1,0,0,0,775,776,1,0,0,0,776,777,5,39,0,0,777,778,5,39,0,0,778,
	779,5,39,0,0,779,780,1,0,0,0,780,781,6,105,16,0,781,782,1,0,0,0,782,783,
	6,105,12,0,783,214,1,0,0,0,784,785,5,123,0,0,785,786,1,0,0,0,786,787,6,
	106,17,0,787,788,6,106,18,0,788,216,1,0,0,0,789,792,3,241,119,0,790,792,
	8,6,0,0,791,789,1,0,0,0,791,790,1,0,0,0,792,793,1,0,0,0,793,791,1,0,0,0,
	793,794,1,0,0,0,794,218,1,0,0,0,795,797,3,227,112,0,796,795,1,0,0,0,796,
	797,1,0,0,0,797,798,1,0,0,0,798,799,5,34,0,0,799,800,6,108,19,0,800,801,
	1,0,0,0,801,802,6,108,12,0,802,220,1,0,0,0,803,805,3,227,112,0,804,803,
	1,0,0,0,804,805,1,0,0,0,805,806,1,0,0,0,806,807,5,34,0,0,807,808,5,34,0,
	0,808,809,5,34,0,0,809,810,1,0,0,0,810,811,6,109,20,0,811,812,1,0,0,0,812,
	813,6,109,12,0,813,222,1,0,0,0,814,815,5,123,0,0,815,816,1,0,0,0,816,817,
	6,110,17,0,817,818,6,110,18,0,818,224,1,0,0,0,819,822,3,241,119,0,820,822,
	8,7,0,0,821,819,1,0,0,0,821,820,1,0,0,0,822,823,1,0,0,0,823,821,1,0,0,0,
	823,824,1,0,0,0,824,226,1,0,0,0,825,831,7,8,0,0,826,827,7,8,0,0,827,831,
	7,2,0,0,828,829,7,2,0,0,829,831,7,8,0,0,830,825,1,0,0,0,830,826,1,0,0,0,
	830,828,1,0,0,0,831,228,1,0,0,0,832,837,5,39,0,0,833,836,3,241,119,0,834,
	836,8,9,0,0,835,833,1,0,0,0,835,834,1,0,0,0,836,839,1,0,0,0,837,835,1,0,
	0,0,837,838,1,0,0,0,838,840,1,0,0,0,839,837,1,0,0,0,840,851,5,39,0,0,841,
	846,5,34,0,0,842,845,3,241,119,0,843,845,8,10,0,0,844,842,1,0,0,0,844,843,
	1,0,0,0,845,848,1,0,0,0,846,844,1,0,0,0,846,847,1,0,0,0,847,849,1,0,0,0,
	848,846,1,0,0,0,849,851,5,34,0,0,850,832,1,0,0,0,850,841,1,0,0,0,851,230,
	1,0,0,0,852,853,5,39,0,0,853,854,5,39,0,0,854,855,5,39,0,0,855,859,1,0,
	0,0,856,858,3,237,117,0,857,856,1,0,0,0,858,861,1,0,0,0,859,860,1,0,0,0,
	859,857,1,0,0,0,860,862,1,0,0,0,861,859,1,0,0,0,862,863,5,39,0,0,863,864,
	5,39,0,0,864,879,5,39,0,0,865,866,5,34,0,0,866,867,5,34,0,0,867,868,5,34,
	0,0,868,872,1,0,0,0,869,871,3,237,117,0,870,869,1,0,0,0,871,874,1,0,0,0,
	872,873,1,0,0,0,872,870,1,0,0,0,873,875,1,0,0,0,874,872,1,0,0,0,875,876,
	5,34,0,0,876,877,5,34,0,0,877,879,5,34,0,0,878,852,1,0,0,0,878,865,1,0,
	0,0,879,232,1,0,0,0,880,885,5,39,0,0,881,884,3,241,119,0,882,884,8,9,0,
	0,883,881,1,0,0,0,883,882,1,0,0,0,884,887,1,0,0,0,885,883,1,0,0,0,885,886,
	1,0,0,0,886,888,1,0,0,0,887,885,1,0,0,0,888,899,5,39,0,0,889,894,5,34,0,
	0,890,893,3,241,119,0,891,893,8,10,0,0,892,890,1,0,0,0,892,891,1,0,0,0,
	893,896,1,0,0,0,894,892,1,0,0,0,894,895,1,0,0,0,895,897,1,0,0,0,896,894,
	1,0,0,0,897,899,5,34,0,0,898,880,1,0,0,0,898,889,1,0,0,0,899,234,1,0,0,
	0,900,901,5,39,0,0,901,902,5,39,0,0,902,903,5,39,0,0,903,907,1,0,0,0,904,
	906,3,237,117,0,905,904,1,0,0,0,906,909,1,0,0,0,907,908,1,0,0,0,907,905,
	1,0,0,0,908,910,1,0,0,0,909,907,1,0,0,0,910,911,5,39,0,0,911,912,5,39,0,
	0,912,927,5,39,0,0,913,914,5,34,0,0,914,915,5,34,0,0,915,916,5,34,0,0,916,
	920,1,0,0,0,917,919,3,237,117,0,918,917,1,0,0,0,919,922,1,0,0,0,920,921,
	1,0,0,0,920,918,1,0,0,0,921,923,1,0,0,0,922,920,1,0,0,0,923,924,5,34,0,
	0,924,925,5,34,0,0,925,927,5,34,0,0,926,900,1,0,0,0,926,913,1,0,0,0,927,
	236,1,0,0,0,928,931,3,239,118,0,929,931,3,241,119,0,930,928,1,0,0,0,930,
	929,1,0,0,0,931,238,1,0,0,0,932,933,8,11,0,0,933,240,1,0,0,0,934,935,5,
	92,0,0,935,939,9,0,0,0,936,937,5,92,0,0,937,939,3,87,42,0,938,934,1,0,0,
	0,938,936,1,0,0,0,939,242,1,0,0,0,940,941,7,12,0,0,941,244,1,0,0,0,942,
	943,7,13,0,0,943,246,1,0,0,0,944,945,7,14,0,0,945,248,1,0,0,0,946,947,7,
	15,0,0,947,250,1,0,0,0,948,949,7,16,0,0,949,252,1,0,0,0,950,952,3,257,127,
	0,951,950,1,0,0,0,951,952,1,0,0,0,952,953,1,0,0,0,953,958,3,259,128,0,954,
	955,3,257,127,0,955,956,5,46,0,0,956,958,1,0,0,0,957,951,1,0,0,0,957,954,
	1,0,0,0,958,254,1,0,0,0,959,962,3,257,127,0,960,962,3,253,125,0,961,959,
	1,0,0,0,961,960,1,0,0,0,962,963,1,0,0,0,963,964,3,261,129,0,964,256,1,0,
	0,0,965,967,3,245,121,0,966,965,1,0,0,0,967,968,1,0,0,0,968,966,1,0,0,0,
	968,969,1,0,0,0,969,258,1,0,0,0,970,972,5,46,0,0,971,973,3,245,121,0,972,
	971,1,0,0,0,973,974,1,0,0,0,974,972,1,0,0,0,974,975,1,0,0,0,975,260,1,0,
	0,0,976,978,7,17,0,0,977,979,7,18,0,0,978,977,1,0,0,0,978,979,1,0,0,0,979,
	981,1,0,0,0,980,982,3,245,121,0,981,980,1,0,0,0,982,983,1,0,0,0,983,981,
	1,0,0,0,983,984,1,0,0,0,984,262,1,0,0,0,985,990,5,39,0,0,986,989,3,269,
	133,0,987,989,3,275,136,0,988,986,1,0,0,0,988,987,1,0,0,0,989,992,1,0,0,
	0,990,988,1,0,0,0,990,991,1,0,0,0,991,993,1,0,0,0,992,990,1,0,0,0,993,1004,
	5,39,0,0,994,999,5,34,0,0,995,998,3,271,134,0,996,998,3,275,136,0,997,995,
	1,0,0,0,997,996,1,0,0,0,998,1001,1,0,0,0,999,997,1,0,0,0,999,1000,1,0,0,
	0,1000,1002,1,0,0,0,1001,999,1,0,0,0,1002,1004,5,34,0,0,1003,985,1,0,0,
	0,1003,994,1,0,0,0,1004,264,1,0,0,0,1005,1006,5,39,0,0,1006,1007,5,39,0,
	0,1007,1008,5,39,0,0,1008,1012,1,0,0,0,1009,1011,3,267,132,0,1010,1009,
	1,0,0,0,1011,1014,1,0,0,0,1012,1013,1,0,0,0,1012,1010,1,0,0,0,1013,1015,
	1,0,0,0,1014,1012,1,0,0,0,1015,1016,5,39,0,0,1016,1017,5,39,0,0,1017,1032,
	5,39,0,0,1018,1019,5,34,0,0,1019,1020,5,34,0,0,1020,1021,5,34,0,0,1021,
	1025,1,0,0,0,1022,1024,3,267,132,0,1023,1022,1,0,0,0,1024,1027,1,0,0,0,
	1025,1026,1,0,0,0,1025,1023,1,0,0,0,1026,1028,1,0,0,0,1027,1025,1,0,0,0,
	1028,1029,5,34,0,0,1029,1030,5,34,0,0,1030,1032,5,34,0,0,1031,1005,1,0,
	0,0,1031,1018,1,0,0,0,1032,266,1,0,0,0,1033,1036,3,273,135,0,1034,1036,
	3,275,136,0,1035,1033,1,0,0,0,1035,1034,1,0,0,0,1036,268,1,0,0,0,1037,1039,
	7,19,0,0,1038,1037,1,0,0,0,1039,270,1,0,0,0,1040,1042,7,20,0,0,1041,1040,
	1,0,0,0,1042,272,1,0,0,0,1043,1045,7,21,0,0,1044,1043,1,0,0,0,1045,274,
	1,0,0,0,1046,1047,5,92,0,0,1047,1048,7,22,0,0,1048,276,1,0,0,0,1049,1051,
	7,23,0,0,1050,1049,1,0,0,0,1051,1052,1,0,0,0,1052,1050,1,0,0,0,1052,1053,
	1,0,0,0,1053,278,1,0,0,0,1054,1058,5,35,0,0,1055,1057,8,24,0,0,1056,1055,
	1,0,0,0,1057,1060,1,0,0,0,1058,1056,1,0,0,0,1058,1059,1,0,0,0,1059,280,
	1,0,0,0,1060,1058,1,0,0,0,1061,1063,5,92,0,0,1062,1064,3,277,137,0,1063,
	1062,1,0,0,0,1063,1064,1,0,0,0,1064,1070,1,0,0,0,1065,1067,5,13,0,0,1066,
	1065,1,0,0,0,1066,1067,1,0,0,0,1067,1068,1,0,0,0,1068,1071,5,10,0,0,1069,
	1071,2,12,13,0,1070,1066,1,0,0,0,1070,1069,1,0,0,0,1071,282,1,0,0,0,1072,
	1074,7,25,0,0,1073,1072,1,0,0,0,1074,284,1,0,0,0,1075,1078,3,283,140,0,
	1076,1078,7,26,0,0,1077,1075,1,0,0,0,1077,1076,1,0,0,0,1078,286,1,0,0,0,
	81,0,1,2,288,297,306,317,329,334,340,535,539,542,544,552,556,560,567,571,
	577,583,585,592,599,606,610,614,759,766,774,791,793,796,804,821,823,830,
	835,837,844,846,850,859,872,878,883,885,892,894,898,907,920,926,930,938,
	951,957,961,968,974,978,983,988,990,997,999,1003,1012,1025,1031,1035,1038,
	1041,1044,1052,1058,1063,1066,1070,1073,1077,21,1,0,0,5,1,0,1,1,1,5,2,0,
	1,2,2,1,3,3,1,42,4,1,55,5,1,56,6,1,62,7,1,63,8,1,75,9,4,0,0,1,77,10,6,0,
	0,1,104,11,1,105,12,5,0,0,7,78,0,1,108,13,1,109,14];

	private static __ATN: ATN;
	public static get _ATN(): ATN {
		if (!Python3Lexer.__ATN) {
			Python3Lexer.__ATN = new ATNDeserializer().deserialize(Python3Lexer._serializedATN);
		}

		return Python3Lexer.__ATN;
	}


	static DecisionsToDFA = Python3Lexer._ATN.decisionToState.map( (ds: DecisionState, index: number) => new DFA(ds, index) );
}
