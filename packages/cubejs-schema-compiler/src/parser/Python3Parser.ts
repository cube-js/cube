// Generated from src/parser/Python3Parser.g4 by ANTLR 4.9.0-SNAPSHOT


import { ATN } from "antlr4ts/atn/ATN";
import { ATNDeserializer } from "antlr4ts/atn/ATNDeserializer";
import { FailedPredicateException } from "antlr4ts/FailedPredicateException";
import { NotNull } from "antlr4ts/Decorators";
import { NoViableAltException } from "antlr4ts/NoViableAltException";
import { Override } from "antlr4ts/Decorators";
import { Parser } from "antlr4ts/Parser";
import { ParserRuleContext } from "antlr4ts/ParserRuleContext";
import { ParserATNSimulator } from "antlr4ts/atn/ParserATNSimulator";
import { ParseTreeListener } from "antlr4ts/tree/ParseTreeListener";
import { ParseTreeVisitor } from "antlr4ts/tree/ParseTreeVisitor";
import { RecognitionException } from "antlr4ts/RecognitionException";
import { RuleContext } from "antlr4ts/RuleContext";
//import { RuleVersion } from "antlr4ts/RuleVersion";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";
import { Token } from "antlr4ts/Token";
import { TokenStream } from "antlr4ts/TokenStream";
import { Vocabulary } from "antlr4ts/Vocabulary";
import { VocabularyImpl } from "antlr4ts/VocabularyImpl";

import * as Utils from "antlr4ts/misc/Utils";

import { Python3ParserListener } from "./Python3ParserListener";
import { Python3ParserVisitor } from "./Python3ParserVisitor";


export class Python3Parser extends Parser {
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
	public static readonly RULE_file_input = 0;
	public static readonly RULE_single_input = 1;
	public static readonly RULE_eval_input = 2;
	public static readonly RULE_decorator = 3;
	public static readonly RULE_decorators = 4;
	public static readonly RULE_decorated = 5;
	public static readonly RULE_async_funcdef = 6;
	public static readonly RULE_funcdef = 7;
	public static readonly RULE_parameters = 8;
	public static readonly RULE_typedargslist = 9;
	public static readonly RULE_tfpdef = 10;
	public static readonly RULE_varargslist = 11;
	public static readonly RULE_vfpdef = 12;
	public static readonly RULE_stmt = 13;
	public static readonly RULE_simple_stmt = 14;
	public static readonly RULE_small_stmt = 15;
	public static readonly RULE_expr_stmt = 16;
	public static readonly RULE_annassign = 17;
	public static readonly RULE_testlist_star_expr = 18;
	public static readonly RULE_augassign = 19;
	public static readonly RULE_del_stmt = 20;
	public static readonly RULE_pass_stmt = 21;
	public static readonly RULE_flow_stmt = 22;
	public static readonly RULE_break_stmt = 23;
	public static readonly RULE_continue_stmt = 24;
	public static readonly RULE_return_stmt = 25;
	public static readonly RULE_yield_stmt = 26;
	public static readonly RULE_raise_stmt = 27;
	public static readonly RULE_import_stmt = 28;
	public static readonly RULE_import_name = 29;
	public static readonly RULE_import_from = 30;
	public static readonly RULE_import_as_name = 31;
	public static readonly RULE_dotted_as_name = 32;
	public static readonly RULE_import_as_names = 33;
	public static readonly RULE_dotted_as_names = 34;
	public static readonly RULE_dotted_name = 35;
	public static readonly RULE_global_stmt = 36;
	public static readonly RULE_nonlocal_stmt = 37;
	public static readonly RULE_assert_stmt = 38;
	public static readonly RULE_compound_stmt = 39;
	public static readonly RULE_async_stmt = 40;
	public static readonly RULE_if_stmt = 41;
	public static readonly RULE_while_stmt = 42;
	public static readonly RULE_for_stmt = 43;
	public static readonly RULE_try_stmt = 44;
	public static readonly RULE_with_stmt = 45;
	public static readonly RULE_with_item = 46;
	public static readonly RULE_except_clause = 47;
	public static readonly RULE_suite = 48;
	public static readonly RULE_test = 49;
	public static readonly RULE_test_nocond = 50;
	public static readonly RULE_lambdef = 51;
	public static readonly RULE_lambdef_nocond = 52;
	public static readonly RULE_or_test = 53;
	public static readonly RULE_and_test = 54;
	public static readonly RULE_not_test = 55;
	public static readonly RULE_comparison = 56;
	public static readonly RULE_comp_op = 57;
	public static readonly RULE_star_expr = 58;
	public static readonly RULE_expr = 59;
	public static readonly RULE_xor_expr = 60;
	public static readonly RULE_and_expr = 61;
	public static readonly RULE_shift_expr = 62;
	public static readonly RULE_arith_expr = 63;
	public static readonly RULE_term = 64;
	public static readonly RULE_factor = 65;
	public static readonly RULE_power = 66;
	public static readonly RULE_atom_expr = 67;
	public static readonly RULE_atom = 68;
	public static readonly RULE_testlist_comp = 69;
	public static readonly RULE_trailer = 70;
	public static readonly RULE_subscriptlist = 71;
	public static readonly RULE_subscript = 72;
	public static readonly RULE_sliceop = 73;
	public static readonly RULE_exprlist = 74;
	public static readonly RULE_testlist = 75;
	public static readonly RULE_dictorsetmaker = 76;
	public static readonly RULE_classdef = 77;
	public static readonly RULE_callArguments = 78;
	public static readonly RULE_arglist = 79;
	public static readonly RULE_argument = 80;
	public static readonly RULE_comp_iter = 81;
	public static readonly RULE_comp_for = 82;
	public static readonly RULE_comp_if = 83;
	public static readonly RULE_encoding_decl = 84;
	public static readonly RULE_yield_expr = 85;
	public static readonly RULE_yield_arg = 86;
	public static readonly RULE_string_template = 87;
	public static readonly RULE_single_string_template_atom = 88;
	public static readonly RULE_double_string_template_atom = 89;
	// tslint:disable:no-trailing-whitespace
	public static readonly ruleNames: string[] = [
		"file_input", "single_input", "eval_input", "decorator", "decorators", 
		"decorated", "async_funcdef", "funcdef", "parameters", "typedargslist", 
		"tfpdef", "varargslist", "vfpdef", "stmt", "simple_stmt", "small_stmt", 
		"expr_stmt", "annassign", "testlist_star_expr", "augassign", "del_stmt", 
		"pass_stmt", "flow_stmt", "break_stmt", "continue_stmt", "return_stmt", 
		"yield_stmt", "raise_stmt", "import_stmt", "import_name", "import_from", 
		"import_as_name", "dotted_as_name", "import_as_names", "dotted_as_names", 
		"dotted_name", "global_stmt", "nonlocal_stmt", "assert_stmt", "compound_stmt", 
		"async_stmt", "if_stmt", "while_stmt", "for_stmt", "try_stmt", "with_stmt", 
		"with_item", "except_clause", "suite", "test", "test_nocond", "lambdef", 
		"lambdef_nocond", "or_test", "and_test", "not_test", "comparison", "comp_op", 
		"star_expr", "expr", "xor_expr", "and_expr", "shift_expr", "arith_expr", 
		"term", "factor", "power", "atom_expr", "atom", "testlist_comp", "trailer", 
		"subscriptlist", "subscript", "sliceop", "exprlist", "testlist", "dictorsetmaker", 
		"classdef", "callArguments", "arglist", "argument", "comp_iter", "comp_for", 
		"comp_if", "encoding_decl", "yield_expr", "yield_arg", "string_template", 
		"single_string_template_atom", "double_string_template_atom",
	];

	private static readonly _LITERAL_NAMES: Array<string | undefined> = [
		undefined, undefined, undefined, undefined, undefined, undefined, undefined, 
		undefined, undefined, undefined, "'def'", "'return'", "'raise'", "'from'", 
		"'import'", "'as'", "'global'", "'nonlocal'", "'assert'", "'if'", "'elif'", 
		"'else'", "'while'", "'for'", "'in'", "'try'", "'finally'", "'with'", 
		"'except'", "'lambda'", "'or'", "'and'", "'not'", "'is'", "'None'", "'True'", 
		"'False'", "'class'", "'yield'", "'del'", "'pass'", "'continue'", "'break'", 
		"'async'", "'await'", undefined, undefined, undefined, undefined, undefined, 
		undefined, undefined, undefined, undefined, undefined, "'.'", "'...'", 
		"'*'", "'('", "')'", "','", "':'", "';'", "'**'", "'='", "'['", "']'", 
		"'|'", "'^'", "'&'", "'<<'", "'>>'", "'+'", "'-'", "'/'", "'%'", "'//'", 
		"'~'", "'{'", undefined, "'}'", "'<'", "'>'", "'=='", "'>='", "'<='", 
		"'<>'", "'!='", "'@'", "'->'", "'+='", "'-='", "'*='", "'@='", "'/='", 
		"'%='", "'&='", "'|='", "'^='", "'<<='", "'>>='", "'**='", "'//='", "'''", 
		"'\"'",
	];
	private static readonly _SYMBOLIC_NAMES: Array<string | undefined> = [
		undefined, "INDENT", "DEDENT", "SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START", 
		"DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START", "SINGLE_QUOTE_LONG_TEMPLATE_STRING_START", 
		"DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START", "STRING", "NUMBER", "INTEGER", 
		"DEF", "RETURN", "RAISE", "FROM", "IMPORT", "AS", "GLOBAL", "NONLOCAL", 
		"ASSERT", "IF", "ELIF", "ELSE", "WHILE", "FOR", "IN", "TRY", "FINALLY", 
		"WITH", "EXCEPT", "LAMBDA", "OR", "AND", "NOT", "IS", "NONE", "TRUE", 
		"FALSE", "CLASS", "YIELD", "DEL", "PASS", "CONTINUE", "BREAK", "ASYNC", 
		"AWAIT", "NEWLINE", "NAME", "STRING_LITERAL", "BYTES_LITERAL", "DECIMAL_INTEGER", 
		"OCT_INTEGER", "HEX_INTEGER", "BIN_INTEGER", "FLOAT_NUMBER", "IMAG_NUMBER", 
		"DOT", "ELLIPSIS", "STAR", "OPEN_PAREN", "CLOSE_PAREN", "COMMA", "COLON", 
		"SEMI_COLON", "POWER", "ASSIGN", "OPEN_BRACK", "CLOSE_BRACK", "OR_OP", 
		"XOR", "AND_OP", "LEFT_SHIFT", "RIGHT_SHIFT", "ADD", "MINUS", "DIV", "MOD", 
		"IDIV", "NOT_OP", "OPEN_BRACE", "TEMPLATE_CLOSE_BRACE", "CLOSE_BRACE", 
		"LESS_THAN", "GREATER_THAN", "EQUALS", "GT_EQ", "LT_EQ", "NOT_EQ_1", "NOT_EQ_2", 
		"AT", "ARROW", "ADD_ASSIGN", "SUB_ASSIGN", "MULT_ASSIGN", "AT_ASSIGN", 
		"DIV_ASSIGN", "MOD_ASSIGN", "AND_ASSIGN", "OR_ASSIGN", "XOR_ASSIGN", "LEFT_SHIFT_ASSIGN", 
		"RIGHT_SHIFT_ASSIGN", "POWER_ASSIGN", "IDIV_ASSIGN", "QUOTE", "DOUBLE_QUOTE", 
		"SKIP_", "UNKNOWN_CHAR", "SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END", "SINGLE_QUOTE_LONG_TEMPLATE_STRING_END", 
		"SINGLE_QUOTE_STRING_ATOM", "DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END", 
		"DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END", "DOUBLE_QUOTE_STRING_ATOM",
	];
	public static readonly VOCABULARY: Vocabulary = new VocabularyImpl(Python3Parser._LITERAL_NAMES, Python3Parser._SYMBOLIC_NAMES, []);

	// @Override
	// @NotNull
	public get vocabulary(): Vocabulary {
		return Python3Parser.VOCABULARY;
	}
	// tslint:enable:no-trailing-whitespace

	// @Override
	public get grammarFileName(): string { return "Python3Parser.g4"; }

	// @Override
	public get ruleNames(): string[] { return Python3Parser.ruleNames; }

	// @Override
	public get serializedATN(): string { return Python3Parser._serializedATN; }

	protected createFailedPredicateException(predicate?: string, message?: string): FailedPredicateException {
		return new FailedPredicateException(this, predicate, message);
	}

	constructor(input: TokenStream) {
		super(input);
		this._interp = new ParserATNSimulator(Python3Parser._ATN, this);
	}
	// @RuleVersion(0)
	public file_input(): File_inputContext {
		let _localctx: File_inputContext = new File_inputContext(this._ctx, this.state);
		this.enterRule(_localctx, 0, Python3Parser.RULE_file_input);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 184;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.DEF) | (1 << Python3Parser.RETURN) | (1 << Python3Parser.RAISE) | (1 << Python3Parser.FROM) | (1 << Python3Parser.IMPORT) | (1 << Python3Parser.GLOBAL) | (1 << Python3Parser.NONLOCAL) | (1 << Python3Parser.ASSERT) | (1 << Python3Parser.IF) | (1 << Python3Parser.WHILE) | (1 << Python3Parser.FOR) | (1 << Python3Parser.TRY) | (1 << Python3Parser.WITH) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.CLASS - 32)) | (1 << (Python3Parser.YIELD - 32)) | (1 << (Python3Parser.DEL - 32)) | (1 << (Python3Parser.PASS - 32)) | (1 << (Python3Parser.CONTINUE - 32)) | (1 << (Python3Parser.BREAK - 32)) | (1 << (Python3Parser.ASYNC - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NEWLINE - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)) | (1 << (Python3Parser.AT - 65)))) !== 0)) {
				{
				this.state = 182;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.NEWLINE:
					{
					this.state = 180;
					this.match(Python3Parser.NEWLINE);
					}
					break;
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.DEF:
				case Python3Parser.RETURN:
				case Python3Parser.RAISE:
				case Python3Parser.FROM:
				case Python3Parser.IMPORT:
				case Python3Parser.GLOBAL:
				case Python3Parser.NONLOCAL:
				case Python3Parser.ASSERT:
				case Python3Parser.IF:
				case Python3Parser.WHILE:
				case Python3Parser.FOR:
				case Python3Parser.TRY:
				case Python3Parser.WITH:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.CLASS:
				case Python3Parser.YIELD:
				case Python3Parser.DEL:
				case Python3Parser.PASS:
				case Python3Parser.CONTINUE:
				case Python3Parser.BREAK:
				case Python3Parser.ASYNC:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.STAR:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
				case Python3Parser.AT:
					{
					this.state = 181;
					this.stmt();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				this.state = 186;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 187;
			this.match(Python3Parser.EOF);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public single_input(): Single_inputContext {
		let _localctx: Single_inputContext = new Single_inputContext(this._ctx, this.state);
		this.enterRule(_localctx, 2, Python3Parser.RULE_single_input);
		try {
			this.state = 194;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.NEWLINE:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 189;
				this.match(Python3Parser.NEWLINE);
				}
				break;
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.RETURN:
			case Python3Parser.RAISE:
			case Python3Parser.FROM:
			case Python3Parser.IMPORT:
			case Python3Parser.GLOBAL:
			case Python3Parser.NONLOCAL:
			case Python3Parser.ASSERT:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.YIELD:
			case Python3Parser.DEL:
			case Python3Parser.PASS:
			case Python3Parser.CONTINUE:
			case Python3Parser.BREAK:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.STAR:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 190;
				this.simple_stmt();
				}
				break;
			case Python3Parser.DEF:
			case Python3Parser.IF:
			case Python3Parser.WHILE:
			case Python3Parser.FOR:
			case Python3Parser.TRY:
			case Python3Parser.WITH:
			case Python3Parser.CLASS:
			case Python3Parser.ASYNC:
			case Python3Parser.AT:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 191;
				this.compound_stmt();
				this.state = 192;
				this.match(Python3Parser.NEWLINE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public eval_input(): Eval_inputContext {
		let _localctx: Eval_inputContext = new Eval_inputContext(this._ctx, this.state);
		this.enterRule(_localctx, 4, Python3Parser.RULE_eval_input);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 196;
			this.testlist();
			this.state = 200;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.NEWLINE) {
				{
				{
				this.state = 197;
				this.match(Python3Parser.NEWLINE);
				}
				}
				this.state = 202;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 203;
			this.match(Python3Parser.EOF);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public decorator(): DecoratorContext {
		let _localctx: DecoratorContext = new DecoratorContext(this._ctx, this.state);
		this.enterRule(_localctx, 6, Python3Parser.RULE_decorator);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 205;
			this.match(Python3Parser.AT);
			this.state = 206;
			this.dotted_name();
			this.state = 212;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.OPEN_PAREN) {
				{
				this.state = 207;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 209;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)) | (1 << (Python3Parser.POWER - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
					{
					this.state = 208;
					this.arglist();
					}
				}

				this.state = 211;
				this.match(Python3Parser.CLOSE_PAREN);
				}
			}

			this.state = 214;
			this.match(Python3Parser.NEWLINE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public decorators(): DecoratorsContext {
		let _localctx: DecoratorsContext = new DecoratorsContext(this._ctx, this.state);
		this.enterRule(_localctx, 8, Python3Parser.RULE_decorators);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 217;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			do {
				{
				{
				this.state = 216;
				this.decorator();
				}
				}
				this.state = 219;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			} while (_la === Python3Parser.AT);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public decorated(): DecoratedContext {
		let _localctx: DecoratedContext = new DecoratedContext(this._ctx, this.state);
		this.enterRule(_localctx, 10, Python3Parser.RULE_decorated);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 221;
			this.decorators();
			this.state = 225;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.CLASS:
				{
				this.state = 222;
				this.classdef();
				}
				break;
			case Python3Parser.DEF:
				{
				this.state = 223;
				this.funcdef();
				}
				break;
			case Python3Parser.ASYNC:
				{
				this.state = 224;
				this.async_funcdef();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public async_funcdef(): Async_funcdefContext {
		let _localctx: Async_funcdefContext = new Async_funcdefContext(this._ctx, this.state);
		this.enterRule(_localctx, 12, Python3Parser.RULE_async_funcdef);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 227;
			this.match(Python3Parser.ASYNC);
			this.state = 228;
			this.funcdef();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public funcdef(): FuncdefContext {
		let _localctx: FuncdefContext = new FuncdefContext(this._ctx, this.state);
		this.enterRule(_localctx, 14, Python3Parser.RULE_funcdef);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 230;
			this.match(Python3Parser.DEF);
			this.state = 231;
			this.match(Python3Parser.NAME);
			this.state = 232;
			this.parameters();
			this.state = 235;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.ARROW) {
				{
				this.state = 233;
				this.match(Python3Parser.ARROW);
				this.state = 234;
				this.test();
				}
			}

			this.state = 237;
			this.match(Python3Parser.COLON);
			this.state = 238;
			this.suite();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public parameters(): ParametersContext {
		let _localctx: ParametersContext = new ParametersContext(this._ctx, this.state);
		this.enterRule(_localctx, 16, Python3Parser.RULE_parameters);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 240;
			this.match(Python3Parser.OPEN_PAREN);
			this.state = 242;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 46)) & ~0x1F) === 0 && ((1 << (_la - 46)) & ((1 << (Python3Parser.NAME - 46)) | (1 << (Python3Parser.STAR - 46)) | (1 << (Python3Parser.POWER - 46)))) !== 0)) {
				{
				this.state = 241;
				this.typedargslist();
				}
			}

			this.state = 244;
			this.match(Python3Parser.CLOSE_PAREN);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public typedargslist(): TypedargslistContext {
		let _localctx: TypedargslistContext = new TypedargslistContext(this._ctx, this.state);
		this.enterRule(_localctx, 18, Python3Parser.RULE_typedargslist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 327;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.NAME:
				{
				this.state = 246;
				this.tfpdef();
				this.state = 249;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.ASSIGN) {
					{
					this.state = 247;
					this.match(Python3Parser.ASSIGN);
					this.state = 248;
					this.test();
					}
				}

				this.state = 259;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 12, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 251;
						this.match(Python3Parser.COMMA);
						this.state = 252;
						this.tfpdef();
						this.state = 255;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.ASSIGN) {
							{
							this.state = 253;
							this.match(Python3Parser.ASSIGN);
							this.state = 254;
							this.test();
							}
						}

						}
						}
					}
					this.state = 261;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 12, this._ctx);
				}
				this.state = 295;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 262;
					this.match(Python3Parser.COMMA);
					this.state = 293;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case Python3Parser.STAR:
						{
						this.state = 263;
						this.match(Python3Parser.STAR);
						this.state = 265;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.NAME) {
							{
							this.state = 264;
							this.tfpdef();
							}
						}

						this.state = 275;
						this._errHandler.sync(this);
						_alt = this.interpreter.adaptivePredict(this._input, 15, this._ctx);
						while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
							if (_alt === 1) {
								{
								{
								this.state = 267;
								this.match(Python3Parser.COMMA);
								this.state = 268;
								this.tfpdef();
								this.state = 271;
								this._errHandler.sync(this);
								_la = this._input.LA(1);
								if (_la === Python3Parser.ASSIGN) {
									{
									this.state = 269;
									this.match(Python3Parser.ASSIGN);
									this.state = 270;
									this.test();
									}
								}

								}
								}
							}
							this.state = 277;
							this._errHandler.sync(this);
							_alt = this.interpreter.adaptivePredict(this._input, 15, this._ctx);
						}
						this.state = 286;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.COMMA) {
							{
							this.state = 278;
							this.match(Python3Parser.COMMA);
							this.state = 284;
							this._errHandler.sync(this);
							_la = this._input.LA(1);
							if (_la === Python3Parser.POWER) {
								{
								this.state = 279;
								this.match(Python3Parser.POWER);
								this.state = 280;
								this.tfpdef();
								this.state = 282;
								this._errHandler.sync(this);
								_la = this._input.LA(1);
								if (_la === Python3Parser.COMMA) {
									{
									this.state = 281;
									this.match(Python3Parser.COMMA);
									}
								}

								}
							}

							}
						}

						}
						break;
					case Python3Parser.POWER:
						{
						this.state = 288;
						this.match(Python3Parser.POWER);
						this.state = 289;
						this.tfpdef();
						this.state = 291;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.COMMA) {
							{
							this.state = 290;
							this.match(Python3Parser.COMMA);
							}
						}

						}
						break;
					case Python3Parser.CLOSE_PAREN:
						break;
					default:
						break;
					}
					}
				}

				}
				break;
			case Python3Parser.STAR:
				{
				this.state = 297;
				this.match(Python3Parser.STAR);
				this.state = 299;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.NAME) {
					{
					this.state = 298;
					this.tfpdef();
					}
				}

				this.state = 309;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 24, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 301;
						this.match(Python3Parser.COMMA);
						this.state = 302;
						this.tfpdef();
						this.state = 305;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.ASSIGN) {
							{
							this.state = 303;
							this.match(Python3Parser.ASSIGN);
							this.state = 304;
							this.test();
							}
						}

						}
						}
					}
					this.state = 311;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 24, this._ctx);
				}
				this.state = 320;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 312;
					this.match(Python3Parser.COMMA);
					this.state = 318;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === Python3Parser.POWER) {
						{
						this.state = 313;
						this.match(Python3Parser.POWER);
						this.state = 314;
						this.tfpdef();
						this.state = 316;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.COMMA) {
							{
							this.state = 315;
							this.match(Python3Parser.COMMA);
							}
						}

						}
					}

					}
				}

				}
				break;
			case Python3Parser.POWER:
				{
				this.state = 322;
				this.match(Python3Parser.POWER);
				this.state = 323;
				this.tfpdef();
				this.state = 325;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 324;
					this.match(Python3Parser.COMMA);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public tfpdef(): TfpdefContext {
		let _localctx: TfpdefContext = new TfpdefContext(this._ctx, this.state);
		this.enterRule(_localctx, 20, Python3Parser.RULE_tfpdef);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 329;
			this.match(Python3Parser.NAME);
			this.state = 332;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COLON) {
				{
				this.state = 330;
				this.match(Python3Parser.COLON);
				this.state = 331;
				this.test();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public varargslist(): VarargslistContext {
		let _localctx: VarargslistContext = new VarargslistContext(this._ctx, this.state);
		this.enterRule(_localctx, 22, Python3Parser.RULE_varargslist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 415;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.NAME:
				{
				this.state = 334;
				this.vfpdef();
				this.state = 337;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.ASSIGN) {
					{
					this.state = 335;
					this.match(Python3Parser.ASSIGN);
					this.state = 336;
					this.test();
					}
				}

				this.state = 347;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 33, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 339;
						this.match(Python3Parser.COMMA);
						this.state = 340;
						this.vfpdef();
						this.state = 343;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.ASSIGN) {
							{
							this.state = 341;
							this.match(Python3Parser.ASSIGN);
							this.state = 342;
							this.test();
							}
						}

						}
						}
					}
					this.state = 349;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 33, this._ctx);
				}
				this.state = 383;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 350;
					this.match(Python3Parser.COMMA);
					this.state = 381;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case Python3Parser.STAR:
						{
						this.state = 351;
						this.match(Python3Parser.STAR);
						this.state = 353;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.NAME) {
							{
							this.state = 352;
							this.vfpdef();
							}
						}

						this.state = 363;
						this._errHandler.sync(this);
						_alt = this.interpreter.adaptivePredict(this._input, 36, this._ctx);
						while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
							if (_alt === 1) {
								{
								{
								this.state = 355;
								this.match(Python3Parser.COMMA);
								this.state = 356;
								this.vfpdef();
								this.state = 359;
								this._errHandler.sync(this);
								_la = this._input.LA(1);
								if (_la === Python3Parser.ASSIGN) {
									{
									this.state = 357;
									this.match(Python3Parser.ASSIGN);
									this.state = 358;
									this.test();
									}
								}

								}
								}
							}
							this.state = 365;
							this._errHandler.sync(this);
							_alt = this.interpreter.adaptivePredict(this._input, 36, this._ctx);
						}
						this.state = 374;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.COMMA) {
							{
							this.state = 366;
							this.match(Python3Parser.COMMA);
							this.state = 372;
							this._errHandler.sync(this);
							_la = this._input.LA(1);
							if (_la === Python3Parser.POWER) {
								{
								this.state = 367;
								this.match(Python3Parser.POWER);
								this.state = 368;
								this.vfpdef();
								this.state = 370;
								this._errHandler.sync(this);
								_la = this._input.LA(1);
								if (_la === Python3Parser.COMMA) {
									{
									this.state = 369;
									this.match(Python3Parser.COMMA);
									}
								}

								}
							}

							}
						}

						}
						break;
					case Python3Parser.POWER:
						{
						this.state = 376;
						this.match(Python3Parser.POWER);
						this.state = 377;
						this.vfpdef();
						this.state = 379;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.COMMA) {
							{
							this.state = 378;
							this.match(Python3Parser.COMMA);
							}
						}

						}
						break;
					case Python3Parser.COLON:
						break;
					default:
						break;
					}
					}
				}

				}
				break;
			case Python3Parser.STAR:
				{
				this.state = 385;
				this.match(Python3Parser.STAR);
				this.state = 387;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.NAME) {
					{
					this.state = 386;
					this.vfpdef();
					}
				}

				this.state = 397;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 45, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 389;
						this.match(Python3Parser.COMMA);
						this.state = 390;
						this.vfpdef();
						this.state = 393;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.ASSIGN) {
							{
							this.state = 391;
							this.match(Python3Parser.ASSIGN);
							this.state = 392;
							this.test();
							}
						}

						}
						}
					}
					this.state = 399;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 45, this._ctx);
				}
				this.state = 408;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 400;
					this.match(Python3Parser.COMMA);
					this.state = 406;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === Python3Parser.POWER) {
						{
						this.state = 401;
						this.match(Python3Parser.POWER);
						this.state = 402;
						this.vfpdef();
						this.state = 404;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la === Python3Parser.COMMA) {
							{
							this.state = 403;
							this.match(Python3Parser.COMMA);
							}
						}

						}
					}

					}
				}

				}
				break;
			case Python3Parser.POWER:
				{
				this.state = 410;
				this.match(Python3Parser.POWER);
				this.state = 411;
				this.vfpdef();
				this.state = 413;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 412;
					this.match(Python3Parser.COMMA);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public vfpdef(): VfpdefContext {
		let _localctx: VfpdefContext = new VfpdefContext(this._ctx, this.state);
		this.enterRule(_localctx, 24, Python3Parser.RULE_vfpdef);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 417;
			this.match(Python3Parser.NAME);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public stmt(): StmtContext {
		let _localctx: StmtContext = new StmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 26, Python3Parser.RULE_stmt);
		try {
			this.state = 421;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.RETURN:
			case Python3Parser.RAISE:
			case Python3Parser.FROM:
			case Python3Parser.IMPORT:
			case Python3Parser.GLOBAL:
			case Python3Parser.NONLOCAL:
			case Python3Parser.ASSERT:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.YIELD:
			case Python3Parser.DEL:
			case Python3Parser.PASS:
			case Python3Parser.CONTINUE:
			case Python3Parser.BREAK:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.STAR:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 419;
				this.simple_stmt();
				}
				break;
			case Python3Parser.DEF:
			case Python3Parser.IF:
			case Python3Parser.WHILE:
			case Python3Parser.FOR:
			case Python3Parser.TRY:
			case Python3Parser.WITH:
			case Python3Parser.CLASS:
			case Python3Parser.ASYNC:
			case Python3Parser.AT:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 420;
				this.compound_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public simple_stmt(): Simple_stmtContext {
		let _localctx: Simple_stmtContext = new Simple_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 28, Python3Parser.RULE_simple_stmt);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 423;
			this.small_stmt();
			this.state = 428;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 52, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 424;
					this.match(Python3Parser.SEMI_COLON);
					this.state = 425;
					this.small_stmt();
					}
					}
				}
				this.state = 430;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 52, this._ctx);
			}
			this.state = 432;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.SEMI_COLON) {
				{
				this.state = 431;
				this.match(Python3Parser.SEMI_COLON);
				}
			}

			this.state = 434;
			this.match(Python3Parser.NEWLINE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public small_stmt(): Small_stmtContext {
		let _localctx: Small_stmtContext = new Small_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 30, Python3Parser.RULE_small_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 444;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.STAR:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				{
				this.state = 436;
				this.expr_stmt();
				}
				break;
			case Python3Parser.DEL:
				{
				this.state = 437;
				this.del_stmt();
				}
				break;
			case Python3Parser.PASS:
				{
				this.state = 438;
				this.pass_stmt();
				}
				break;
			case Python3Parser.RETURN:
			case Python3Parser.RAISE:
			case Python3Parser.YIELD:
			case Python3Parser.CONTINUE:
			case Python3Parser.BREAK:
				{
				this.state = 439;
				this.flow_stmt();
				}
				break;
			case Python3Parser.FROM:
			case Python3Parser.IMPORT:
				{
				this.state = 440;
				this.import_stmt();
				}
				break;
			case Python3Parser.GLOBAL:
				{
				this.state = 441;
				this.global_stmt();
				}
				break;
			case Python3Parser.NONLOCAL:
				{
				this.state = 442;
				this.nonlocal_stmt();
				}
				break;
			case Python3Parser.ASSERT:
				{
				this.state = 443;
				this.assert_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public expr_stmt(): Expr_stmtContext {
		let _localctx: Expr_stmtContext = new Expr_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 32, Python3Parser.RULE_expr_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 446;
			this.testlist_star_expr();
			this.state = 463;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.COLON:
				{
				this.state = 447;
				this.annassign();
				}
				break;
			case Python3Parser.ADD_ASSIGN:
			case Python3Parser.SUB_ASSIGN:
			case Python3Parser.MULT_ASSIGN:
			case Python3Parser.AT_ASSIGN:
			case Python3Parser.DIV_ASSIGN:
			case Python3Parser.MOD_ASSIGN:
			case Python3Parser.AND_ASSIGN:
			case Python3Parser.OR_ASSIGN:
			case Python3Parser.XOR_ASSIGN:
			case Python3Parser.LEFT_SHIFT_ASSIGN:
			case Python3Parser.RIGHT_SHIFT_ASSIGN:
			case Python3Parser.POWER_ASSIGN:
			case Python3Parser.IDIV_ASSIGN:
				{
				this.state = 448;
				this.augassign();
				this.state = 451;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.YIELD:
					{
					this.state = 449;
					this.yield_expr();
					}
					break;
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
					{
					this.state = 450;
					this.testlist();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case Python3Parser.NEWLINE:
			case Python3Parser.SEMI_COLON:
			case Python3Parser.ASSIGN:
				{
				this.state = 460;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === Python3Parser.ASSIGN) {
					{
					{
					this.state = 453;
					this.match(Python3Parser.ASSIGN);
					this.state = 456;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case Python3Parser.YIELD:
						{
						this.state = 454;
						this.yield_expr();
						}
						break;
					case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
					case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
					case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
					case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
					case Python3Parser.STRING:
					case Python3Parser.NUMBER:
					case Python3Parser.LAMBDA:
					case Python3Parser.NOT:
					case Python3Parser.NONE:
					case Python3Parser.TRUE:
					case Python3Parser.FALSE:
					case Python3Parser.AWAIT:
					case Python3Parser.NAME:
					case Python3Parser.ELLIPSIS:
					case Python3Parser.STAR:
					case Python3Parser.OPEN_PAREN:
					case Python3Parser.OPEN_BRACK:
					case Python3Parser.ADD:
					case Python3Parser.MINUS:
					case Python3Parser.NOT_OP:
					case Python3Parser.OPEN_BRACE:
						{
						this.state = 455;
						this.testlist_star_expr();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
					this.state = 462;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public annassign(): AnnassignContext {
		let _localctx: AnnassignContext = new AnnassignContext(this._ctx, this.state);
		this.enterRule(_localctx, 34, Python3Parser.RULE_annassign);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 465;
			this.match(Python3Parser.COLON);
			this.state = 466;
			this.test();
			this.state = 469;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.ASSIGN) {
				{
				this.state = 467;
				this.match(Python3Parser.ASSIGN);
				this.state = 468;
				this.test();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public testlist_star_expr(): Testlist_star_exprContext {
		let _localctx: Testlist_star_exprContext = new Testlist_star_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 36, Python3Parser.RULE_testlist_star_expr);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 473;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				{
				this.state = 471;
				this.test();
				}
				break;
			case Python3Parser.STAR:
				{
				this.state = 472;
				this.star_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 482;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 62, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 475;
					this.match(Python3Parser.COMMA);
					this.state = 478;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
					case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
					case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
					case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
					case Python3Parser.STRING:
					case Python3Parser.NUMBER:
					case Python3Parser.LAMBDA:
					case Python3Parser.NOT:
					case Python3Parser.NONE:
					case Python3Parser.TRUE:
					case Python3Parser.FALSE:
					case Python3Parser.AWAIT:
					case Python3Parser.NAME:
					case Python3Parser.ELLIPSIS:
					case Python3Parser.OPEN_PAREN:
					case Python3Parser.OPEN_BRACK:
					case Python3Parser.ADD:
					case Python3Parser.MINUS:
					case Python3Parser.NOT_OP:
					case Python3Parser.OPEN_BRACE:
						{
						this.state = 476;
						this.test();
						}
						break;
					case Python3Parser.STAR:
						{
						this.state = 477;
						this.star_expr();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
				}
				this.state = 484;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 62, this._ctx);
			}
			this.state = 486;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 485;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public augassign(): AugassignContext {
		let _localctx: AugassignContext = new AugassignContext(this._ctx, this.state);
		this.enterRule(_localctx, 38, Python3Parser.RULE_augassign);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 488;
			_la = this._input.LA(1);
			if (!(((((_la - 90)) & ~0x1F) === 0 && ((1 << (_la - 90)) & ((1 << (Python3Parser.ADD_ASSIGN - 90)) | (1 << (Python3Parser.SUB_ASSIGN - 90)) | (1 << (Python3Parser.MULT_ASSIGN - 90)) | (1 << (Python3Parser.AT_ASSIGN - 90)) | (1 << (Python3Parser.DIV_ASSIGN - 90)) | (1 << (Python3Parser.MOD_ASSIGN - 90)) | (1 << (Python3Parser.AND_ASSIGN - 90)) | (1 << (Python3Parser.OR_ASSIGN - 90)) | (1 << (Python3Parser.XOR_ASSIGN - 90)) | (1 << (Python3Parser.LEFT_SHIFT_ASSIGN - 90)) | (1 << (Python3Parser.RIGHT_SHIFT_ASSIGN - 90)) | (1 << (Python3Parser.POWER_ASSIGN - 90)) | (1 << (Python3Parser.IDIV_ASSIGN - 90)))) !== 0))) {
			this._errHandler.recoverInline(this);
			} else {
				if (this._input.LA(1) === Token.EOF) {
					this.matchedEOF = true;
				}

				this._errHandler.reportMatch(this);
				this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public del_stmt(): Del_stmtContext {
		let _localctx: Del_stmtContext = new Del_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 40, Python3Parser.RULE_del_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 490;
			this.match(Python3Parser.DEL);
			this.state = 491;
			this.exprlist();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public pass_stmt(): Pass_stmtContext {
		let _localctx: Pass_stmtContext = new Pass_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 42, Python3Parser.RULE_pass_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 493;
			this.match(Python3Parser.PASS);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public flow_stmt(): Flow_stmtContext {
		let _localctx: Flow_stmtContext = new Flow_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 44, Python3Parser.RULE_flow_stmt);
		try {
			this.state = 500;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.BREAK:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 495;
				this.break_stmt();
				}
				break;
			case Python3Parser.CONTINUE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 496;
				this.continue_stmt();
				}
				break;
			case Python3Parser.RETURN:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 497;
				this.return_stmt();
				}
				break;
			case Python3Parser.RAISE:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 498;
				this.raise_stmt();
				}
				break;
			case Python3Parser.YIELD:
				this.enterOuterAlt(_localctx, 5);
				{
				this.state = 499;
				this.yield_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public break_stmt(): Break_stmtContext {
		let _localctx: Break_stmtContext = new Break_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 46, Python3Parser.RULE_break_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 502;
			this.match(Python3Parser.BREAK);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public continue_stmt(): Continue_stmtContext {
		let _localctx: Continue_stmtContext = new Continue_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 48, Python3Parser.RULE_continue_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 504;
			this.match(Python3Parser.CONTINUE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public return_stmt(): Return_stmtContext {
		let _localctx: Return_stmtContext = new Return_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 50, Python3Parser.RULE_return_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 506;
			this.match(Python3Parser.RETURN);
			this.state = 508;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
				{
				this.state = 507;
				this.testlist();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public yield_stmt(): Yield_stmtContext {
		let _localctx: Yield_stmtContext = new Yield_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 52, Python3Parser.RULE_yield_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 510;
			this.yield_expr();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public raise_stmt(): Raise_stmtContext {
		let _localctx: Raise_stmtContext = new Raise_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 54, Python3Parser.RULE_raise_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 512;
			this.match(Python3Parser.RAISE);
			this.state = 518;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
				{
				this.state = 513;
				this.test();
				this.state = 516;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.FROM) {
					{
					this.state = 514;
					this.match(Python3Parser.FROM);
					this.state = 515;
					this.test();
					}
				}

				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public import_stmt(): Import_stmtContext {
		let _localctx: Import_stmtContext = new Import_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 56, Python3Parser.RULE_import_stmt);
		try {
			this.state = 522;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.IMPORT:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 520;
				this.import_name();
				}
				break;
			case Python3Parser.FROM:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 521;
				this.import_from();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public import_name(): Import_nameContext {
		let _localctx: Import_nameContext = new Import_nameContext(this._ctx, this.state);
		this.enterRule(_localctx, 58, Python3Parser.RULE_import_name);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 524;
			this.match(Python3Parser.IMPORT);
			this.state = 525;
			this.dotted_as_names();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public import_from(): Import_fromContext {
		let _localctx: Import_fromContext = new Import_fromContext(this._ctx, this.state);
		this.enterRule(_localctx, 60, Python3Parser.RULE_import_from);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			{
			this.state = 527;
			this.match(Python3Parser.FROM);
			this.state = 540;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 71, this._ctx) ) {
			case 1:
				{
				this.state = 531;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === Python3Parser.DOT || _la === Python3Parser.ELLIPSIS) {
					{
					{
					this.state = 528;
					_la = this._input.LA(1);
					if (!(_la === Python3Parser.DOT || _la === Python3Parser.ELLIPSIS)) {
					this._errHandler.recoverInline(this);
					} else {
						if (this._input.LA(1) === Token.EOF) {
							this.matchedEOF = true;
						}

						this._errHandler.reportMatch(this);
						this.consume();
					}
					}
					}
					this.state = 533;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 534;
				this.dotted_name();
				}
				break;

			case 2:
				{
				this.state = 536;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 535;
					_la = this._input.LA(1);
					if (!(_la === Python3Parser.DOT || _la === Python3Parser.ELLIPSIS)) {
					this._errHandler.recoverInline(this);
					} else {
						if (this._input.LA(1) === Token.EOF) {
							this.matchedEOF = true;
						}

						this._errHandler.reportMatch(this);
						this.consume();
					}
					}
					}
					this.state = 538;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while (_la === Python3Parser.DOT || _la === Python3Parser.ELLIPSIS);
				}
				break;
			}
			this.state = 542;
			this.match(Python3Parser.IMPORT);
			this.state = 549;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.STAR:
				{
				this.state = 543;
				this.match(Python3Parser.STAR);
				}
				break;
			case Python3Parser.OPEN_PAREN:
				{
				this.state = 544;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 545;
				this.import_as_names();
				this.state = 546;
				this.match(Python3Parser.CLOSE_PAREN);
				}
				break;
			case Python3Parser.NAME:
				{
				this.state = 548;
				this.import_as_names();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public import_as_name(): Import_as_nameContext {
		let _localctx: Import_as_nameContext = new Import_as_nameContext(this._ctx, this.state);
		this.enterRule(_localctx, 62, Python3Parser.RULE_import_as_name);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 551;
			this.match(Python3Parser.NAME);
			this.state = 554;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.AS) {
				{
				this.state = 552;
				this.match(Python3Parser.AS);
				this.state = 553;
				this.match(Python3Parser.NAME);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public dotted_as_name(): Dotted_as_nameContext {
		let _localctx: Dotted_as_nameContext = new Dotted_as_nameContext(this._ctx, this.state);
		this.enterRule(_localctx, 64, Python3Parser.RULE_dotted_as_name);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 556;
			this.dotted_name();
			this.state = 559;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.AS) {
				{
				this.state = 557;
				this.match(Python3Parser.AS);
				this.state = 558;
				this.match(Python3Parser.NAME);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public import_as_names(): Import_as_namesContext {
		let _localctx: Import_as_namesContext = new Import_as_namesContext(this._ctx, this.state);
		this.enterRule(_localctx, 66, Python3Parser.RULE_import_as_names);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 561;
			this.import_as_name();
			this.state = 566;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 75, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 562;
					this.match(Python3Parser.COMMA);
					this.state = 563;
					this.import_as_name();
					}
					}
				}
				this.state = 568;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 75, this._ctx);
			}
			this.state = 570;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 569;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public dotted_as_names(): Dotted_as_namesContext {
		let _localctx: Dotted_as_namesContext = new Dotted_as_namesContext(this._ctx, this.state);
		this.enterRule(_localctx, 68, Python3Parser.RULE_dotted_as_names);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 572;
			this.dotted_as_name();
			this.state = 577;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.COMMA) {
				{
				{
				this.state = 573;
				this.match(Python3Parser.COMMA);
				this.state = 574;
				this.dotted_as_name();
				}
				}
				this.state = 579;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public dotted_name(): Dotted_nameContext {
		let _localctx: Dotted_nameContext = new Dotted_nameContext(this._ctx, this.state);
		this.enterRule(_localctx, 70, Python3Parser.RULE_dotted_name);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 580;
			this.match(Python3Parser.NAME);
			this.state = 585;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.DOT) {
				{
				{
				this.state = 581;
				this.match(Python3Parser.DOT);
				this.state = 582;
				this.match(Python3Parser.NAME);
				}
				}
				this.state = 587;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public global_stmt(): Global_stmtContext {
		let _localctx: Global_stmtContext = new Global_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 72, Python3Parser.RULE_global_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 588;
			this.match(Python3Parser.GLOBAL);
			this.state = 589;
			this.match(Python3Parser.NAME);
			this.state = 594;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.COMMA) {
				{
				{
				this.state = 590;
				this.match(Python3Parser.COMMA);
				this.state = 591;
				this.match(Python3Parser.NAME);
				}
				}
				this.state = 596;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public nonlocal_stmt(): Nonlocal_stmtContext {
		let _localctx: Nonlocal_stmtContext = new Nonlocal_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 74, Python3Parser.RULE_nonlocal_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 597;
			this.match(Python3Parser.NONLOCAL);
			this.state = 598;
			this.match(Python3Parser.NAME);
			this.state = 603;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.COMMA) {
				{
				{
				this.state = 599;
				this.match(Python3Parser.COMMA);
				this.state = 600;
				this.match(Python3Parser.NAME);
				}
				}
				this.state = 605;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public assert_stmt(): Assert_stmtContext {
		let _localctx: Assert_stmtContext = new Assert_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 76, Python3Parser.RULE_assert_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 606;
			this.match(Python3Parser.ASSERT);
			this.state = 607;
			this.test();
			this.state = 610;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 608;
				this.match(Python3Parser.COMMA);
				this.state = 609;
				this.test();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public compound_stmt(): Compound_stmtContext {
		let _localctx: Compound_stmtContext = new Compound_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 78, Python3Parser.RULE_compound_stmt);
		try {
			this.state = 621;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.IF:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 612;
				this.if_stmt();
				}
				break;
			case Python3Parser.WHILE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 613;
				this.while_stmt();
				}
				break;
			case Python3Parser.FOR:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 614;
				this.for_stmt();
				}
				break;
			case Python3Parser.TRY:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 615;
				this.try_stmt();
				}
				break;
			case Python3Parser.WITH:
				this.enterOuterAlt(_localctx, 5);
				{
				this.state = 616;
				this.with_stmt();
				}
				break;
			case Python3Parser.DEF:
				this.enterOuterAlt(_localctx, 6);
				{
				this.state = 617;
				this.funcdef();
				}
				break;
			case Python3Parser.CLASS:
				this.enterOuterAlt(_localctx, 7);
				{
				this.state = 618;
				this.classdef();
				}
				break;
			case Python3Parser.AT:
				this.enterOuterAlt(_localctx, 8);
				{
				this.state = 619;
				this.decorated();
				}
				break;
			case Python3Parser.ASYNC:
				this.enterOuterAlt(_localctx, 9);
				{
				this.state = 620;
				this.async_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public async_stmt(): Async_stmtContext {
		let _localctx: Async_stmtContext = new Async_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 80, Python3Parser.RULE_async_stmt);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 623;
			this.match(Python3Parser.ASYNC);
			this.state = 627;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.DEF:
				{
				this.state = 624;
				this.funcdef();
				}
				break;
			case Python3Parser.WITH:
				{
				this.state = 625;
				this.with_stmt();
				}
				break;
			case Python3Parser.FOR:
				{
				this.state = 626;
				this.for_stmt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public if_stmt(): If_stmtContext {
		let _localctx: If_stmtContext = new If_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 82, Python3Parser.RULE_if_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 629;
			this.match(Python3Parser.IF);
			this.state = 630;
			this.test();
			this.state = 631;
			this.match(Python3Parser.COLON);
			this.state = 632;
			this.suite();
			this.state = 640;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.ELIF) {
				{
				{
				this.state = 633;
				this.match(Python3Parser.ELIF);
				this.state = 634;
				this.test();
				this.state = 635;
				this.match(Python3Parser.COLON);
				this.state = 636;
				this.suite();
				}
				}
				this.state = 642;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 646;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.ELSE) {
				{
				this.state = 643;
				this.match(Python3Parser.ELSE);
				this.state = 644;
				this.match(Python3Parser.COLON);
				this.state = 645;
				this.suite();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public while_stmt(): While_stmtContext {
		let _localctx: While_stmtContext = new While_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 84, Python3Parser.RULE_while_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 648;
			this.match(Python3Parser.WHILE);
			this.state = 649;
			this.test();
			this.state = 650;
			this.match(Python3Parser.COLON);
			this.state = 651;
			this.suite();
			this.state = 655;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.ELSE) {
				{
				this.state = 652;
				this.match(Python3Parser.ELSE);
				this.state = 653;
				this.match(Python3Parser.COLON);
				this.state = 654;
				this.suite();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public for_stmt(): For_stmtContext {
		let _localctx: For_stmtContext = new For_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 86, Python3Parser.RULE_for_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 657;
			this.match(Python3Parser.FOR);
			this.state = 658;
			this.exprlist();
			this.state = 659;
			this.match(Python3Parser.IN);
			this.state = 660;
			this.testlist();
			this.state = 661;
			this.match(Python3Parser.COLON);
			this.state = 662;
			this.suite();
			this.state = 666;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.ELSE) {
				{
				this.state = 663;
				this.match(Python3Parser.ELSE);
				this.state = 664;
				this.match(Python3Parser.COLON);
				this.state = 665;
				this.suite();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public try_stmt(): Try_stmtContext {
		let _localctx: Try_stmtContext = new Try_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 88, Python3Parser.RULE_try_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			{
			this.state = 668;
			this.match(Python3Parser.TRY);
			this.state = 669;
			this.match(Python3Parser.COLON);
			this.state = 670;
			this.suite();
			this.state = 692;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.EXCEPT:
				{
				this.state = 675;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 671;
					this.except_clause();
					this.state = 672;
					this.match(Python3Parser.COLON);
					this.state = 673;
					this.suite();
					}
					}
					this.state = 677;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while (_la === Python3Parser.EXCEPT);
				this.state = 682;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.ELSE) {
					{
					this.state = 679;
					this.match(Python3Parser.ELSE);
					this.state = 680;
					this.match(Python3Parser.COLON);
					this.state = 681;
					this.suite();
					}
				}

				this.state = 687;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.FINALLY) {
					{
					this.state = 684;
					this.match(Python3Parser.FINALLY);
					this.state = 685;
					this.match(Python3Parser.COLON);
					this.state = 686;
					this.suite();
					}
				}

				}
				break;
			case Python3Parser.FINALLY:
				{
				this.state = 689;
				this.match(Python3Parser.FINALLY);
				this.state = 690;
				this.match(Python3Parser.COLON);
				this.state = 691;
				this.suite();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public with_stmt(): With_stmtContext {
		let _localctx: With_stmtContext = new With_stmtContext(this._ctx, this.state);
		this.enterRule(_localctx, 90, Python3Parser.RULE_with_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 694;
			this.match(Python3Parser.WITH);
			this.state = 695;
			this.with_item();
			this.state = 700;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.COMMA) {
				{
				{
				this.state = 696;
				this.match(Python3Parser.COMMA);
				this.state = 697;
				this.with_item();
				}
				}
				this.state = 702;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			this.state = 703;
			this.match(Python3Parser.COLON);
			this.state = 704;
			this.suite();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public with_item(): With_itemContext {
		let _localctx: With_itemContext = new With_itemContext(this._ctx, this.state);
		this.enterRule(_localctx, 92, Python3Parser.RULE_with_item);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 706;
			this.test();
			this.state = 709;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.AS) {
				{
				this.state = 707;
				this.match(Python3Parser.AS);
				this.state = 708;
				this.expr();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public except_clause(): Except_clauseContext {
		let _localctx: Except_clauseContext = new Except_clauseContext(this._ctx, this.state);
		this.enterRule(_localctx, 94, Python3Parser.RULE_except_clause);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 711;
			this.match(Python3Parser.EXCEPT);
			this.state = 717;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
				{
				this.state = 712;
				this.test();
				this.state = 715;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.AS) {
					{
					this.state = 713;
					this.match(Python3Parser.AS);
					this.state = 714;
					this.match(Python3Parser.NAME);
					}
				}

				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public suite(): SuiteContext {
		let _localctx: SuiteContext = new SuiteContext(this._ctx, this.state);
		this.enterRule(_localctx, 96, Python3Parser.RULE_suite);
		let _la: number;
		try {
			this.state = 729;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.RETURN:
			case Python3Parser.RAISE:
			case Python3Parser.FROM:
			case Python3Parser.IMPORT:
			case Python3Parser.GLOBAL:
			case Python3Parser.NONLOCAL:
			case Python3Parser.ASSERT:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.YIELD:
			case Python3Parser.DEL:
			case Python3Parser.PASS:
			case Python3Parser.CONTINUE:
			case Python3Parser.BREAK:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.STAR:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 719;
				this.simple_stmt();
				}
				break;
			case Python3Parser.NEWLINE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 720;
				this.match(Python3Parser.NEWLINE);
				this.state = 721;
				this.match(Python3Parser.INDENT);
				this.state = 723;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 722;
					this.stmt();
					}
					}
					this.state = 725;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.DEF) | (1 << Python3Parser.RETURN) | (1 << Python3Parser.RAISE) | (1 << Python3Parser.FROM) | (1 << Python3Parser.IMPORT) | (1 << Python3Parser.GLOBAL) | (1 << Python3Parser.NONLOCAL) | (1 << Python3Parser.ASSERT) | (1 << Python3Parser.IF) | (1 << Python3Parser.WHILE) | (1 << Python3Parser.FOR) | (1 << Python3Parser.TRY) | (1 << Python3Parser.WITH) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.CLASS - 32)) | (1 << (Python3Parser.YIELD - 32)) | (1 << (Python3Parser.DEL - 32)) | (1 << (Python3Parser.PASS - 32)) | (1 << (Python3Parser.CONTINUE - 32)) | (1 << (Python3Parser.BREAK - 32)) | (1 << (Python3Parser.ASYNC - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)) | (1 << (Python3Parser.AT - 65)))) !== 0));
				this.state = 727;
				this.match(Python3Parser.DEDENT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public test(): TestContext {
		let _localctx: TestContext = new TestContext(this._ctx, this.state);
		this.enterRule(_localctx, 98, Python3Parser.RULE_test);
		let _la: number;
		try {
			this.state = 740;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 731;
				this.or_test();
				this.state = 737;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.IF) {
					{
					this.state = 732;
					this.match(Python3Parser.IF);
					this.state = 733;
					this.or_test();
					this.state = 734;
					this.match(Python3Parser.ELSE);
					this.state = 735;
					this.test();
					}
				}

				}
				break;
			case Python3Parser.LAMBDA:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 739;
				this.lambdef();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public test_nocond(): Test_nocondContext {
		let _localctx: Test_nocondContext = new Test_nocondContext(this._ctx, this.state);
		this.enterRule(_localctx, 100, Python3Parser.RULE_test_nocond);
		try {
			this.state = 744;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 742;
				this.or_test();
				}
				break;
			case Python3Parser.LAMBDA:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 743;
				this.lambdef_nocond();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public lambdef(): LambdefContext {
		let _localctx: LambdefContext = new LambdefContext(this._ctx, this.state);
		this.enterRule(_localctx, 102, Python3Parser.RULE_lambdef);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 746;
			this.match(Python3Parser.LAMBDA);
			this.state = 748;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 46)) & ~0x1F) === 0 && ((1 << (_la - 46)) & ((1 << (Python3Parser.NAME - 46)) | (1 << (Python3Parser.STAR - 46)) | (1 << (Python3Parser.POWER - 46)))) !== 0)) {
				{
				this.state = 747;
				this.varargslist();
				}
			}

			this.state = 750;
			this.match(Python3Parser.COLON);
			this.state = 751;
			this.test();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public lambdef_nocond(): Lambdef_nocondContext {
		let _localctx: Lambdef_nocondContext = new Lambdef_nocondContext(this._ctx, this.state);
		this.enterRule(_localctx, 104, Python3Parser.RULE_lambdef_nocond);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 753;
			this.match(Python3Parser.LAMBDA);
			this.state = 755;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 46)) & ~0x1F) === 0 && ((1 << (_la - 46)) & ((1 << (Python3Parser.NAME - 46)) | (1 << (Python3Parser.STAR - 46)) | (1 << (Python3Parser.POWER - 46)))) !== 0)) {
				{
				this.state = 754;
				this.varargslist();
				}
			}

			this.state = 757;
			this.match(Python3Parser.COLON);
			this.state = 758;
			this.test_nocond();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public or_test(): Or_testContext {
		let _localctx: Or_testContext = new Or_testContext(this._ctx, this.state);
		this.enterRule(_localctx, 106, Python3Parser.RULE_or_test);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 760;
			this.and_test();
			this.state = 765;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.OR) {
				{
				{
				this.state = 761;
				this.match(Python3Parser.OR);
				this.state = 762;
				this.and_test();
				}
				}
				this.state = 767;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public and_test(): And_testContext {
		let _localctx: And_testContext = new And_testContext(this._ctx, this.state);
		this.enterRule(_localctx, 108, Python3Parser.RULE_and_test);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 768;
			this.not_test();
			this.state = 773;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.AND) {
				{
				{
				this.state = 769;
				this.match(Python3Parser.AND);
				this.state = 770;
				this.not_test();
				}
				}
				this.state = 775;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public not_test(): Not_testContext {
		let _localctx: Not_testContext = new Not_testContext(this._ctx, this.state);
		this.enterRule(_localctx, 110, Python3Parser.RULE_not_test);
		try {
			this.state = 779;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.NOT:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 776;
				this.match(Python3Parser.NOT);
				this.state = 777;
				this.not_test();
				}
				break;
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 778;
				this.comparison();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public comparison(): ComparisonContext {
		let _localctx: ComparisonContext = new ComparisonContext(this._ctx, this.state);
		this.enterRule(_localctx, 112, Python3Parser.RULE_comparison);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 781;
			this.expr();
			this.state = 787;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (((((_la - 24)) & ~0x1F) === 0 && ((1 << (_la - 24)) & ((1 << (Python3Parser.IN - 24)) | (1 << (Python3Parser.NOT - 24)) | (1 << (Python3Parser.IS - 24)))) !== 0) || ((((_la - 81)) & ~0x1F) === 0 && ((1 << (_la - 81)) & ((1 << (Python3Parser.LESS_THAN - 81)) | (1 << (Python3Parser.GREATER_THAN - 81)) | (1 << (Python3Parser.EQUALS - 81)) | (1 << (Python3Parser.GT_EQ - 81)) | (1 << (Python3Parser.LT_EQ - 81)) | (1 << (Python3Parser.NOT_EQ_1 - 81)) | (1 << (Python3Parser.NOT_EQ_2 - 81)))) !== 0)) {
				{
				{
				this.state = 782;
				this.comp_op();
				this.state = 783;
				this.expr();
				}
				}
				this.state = 789;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public comp_op(): Comp_opContext {
		let _localctx: Comp_opContext = new Comp_opContext(this._ctx, this.state);
		this.enterRule(_localctx, 114, Python3Parser.RULE_comp_op);
		try {
			this.state = 803;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 107, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 790;
				this.match(Python3Parser.LESS_THAN);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 791;
				this.match(Python3Parser.GREATER_THAN);
				}
				break;

			case 3:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 792;
				this.match(Python3Parser.EQUALS);
				}
				break;

			case 4:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 793;
				this.match(Python3Parser.GT_EQ);
				}
				break;

			case 5:
				this.enterOuterAlt(_localctx, 5);
				{
				this.state = 794;
				this.match(Python3Parser.LT_EQ);
				}
				break;

			case 6:
				this.enterOuterAlt(_localctx, 6);
				{
				this.state = 795;
				this.match(Python3Parser.NOT_EQ_1);
				}
				break;

			case 7:
				this.enterOuterAlt(_localctx, 7);
				{
				this.state = 796;
				this.match(Python3Parser.NOT_EQ_2);
				}
				break;

			case 8:
				this.enterOuterAlt(_localctx, 8);
				{
				this.state = 797;
				this.match(Python3Parser.IN);
				}
				break;

			case 9:
				this.enterOuterAlt(_localctx, 9);
				{
				this.state = 798;
				this.match(Python3Parser.NOT);
				this.state = 799;
				this.match(Python3Parser.IN);
				}
				break;

			case 10:
				this.enterOuterAlt(_localctx, 10);
				{
				this.state = 800;
				this.match(Python3Parser.IS);
				}
				break;

			case 11:
				this.enterOuterAlt(_localctx, 11);
				{
				this.state = 801;
				this.match(Python3Parser.IS);
				this.state = 802;
				this.match(Python3Parser.NOT);
				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public star_expr(): Star_exprContext {
		let _localctx: Star_exprContext = new Star_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 116, Python3Parser.RULE_star_expr);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 805;
			this.match(Python3Parser.STAR);
			this.state = 806;
			this.expr();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public expr(): ExprContext {
		let _localctx: ExprContext = new ExprContext(this._ctx, this.state);
		this.enterRule(_localctx, 118, Python3Parser.RULE_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 808;
			this.xor_expr();
			this.state = 813;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.OR_OP) {
				{
				{
				this.state = 809;
				this.match(Python3Parser.OR_OP);
				this.state = 810;
				this.xor_expr();
				}
				}
				this.state = 815;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public xor_expr(): Xor_exprContext {
		let _localctx: Xor_exprContext = new Xor_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 120, Python3Parser.RULE_xor_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 816;
			this.and_expr();
			this.state = 821;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.XOR) {
				{
				{
				this.state = 817;
				this.match(Python3Parser.XOR);
				this.state = 818;
				this.and_expr();
				}
				}
				this.state = 823;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public and_expr(): And_exprContext {
		let _localctx: And_exprContext = new And_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 122, Python3Parser.RULE_and_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 824;
			this.shift_expr();
			this.state = 829;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.AND_OP) {
				{
				{
				this.state = 825;
				this.match(Python3Parser.AND_OP);
				this.state = 826;
				this.shift_expr();
				}
				}
				this.state = 831;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public shift_expr(): Shift_exprContext {
		let _localctx: Shift_exprContext = new Shift_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 124, Python3Parser.RULE_shift_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 832;
			this.arith_expr();
			this.state = 837;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.LEFT_SHIFT || _la === Python3Parser.RIGHT_SHIFT) {
				{
				{
				this.state = 833;
				_la = this._input.LA(1);
				if (!(_la === Python3Parser.LEFT_SHIFT || _la === Python3Parser.RIGHT_SHIFT)) {
				this._errHandler.recoverInline(this);
				} else {
					if (this._input.LA(1) === Token.EOF) {
						this.matchedEOF = true;
					}

					this._errHandler.reportMatch(this);
					this.consume();
				}
				this.state = 834;
				this.arith_expr();
				}
				}
				this.state = 839;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public arith_expr(): Arith_exprContext {
		let _localctx: Arith_exprContext = new Arith_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 126, Python3Parser.RULE_arith_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 840;
			this.term();
			this.state = 845;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === Python3Parser.ADD || _la === Python3Parser.MINUS) {
				{
				{
				this.state = 841;
				_la = this._input.LA(1);
				if (!(_la === Python3Parser.ADD || _la === Python3Parser.MINUS)) {
				this._errHandler.recoverInline(this);
				} else {
					if (this._input.LA(1) === Token.EOF) {
						this.matchedEOF = true;
					}

					this._errHandler.reportMatch(this);
					this.consume();
				}
				this.state = 842;
				this.term();
				}
				}
				this.state = 847;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public term(): TermContext {
		let _localctx: TermContext = new TermContext(this._ctx, this.state);
		this.enterRule(_localctx, 128, Python3Parser.RULE_term);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 848;
			this.factor();
			this.state = 853;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (((((_la - 57)) & ~0x1F) === 0 && ((1 << (_la - 57)) & ((1 << (Python3Parser.STAR - 57)) | (1 << (Python3Parser.DIV - 57)) | (1 << (Python3Parser.MOD - 57)) | (1 << (Python3Parser.IDIV - 57)) | (1 << (Python3Parser.AT - 57)))) !== 0)) {
				{
				{
				this.state = 849;
				_la = this._input.LA(1);
				if (!(((((_la - 57)) & ~0x1F) === 0 && ((1 << (_la - 57)) & ((1 << (Python3Parser.STAR - 57)) | (1 << (Python3Parser.DIV - 57)) | (1 << (Python3Parser.MOD - 57)) | (1 << (Python3Parser.IDIV - 57)) | (1 << (Python3Parser.AT - 57)))) !== 0))) {
				this._errHandler.recoverInline(this);
				} else {
					if (this._input.LA(1) === Token.EOF) {
						this.matchedEOF = true;
					}

					this._errHandler.reportMatch(this);
					this.consume();
				}
				this.state = 850;
				this.factor();
				}
				}
				this.state = 855;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public factor(): FactorContext {
		let _localctx: FactorContext = new FactorContext(this._ctx, this.state);
		this.enterRule(_localctx, 130, Python3Parser.RULE_factor);
		let _la: number;
		try {
			this.state = 859;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 856;
				_la = this._input.LA(1);
				if (!(((((_la - 72)) & ~0x1F) === 0 && ((1 << (_la - 72)) & ((1 << (Python3Parser.ADD - 72)) | (1 << (Python3Parser.MINUS - 72)) | (1 << (Python3Parser.NOT_OP - 72)))) !== 0))) {
				this._errHandler.recoverInline(this);
				} else {
					if (this._input.LA(1) === Token.EOF) {
						this.matchedEOF = true;
					}

					this._errHandler.reportMatch(this);
					this.consume();
				}
				this.state = 857;
				this.factor();
				}
				break;
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 858;
				this.power();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public power(): PowerContext {
		let _localctx: PowerContext = new PowerContext(this._ctx, this.state);
		this.enterRule(_localctx, 132, Python3Parser.RULE_power);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 861;
			this.atom_expr();
			this.state = 864;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.POWER) {
				{
				this.state = 862;
				this.match(Python3Parser.POWER);
				this.state = 863;
				this.factor();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public atom_expr(): Atom_exprContext {
		let _localctx: Atom_exprContext = new Atom_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 134, Python3Parser.RULE_atom_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 867;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.AWAIT) {
				{
				this.state = 866;
				this.match(Python3Parser.AWAIT);
				}
			}

			this.state = 869;
			this.atom();
			this.state = 873;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (((((_la - 55)) & ~0x1F) === 0 && ((1 << (_la - 55)) & ((1 << (Python3Parser.DOT - 55)) | (1 << (Python3Parser.OPEN_PAREN - 55)) | (1 << (Python3Parser.OPEN_BRACK - 55)))) !== 0)) {
				{
				{
				this.state = 870;
				this.trailer();
				}
				}
				this.state = 875;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public atom(): AtomContext {
		let _localctx: AtomContext = new AtomContext(this._ctx, this.state);
		this.enterRule(_localctx, 136, Python3Parser.RULE_atom);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 908;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.OPEN_PAREN:
				{
				this.state = 876;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 879;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.YIELD:
					{
					this.state = 877;
					this.yield_expr();
					}
					break;
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.STAR:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
					{
					this.state = 878;
					this.testlist_comp();
					}
					break;
				case Python3Parser.CLOSE_PAREN:
					break;
				default:
					break;
				}
				this.state = 881;
				this.match(Python3Parser.CLOSE_PAREN);
				}
				break;
			case Python3Parser.OPEN_BRACK:
				{
				this.state = 882;
				this.match(Python3Parser.OPEN_BRACK);
				this.state = 884;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
					{
					this.state = 883;
					this.testlist_comp();
					}
				}

				this.state = 886;
				this.match(Python3Parser.CLOSE_BRACK);
				}
				break;
			case Python3Parser.OPEN_BRACE:
				{
				this.state = 887;
				this.match(Python3Parser.OPEN_BRACE);
				this.state = 889;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)) | (1 << (Python3Parser.POWER - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
					{
					this.state = 888;
					this.dictorsetmaker();
					}
				}

				this.state = 891;
				this.match(Python3Parser.CLOSE_BRACE);
				}
				break;
			case Python3Parser.NAME:
				{
				this.state = 892;
				this.match(Python3Parser.NAME);
				}
				break;
			case Python3Parser.NUMBER:
				{
				this.state = 893;
				this.match(Python3Parser.NUMBER);
				}
				break;
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				{
				this.state = 895;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 894;
					this.string_template();
					}
					}
					this.state = 897;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START))) !== 0));
				}
				break;
			case Python3Parser.STRING:
				{
				this.state = 900;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				do {
					{
					{
					this.state = 899;
					this.match(Python3Parser.STRING);
					}
					}
					this.state = 902;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while (_la === Python3Parser.STRING);
				}
				break;
			case Python3Parser.ELLIPSIS:
				{
				this.state = 904;
				this.match(Python3Parser.ELLIPSIS);
				}
				break;
			case Python3Parser.NONE:
				{
				this.state = 905;
				this.match(Python3Parser.NONE);
				}
				break;
			case Python3Parser.TRUE:
				{
				this.state = 906;
				this.match(Python3Parser.TRUE);
				}
				break;
			case Python3Parser.FALSE:
				{
				this.state = 907;
				this.match(Python3Parser.FALSE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public testlist_comp(): Testlist_compContext {
		let _localctx: Testlist_compContext = new Testlist_compContext(this._ctx, this.state);
		this.enterRule(_localctx, 138, Python3Parser.RULE_testlist_comp);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 912;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				{
				this.state = 910;
				this.test();
				}
				break;
			case Python3Parser.STAR:
				{
				this.state = 911;
				this.star_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 928;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.FOR:
			case Python3Parser.ASYNC:
				{
				this.state = 914;
				this.comp_for();
				}
				break;
			case Python3Parser.CLOSE_PAREN:
			case Python3Parser.COMMA:
			case Python3Parser.CLOSE_BRACK:
				{
				this.state = 922;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 126, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 915;
						this.match(Python3Parser.COMMA);
						this.state = 918;
						this._errHandler.sync(this);
						switch (this._input.LA(1)) {
						case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
						case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
						case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
						case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
						case Python3Parser.STRING:
						case Python3Parser.NUMBER:
						case Python3Parser.LAMBDA:
						case Python3Parser.NOT:
						case Python3Parser.NONE:
						case Python3Parser.TRUE:
						case Python3Parser.FALSE:
						case Python3Parser.AWAIT:
						case Python3Parser.NAME:
						case Python3Parser.ELLIPSIS:
						case Python3Parser.OPEN_PAREN:
						case Python3Parser.OPEN_BRACK:
						case Python3Parser.ADD:
						case Python3Parser.MINUS:
						case Python3Parser.NOT_OP:
						case Python3Parser.OPEN_BRACE:
							{
							this.state = 916;
							this.test();
							}
							break;
						case Python3Parser.STAR:
							{
							this.state = 917;
							this.star_expr();
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						}
						}
					}
					this.state = 924;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 126, this._ctx);
				}
				this.state = 926;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COMMA) {
					{
					this.state = 925;
					this.match(Python3Parser.COMMA);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public trailer(): TrailerContext {
		let _localctx: TrailerContext = new TrailerContext(this._ctx, this.state);
		this.enterRule(_localctx, 140, Python3Parser.RULE_trailer);
		try {
			this.state = 937;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.OPEN_PAREN:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 930;
				this.callArguments();
				}
				break;
			case Python3Parser.OPEN_BRACK:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 931;
				this.match(Python3Parser.OPEN_BRACK);
				this.state = 932;
				this.subscriptlist();
				this.state = 933;
				this.match(Python3Parser.CLOSE_BRACK);
				}
				break;
			case Python3Parser.DOT:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 935;
				this.match(Python3Parser.DOT);
				this.state = 936;
				this.match(Python3Parser.NAME);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public subscriptlist(): SubscriptlistContext {
		let _localctx: SubscriptlistContext = new SubscriptlistContext(this._ctx, this.state);
		this.enterRule(_localctx, 142, Python3Parser.RULE_subscriptlist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 939;
			this.subscript();
			this.state = 944;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 130, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 940;
					this.match(Python3Parser.COMMA);
					this.state = 941;
					this.subscript();
					}
					}
				}
				this.state = 946;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 130, this._ctx);
			}
			this.state = 948;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 947;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public subscript(): SubscriptContext {
		let _localctx: SubscriptContext = new SubscriptContext(this._ctx, this.state);
		this.enterRule(_localctx, 144, Python3Parser.RULE_subscript);
		let _la: number;
		try {
			this.state = 961;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 135, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 950;
				this.test();
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 952;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
					{
					this.state = 951;
					this.test();
					}
				}

				this.state = 954;
				this.match(Python3Parser.COLON);
				this.state = 956;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
					{
					this.state = 955;
					this.test();
					}
				}

				this.state = 959;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.COLON) {
					{
					this.state = 958;
					this.sliceop();
					}
				}

				}
				break;
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public sliceop(): SliceopContext {
		let _localctx: SliceopContext = new SliceopContext(this._ctx, this.state);
		this.enterRule(_localctx, 146, Python3Parser.RULE_sliceop);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 963;
			this.match(Python3Parser.COLON);
			this.state = 965;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
				{
				this.state = 964;
				this.test();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public exprlist(): ExprlistContext {
		let _localctx: ExprlistContext = new ExprlistContext(this._ctx, this.state);
		this.enterRule(_localctx, 148, Python3Parser.RULE_exprlist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 969;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				{
				this.state = 967;
				this.expr();
				}
				break;
			case Python3Parser.STAR:
				{
				this.state = 968;
				this.star_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this.state = 978;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 139, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 971;
					this.match(Python3Parser.COMMA);
					this.state = 974;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
					case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
					case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
					case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
					case Python3Parser.STRING:
					case Python3Parser.NUMBER:
					case Python3Parser.NONE:
					case Python3Parser.TRUE:
					case Python3Parser.FALSE:
					case Python3Parser.AWAIT:
					case Python3Parser.NAME:
					case Python3Parser.ELLIPSIS:
					case Python3Parser.OPEN_PAREN:
					case Python3Parser.OPEN_BRACK:
					case Python3Parser.ADD:
					case Python3Parser.MINUS:
					case Python3Parser.NOT_OP:
					case Python3Parser.OPEN_BRACE:
						{
						this.state = 972;
						this.expr();
						}
						break;
					case Python3Parser.STAR:
						{
						this.state = 973;
						this.star_expr();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					}
				}
				this.state = 980;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 139, this._ctx);
			}
			this.state = 982;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 981;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public testlist(): TestlistContext {
		let _localctx: TestlistContext = new TestlistContext(this._ctx, this.state);
		this.enterRule(_localctx, 150, Python3Parser.RULE_testlist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 984;
			this.test();
			this.state = 989;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 141, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 985;
					this.match(Python3Parser.COMMA);
					this.state = 986;
					this.test();
					}
					}
				}
				this.state = 991;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 141, this._ctx);
			}
			this.state = 993;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 992;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public dictorsetmaker(): DictorsetmakerContext {
		let _localctx: DictorsetmakerContext = new DictorsetmakerContext(this._ctx, this.state);
		this.enterRule(_localctx, 152, Python3Parser.RULE_dictorsetmaker);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1043;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 153, this._ctx) ) {
			case 1:
				{
				{
				this.state = 1001;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
					{
					this.state = 995;
					this.test();
					this.state = 996;
					this.match(Python3Parser.COLON);
					this.state = 997;
					this.test();
					}
					break;
				case Python3Parser.POWER:
					{
					this.state = 999;
					this.match(Python3Parser.POWER);
					this.state = 1000;
					this.expr();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				this.state = 1021;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.FOR:
				case Python3Parser.ASYNC:
					{
					this.state = 1003;
					this.comp_for();
					}
					break;
				case Python3Parser.COMMA:
				case Python3Parser.CLOSE_BRACE:
					{
					this.state = 1015;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 145, this._ctx);
					while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
						if (_alt === 1) {
							{
							{
							this.state = 1004;
							this.match(Python3Parser.COMMA);
							this.state = 1011;
							this._errHandler.sync(this);
							switch (this._input.LA(1)) {
							case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
							case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
							case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
							case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
							case Python3Parser.STRING:
							case Python3Parser.NUMBER:
							case Python3Parser.LAMBDA:
							case Python3Parser.NOT:
							case Python3Parser.NONE:
							case Python3Parser.TRUE:
							case Python3Parser.FALSE:
							case Python3Parser.AWAIT:
							case Python3Parser.NAME:
							case Python3Parser.ELLIPSIS:
							case Python3Parser.OPEN_PAREN:
							case Python3Parser.OPEN_BRACK:
							case Python3Parser.ADD:
							case Python3Parser.MINUS:
							case Python3Parser.NOT_OP:
							case Python3Parser.OPEN_BRACE:
								{
								this.state = 1005;
								this.test();
								this.state = 1006;
								this.match(Python3Parser.COLON);
								this.state = 1007;
								this.test();
								}
								break;
							case Python3Parser.POWER:
								{
								this.state = 1009;
								this.match(Python3Parser.POWER);
								this.state = 1010;
								this.expr();
								}
								break;
							default:
								throw new NoViableAltException(this);
							}
							}
							}
						}
						this.state = 1017;
						this._errHandler.sync(this);
						_alt = this.interpreter.adaptivePredict(this._input, 145, this._ctx);
					}
					this.state = 1019;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === Python3Parser.COMMA) {
						{
						this.state = 1018;
						this.match(Python3Parser.COMMA);
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				break;

			case 2:
				{
				{
				this.state = 1025;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
					{
					this.state = 1023;
					this.test();
					}
					break;
				case Python3Parser.STAR:
					{
					this.state = 1024;
					this.star_expr();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				this.state = 1041;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.FOR:
				case Python3Parser.ASYNC:
					{
					this.state = 1027;
					this.comp_for();
					}
					break;
				case Python3Parser.COMMA:
				case Python3Parser.CLOSE_BRACE:
					{
					this.state = 1035;
					this._errHandler.sync(this);
					_alt = this.interpreter.adaptivePredict(this._input, 150, this._ctx);
					while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
						if (_alt === 1) {
							{
							{
							this.state = 1028;
							this.match(Python3Parser.COMMA);
							this.state = 1031;
							this._errHandler.sync(this);
							switch (this._input.LA(1)) {
							case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
							case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
							case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
							case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
							case Python3Parser.STRING:
							case Python3Parser.NUMBER:
							case Python3Parser.LAMBDA:
							case Python3Parser.NOT:
							case Python3Parser.NONE:
							case Python3Parser.TRUE:
							case Python3Parser.FALSE:
							case Python3Parser.AWAIT:
							case Python3Parser.NAME:
							case Python3Parser.ELLIPSIS:
							case Python3Parser.OPEN_PAREN:
							case Python3Parser.OPEN_BRACK:
							case Python3Parser.ADD:
							case Python3Parser.MINUS:
							case Python3Parser.NOT_OP:
							case Python3Parser.OPEN_BRACE:
								{
								this.state = 1029;
								this.test();
								}
								break;
							case Python3Parser.STAR:
								{
								this.state = 1030;
								this.star_expr();
								}
								break;
							default:
								throw new NoViableAltException(this);
							}
							}
							}
						}
						this.state = 1037;
						this._errHandler.sync(this);
						_alt = this.interpreter.adaptivePredict(this._input, 150, this._ctx);
					}
					this.state = 1039;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la === Python3Parser.COMMA) {
						{
						this.state = 1038;
						this.match(Python3Parser.COMMA);
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				break;
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public classdef(): ClassdefContext {
		let _localctx: ClassdefContext = new ClassdefContext(this._ctx, this.state);
		this.enterRule(_localctx, 154, Python3Parser.RULE_classdef);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1045;
			this.match(Python3Parser.CLASS);
			this.state = 1046;
			this.match(Python3Parser.NAME);
			this.state = 1052;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.OPEN_PAREN) {
				{
				this.state = 1047;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 1049;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)) | (1 << (Python3Parser.POWER - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
					{
					this.state = 1048;
					this.arglist();
					}
				}

				this.state = 1051;
				this.match(Python3Parser.CLOSE_PAREN);
				}
			}

			this.state = 1054;
			this.match(Python3Parser.COLON);
			this.state = 1055;
			this.suite();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public callArguments(): CallArgumentsContext {
		let _localctx: CallArgumentsContext = new CallArgumentsContext(this._ctx, this.state);
		this.enterRule(_localctx, 156, Python3Parser.RULE_callArguments);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1057;
			this.match(Python3Parser.OPEN_PAREN);
			this.state = 1059;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.STAR - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)) | (1 << (Python3Parser.POWER - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
				{
				this.state = 1058;
				this.arglist();
				}
			}

			this.state = 1061;
			this.match(Python3Parser.CLOSE_PAREN);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public arglist(): ArglistContext {
		let _localctx: ArglistContext = new ArglistContext(this._ctx, this.state);
		this.enterRule(_localctx, 158, Python3Parser.RULE_arglist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1063;
			this.argument();
			this.state = 1068;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 157, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 1064;
					this.match(Python3Parser.COMMA);
					this.state = 1065;
					this.argument();
					}
					}
				}
				this.state = 1070;
				this._errHandler.sync(this);
				_alt = this.interpreter.adaptivePredict(this._input, 157, this._ctx);
			}
			this.state = 1072;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.COMMA) {
				{
				this.state = 1071;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public argument(): ArgumentContext {
		let _localctx: ArgumentContext = new ArgumentContext(this._ctx, this.state);
		this.enterRule(_localctx, 160, Python3Parser.RULE_argument);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1086;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 160, this._ctx) ) {
			case 1:
				{
				this.state = 1074;
				this.test();
				this.state = 1076;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === Python3Parser.FOR || _la === Python3Parser.ASYNC) {
					{
					this.state = 1075;
					this.comp_for();
					}
				}

				}
				break;

			case 2:
				{
				this.state = 1078;
				this.test();
				this.state = 1079;
				this.match(Python3Parser.ASSIGN);
				this.state = 1080;
				this.test();
				}
				break;

			case 3:
				{
				this.state = 1082;
				this.match(Python3Parser.POWER);
				this.state = 1083;
				this.test();
				}
				break;

			case 4:
				{
				this.state = 1084;
				this.match(Python3Parser.STAR);
				this.state = 1085;
				this.test();
				}
				break;
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public comp_iter(): Comp_iterContext {
		let _localctx: Comp_iterContext = new Comp_iterContext(this._ctx, this.state);
		this.enterRule(_localctx, 162, Python3Parser.RULE_comp_iter);
		try {
			this.state = 1090;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.FOR:
			case Python3Parser.ASYNC:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1088;
				this.comp_for();
				}
				break;
			case Python3Parser.IF:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1089;
				this.comp_if();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public comp_for(): Comp_forContext {
		let _localctx: Comp_forContext = new Comp_forContext(this._ctx, this.state);
		this.enterRule(_localctx, 164, Python3Parser.RULE_comp_for);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1093;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === Python3Parser.ASYNC) {
				{
				this.state = 1092;
				this.match(Python3Parser.ASYNC);
				}
			}

			this.state = 1095;
			this.match(Python3Parser.FOR);
			this.state = 1096;
			this.exprlist();
			this.state = 1097;
			this.match(Python3Parser.IN);
			this.state = 1098;
			this.or_test();
			this.state = 1100;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 19)) & ~0x1F) === 0 && ((1 << (_la - 19)) & ((1 << (Python3Parser.IF - 19)) | (1 << (Python3Parser.FOR - 19)) | (1 << (Python3Parser.ASYNC - 19)))) !== 0)) {
				{
				this.state = 1099;
				this.comp_iter();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public comp_if(): Comp_ifContext {
		let _localctx: Comp_ifContext = new Comp_ifContext(this._ctx, this.state);
		this.enterRule(_localctx, 166, Python3Parser.RULE_comp_if);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1102;
			this.match(Python3Parser.IF);
			this.state = 1103;
			this.test_nocond();
			this.state = 1105;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 19)) & ~0x1F) === 0 && ((1 << (_la - 19)) & ((1 << (Python3Parser.IF - 19)) | (1 << (Python3Parser.FOR - 19)) | (1 << (Python3Parser.ASYNC - 19)))) !== 0)) {
				{
				this.state = 1104;
				this.comp_iter();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public encoding_decl(): Encoding_declContext {
		let _localctx: Encoding_declContext = new Encoding_declContext(this._ctx, this.state);
		this.enterRule(_localctx, 168, Python3Parser.RULE_encoding_decl);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1107;
			this.match(Python3Parser.NAME);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public yield_expr(): Yield_exprContext {
		let _localctx: Yield_exprContext = new Yield_exprContext(this._ctx, this.state);
		this.enterRule(_localctx, 170, Python3Parser.RULE_yield_expr);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 1109;
			this.match(Python3Parser.YIELD);
			this.state = 1111;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START) | (1 << Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START) | (1 << Python3Parser.STRING) | (1 << Python3Parser.NUMBER) | (1 << Python3Parser.FROM) | (1 << Python3Parser.LAMBDA))) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & ((1 << (Python3Parser.NOT - 32)) | (1 << (Python3Parser.NONE - 32)) | (1 << (Python3Parser.TRUE - 32)) | (1 << (Python3Parser.FALSE - 32)) | (1 << (Python3Parser.AWAIT - 32)) | (1 << (Python3Parser.NAME - 32)) | (1 << (Python3Parser.ELLIPSIS - 32)) | (1 << (Python3Parser.OPEN_PAREN - 32)))) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & ((1 << (Python3Parser.OPEN_BRACK - 65)) | (1 << (Python3Parser.ADD - 65)) | (1 << (Python3Parser.MINUS - 65)) | (1 << (Python3Parser.NOT_OP - 65)) | (1 << (Python3Parser.OPEN_BRACE - 65)))) !== 0)) {
				{
				this.state = 1110;
				this.yield_arg();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public yield_arg(): Yield_argContext {
		let _localctx: Yield_argContext = new Yield_argContext(this._ctx, this.state);
		this.enterRule(_localctx, 172, Python3Parser.RULE_yield_arg);
		try {
			this.state = 1116;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.FROM:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1113;
				this.match(Python3Parser.FROM);
				this.state = 1114;
				this.test();
				}
				break;
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
			case Python3Parser.STRING:
			case Python3Parser.NUMBER:
			case Python3Parser.LAMBDA:
			case Python3Parser.NOT:
			case Python3Parser.NONE:
			case Python3Parser.TRUE:
			case Python3Parser.FALSE:
			case Python3Parser.AWAIT:
			case Python3Parser.NAME:
			case Python3Parser.ELLIPSIS:
			case Python3Parser.OPEN_PAREN:
			case Python3Parser.OPEN_BRACK:
			case Python3Parser.ADD:
			case Python3Parser.MINUS:
			case Python3Parser.NOT_OP:
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1115;
				this.testlist();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public string_template(): String_templateContext {
		let _localctx: String_templateContext = new String_templateContext(this._ctx, this.state);
		this.enterRule(_localctx, 174, Python3Parser.RULE_string_template);
		let _la: number;
		try {
			this.state = 1150;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1118;
				this.match(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START);
				this.state = 1122;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === Python3Parser.OPEN_BRACE || _la === Python3Parser.SINGLE_QUOTE_STRING_ATOM) {
					{
					{
					this.state = 1119;
					this.single_string_template_atom();
					}
					}
					this.state = 1124;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 1125;
				this.match(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END);
				}
				break;
			case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1126;
				this.match(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START);
				this.state = 1130;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === Python3Parser.OPEN_BRACE || _la === Python3Parser.SINGLE_QUOTE_STRING_ATOM) {
					{
					{
					this.state = 1127;
					this.single_string_template_atom();
					}
					}
					this.state = 1132;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 1133;
				this.match(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_END);
				}
				break;
			case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				this.enterOuterAlt(_localctx, 3);
				{
				this.state = 1134;
				this.match(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START);
				this.state = 1138;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === Python3Parser.OPEN_BRACE || _la === Python3Parser.DOUBLE_QUOTE_STRING_ATOM) {
					{
					{
					this.state = 1135;
					this.double_string_template_atom();
					}
					}
					this.state = 1140;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 1141;
				this.match(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END);
				}
				break;
			case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				this.enterOuterAlt(_localctx, 4);
				{
				this.state = 1142;
				this.match(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START);
				this.state = 1146;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la === Python3Parser.OPEN_BRACE || _la === Python3Parser.DOUBLE_QUOTE_STRING_ATOM) {
					{
					{
					this.state = 1143;
					this.double_string_template_atom();
					}
					}
					this.state = 1148;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				this.state = 1149;
				this.match(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public single_string_template_atom(): Single_string_template_atomContext {
		let _localctx: Single_string_template_atomContext = new Single_string_template_atomContext(this._ctx, this.state);
		this.enterRule(_localctx, 176, Python3Parser.RULE_single_string_template_atom);
		try {
			this.state = 1160;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.SINGLE_QUOTE_STRING_ATOM:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1152;
				this.match(Python3Parser.SINGLE_QUOTE_STRING_ATOM);
				}
				break;
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1153;
				this.match(Python3Parser.OPEN_BRACE);
				this.state = 1156;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
					{
					this.state = 1154;
					this.test();
					}
					break;
				case Python3Parser.STAR:
					{
					this.state = 1155;
					this.star_expr();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				this.state = 1158;
				this.match(Python3Parser.TEMPLATE_CLOSE_BRACE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public double_string_template_atom(): Double_string_template_atomContext {
		let _localctx: Double_string_template_atomContext = new Double_string_template_atomContext(this._ctx, this.state);
		this.enterRule(_localctx, 178, Python3Parser.RULE_double_string_template_atom);
		try {
			this.state = 1170;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case Python3Parser.DOUBLE_QUOTE_STRING_ATOM:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 1162;
				this.match(Python3Parser.DOUBLE_QUOTE_STRING_ATOM);
				}
				break;
			case Python3Parser.OPEN_BRACE:
				this.enterOuterAlt(_localctx, 2);
				{
				this.state = 1163;
				this.match(Python3Parser.OPEN_BRACE);
				this.state = 1166;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START:
				case Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START:
				case Python3Parser.STRING:
				case Python3Parser.NUMBER:
				case Python3Parser.LAMBDA:
				case Python3Parser.NOT:
				case Python3Parser.NONE:
				case Python3Parser.TRUE:
				case Python3Parser.FALSE:
				case Python3Parser.AWAIT:
				case Python3Parser.NAME:
				case Python3Parser.ELLIPSIS:
				case Python3Parser.OPEN_PAREN:
				case Python3Parser.OPEN_BRACK:
				case Python3Parser.ADD:
				case Python3Parser.MINUS:
				case Python3Parser.NOT_OP:
				case Python3Parser.OPEN_BRACE:
					{
					this.state = 1164;
					this.test();
					}
					break;
				case Python3Parser.STAR:
					{
					this.state = 1165;
					this.star_expr();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				this.state = 1168;
				this.match(Python3Parser.TEMPLATE_CLOSE_BRACE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				_localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return _localctx;
	}

	private static readonly _serializedATNSegments: number = 3;
	private static readonly _serializedATNSegment0: string =
		"\x03\uC91D\uCABA\u058D\uAFBA\u4F53\u0607\uEA8B\uC241\x03r\u0497\x04\x02" +
		"\t\x02\x04\x03\t\x03\x04\x04\t\x04\x04\x05\t\x05\x04\x06\t\x06\x04\x07" +
		"\t\x07\x04\b\t\b\x04\t\t\t\x04\n\t\n\x04\v\t\v\x04\f\t\f\x04\r\t\r\x04" +
		"\x0E\t\x0E\x04\x0F\t\x0F\x04\x10\t\x10\x04\x11\t\x11\x04\x12\t\x12\x04" +
		"\x13\t\x13\x04\x14\t\x14\x04\x15\t\x15\x04\x16\t\x16\x04\x17\t\x17\x04" +
		"\x18\t\x18\x04\x19\t\x19\x04\x1A\t\x1A\x04\x1B\t\x1B\x04\x1C\t\x1C\x04" +
		"\x1D\t\x1D\x04\x1E\t\x1E\x04\x1F\t\x1F\x04 \t \x04!\t!\x04\"\t\"\x04#" +
		"\t#\x04$\t$\x04%\t%\x04&\t&\x04\'\t\'\x04(\t(\x04)\t)\x04*\t*\x04+\t+" +
		"\x04,\t,\x04-\t-\x04.\t.\x04/\t/\x040\t0\x041\t1\x042\t2\x043\t3\x044" +
		"\t4\x045\t5\x046\t6\x047\t7\x048\t8\x049\t9\x04:\t:\x04;\t;\x04<\t<\x04" +
		"=\t=\x04>\t>\x04?\t?\x04@\t@\x04A\tA\x04B\tB\x04C\tC\x04D\tD\x04E\tE\x04" +
		"F\tF\x04G\tG\x04H\tH\x04I\tI\x04J\tJ\x04K\tK\x04L\tL\x04M\tM\x04N\tN\x04" +
		"O\tO\x04P\tP\x04Q\tQ\x04R\tR\x04S\tS\x04T\tT\x04U\tU\x04V\tV\x04W\tW\x04" +
		"X\tX\x04Y\tY\x04Z\tZ\x04[\t[\x03\x02\x03\x02\x07\x02\xB9\n\x02\f\x02\x0E" +
		"\x02\xBC\v\x02\x03\x02\x03\x02\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03" +
		"\x05\x03\xC5\n\x03\x03\x04\x03\x04\x07\x04\xC9\n\x04\f\x04\x0E\x04\xCC" +
		"\v\x04\x03\x04\x03\x04\x03\x05\x03\x05\x03\x05\x03\x05\x05\x05\xD4\n\x05" +
		"\x03\x05\x05\x05\xD7\n\x05\x03\x05\x03\x05\x03\x06\x06\x06\xDC\n\x06\r" +
		"\x06\x0E\x06\xDD\x03\x07\x03\x07\x03\x07\x03\x07\x05\x07\xE4\n\x07\x03" +
		"\b\x03\b\x03\b\x03\t\x03\t\x03\t\x03\t\x03\t\x05\t\xEE\n\t\x03\t\x03\t" +
		"\x03\t\x03\n\x03\n\x05\n\xF5\n\n\x03\n\x03\n\x03\v\x03\v\x03\v\x05\v\xFC" +
		"\n\v\x03\v\x03\v\x03\v\x03\v\x05\v\u0102\n\v\x07\v\u0104\n\v\f\v\x0E\v" +
		"\u0107\v\v\x03\v\x03\v\x03\v\x05\v\u010C\n\v\x03\v\x03\v\x03\v\x03\v\x05" +
		"\v\u0112\n\v\x07\v\u0114\n\v\f\v\x0E\v\u0117\v\v\x03\v\x03\v\x03\v\x03" +
		"\v\x05\v\u011D\n\v\x05\v\u011F\n\v\x05\v\u0121\n\v\x03\v\x03\v\x03\v\x05" +
		"\v\u0126\n\v\x05\v\u0128\n\v\x05\v\u012A\n\v\x03\v\x03\v\x05\v\u012E\n" +
		"\v\x03\v\x03\v\x03\v\x03\v\x05\v\u0134\n\v\x07\v\u0136\n\v\f\v\x0E\v\u0139" +
		"\v\v\x03\v\x03\v\x03\v\x03\v\x05\v\u013F\n\v\x05\v\u0141\n\v\x05\v\u0143" +
		"\n\v\x03\v\x03\v\x03\v\x05\v\u0148\n\v\x05\v\u014A\n\v\x03\f\x03\f\x03" +
		"\f\x05\f\u014F\n\f\x03\r\x03\r\x03\r\x05\r\u0154\n\r\x03\r\x03\r\x03\r" +
		"\x03\r\x05\r\u015A\n\r\x07\r\u015C\n\r\f\r\x0E\r\u015F\v\r\x03\r\x03\r" +
		"\x03\r\x05\r\u0164\n\r\x03\r\x03\r\x03\r\x03\r\x05\r\u016A\n\r\x07\r\u016C" +
		"\n\r\f\r\x0E\r\u016F\v\r\x03\r\x03\r\x03\r\x03\r\x05\r\u0175\n\r\x05\r" +
		"\u0177\n\r\x05\r\u0179\n\r\x03\r\x03\r\x03\r\x05\r\u017E\n\r\x05\r\u0180" +
		"\n\r\x05\r\u0182\n\r\x03\r\x03\r\x05\r\u0186\n\r\x03\r\x03\r\x03\r\x03" +
		"\r\x05\r\u018C\n\r\x07\r\u018E\n\r\f\r\x0E\r\u0191\v\r\x03\r\x03\r\x03" +
		"\r\x03\r\x05\r\u0197\n\r\x05\r\u0199\n\r\x05\r\u019B\n\r\x03\r\x03\r\x03" +
		"\r\x05\r\u01A0\n\r\x05\r\u01A2\n\r\x03\x0E\x03\x0E\x03\x0F\x03\x0F\x05" +
		"\x0F\u01A8\n\x0F\x03\x10\x03\x10\x03\x10\x07\x10\u01AD\n\x10\f\x10\x0E" +
		"\x10\u01B0\v\x10\x03\x10\x05\x10\u01B3\n\x10\x03\x10\x03\x10\x03\x11\x03" +
		"\x11\x03\x11\x03\x11\x03\x11\x03\x11\x03\x11\x03\x11\x05\x11\u01BF\n\x11" +
		"\x03\x12\x03\x12\x03\x12\x03\x12\x03\x12\x05\x12\u01C6\n\x12\x03\x12\x03" +
		"\x12\x03\x12\x05\x12\u01CB\n\x12\x07\x12\u01CD\n\x12\f\x12\x0E\x12\u01D0" +
		"\v\x12\x05\x12\u01D2\n\x12\x03\x13\x03\x13\x03\x13\x03\x13\x05\x13\u01D8" +
		"\n\x13\x03\x14\x03\x14\x05\x14\u01DC\n\x14\x03\x14\x03\x14\x03\x14\x05" +
		"\x14\u01E1\n\x14\x07\x14\u01E3\n\x14\f\x14\x0E\x14\u01E6\v\x14\x03\x14" +
		"\x05\x14\u01E9\n\x14\x03\x15\x03\x15\x03\x16\x03\x16\x03\x16\x03\x17\x03" +
		"\x17\x03\x18\x03\x18\x03\x18\x03\x18\x03\x18\x05\x18\u01F7\n\x18\x03\x19" +
		"\x03\x19\x03\x1A\x03\x1A\x03\x1B\x03\x1B\x05\x1B\u01FF\n\x1B\x03\x1C\x03" +
		"\x1C\x03\x1D\x03\x1D\x03\x1D\x03\x1D\x05\x1D\u0207\n\x1D\x05\x1D\u0209" +
		"\n\x1D\x03\x1E\x03\x1E\x05\x1E\u020D\n\x1E\x03\x1F\x03\x1F\x03\x1F\x03" +
		" \x03 \x07 \u0214\n \f \x0E \u0217\v \x03 \x03 \x06 \u021B\n \r \x0E " +
		"\u021C\x05 \u021F\n \x03 \x03 \x03 \x03 \x03 \x03 \x03 \x05 \u0228\n " +
		"\x03!\x03!\x03!\x05!\u022D\n!\x03\"\x03\"\x03\"\x05\"\u0232\n\"\x03#\x03" +
		"#\x03#\x07#\u0237\n#\f#\x0E#\u023A\v#\x03#\x05#\u023D\n#\x03$\x03$\x03" +
		"$\x07$\u0242\n$\f$\x0E$\u0245\v$\x03%\x03%\x03%\x07%\u024A\n%\f%\x0E%" +
		"\u024D\v%\x03&\x03&\x03&\x03&\x07&\u0253\n&\f&\x0E&\u0256\v&\x03\'\x03" +
		"\'\x03\'\x03\'\x07\'\u025C\n\'\f\'\x0E\'\u025F\v\'\x03(\x03(\x03(\x03" +
		"(\x05(\u0265\n(\x03)\x03)\x03)\x03)\x03)\x03)\x03)\x03)\x03)\x05)\u0270" +
		"\n)\x03*\x03*\x03*\x03*\x05*\u0276\n*\x03+\x03+\x03+\x03+\x03+\x03+\x03" +
		"+\x03+\x03+\x07+\u0281\n+\f+\x0E+\u0284\v+\x03+\x03+\x03+\x05+\u0289\n" +
		"+\x03,\x03,\x03,\x03,\x03,\x03,\x03,\x05,\u0292\n,\x03-\x03-\x03-\x03" +
		"-\x03-\x03-\x03-\x03-\x03-\x05-\u029D\n-\x03.\x03.\x03.\x03.\x03.\x03" +
		".\x03.\x06.\u02A6\n.\r.\x0E.\u02A7\x03.\x03.\x03.\x05.\u02AD\n.\x03.\x03" +
		".\x03.\x05.\u02B2\n.\x03.\x03.\x03.\x05.\u02B7\n.\x03/\x03/\x03/\x03/" +
		"\x07/\u02BD\n/\f/\x0E/\u02C0\v/\x03/\x03/\x03/\x030\x030\x030\x050\u02C8" +
		"\n0\x031\x031\x031\x031\x051\u02CE\n1\x051\u02D0\n1\x032\x032\x032\x03" +
		"2\x062\u02D6\n2\r2\x0E2\u02D7\x032\x032\x052\u02DC\n2\x033\x033\x033\x03" +
		"3\x033\x033\x053\u02E4\n3\x033\x053\u02E7\n3\x034\x034\x054\u02EB\n4\x03" +
		"5\x035\x055\u02EF\n5\x035\x035\x035\x036\x036\x056\u02F6\n6\x036\x036" +
		"\x036\x037\x037\x037\x077\u02FE\n7\f7\x0E7\u0301\v7\x038\x038\x038\x07" +
		"8\u0306\n8\f8\x0E8\u0309\v8\x039\x039\x039\x059\u030E\n9\x03:\x03:\x03" +
		":\x03:\x07:\u0314\n:\f:\x0E:\u0317\v:\x03;\x03;\x03;\x03;\x03;\x03;\x03" +
		";\x03;\x03;\x03;\x03;\x03;\x03;\x05;\u0326\n;\x03<\x03<\x03<\x03=\x03" +
		"=\x03=\x07=\u032E\n=\f=\x0E=\u0331\v=\x03>\x03>\x03>\x07>\u0336\n>\f>" +
		"\x0E>\u0339\v>\x03?\x03?\x03?\x07?\u033E\n?\f?\x0E?\u0341\v?\x03@\x03" +
		"@\x03@\x07@\u0346\n@\f@\x0E@\u0349\v@\x03A\x03A\x03A\x07A\u034E\nA\fA" +
		"\x0EA\u0351\vA\x03B\x03B\x03B\x07B\u0356\nB\fB\x0EB\u0359\vB\x03C\x03" +
		"C\x03C\x05C\u035E\nC\x03D\x03D\x03D\x05D\u0363\nD\x03E\x05E\u0366\nE\x03" +
		"E\x03E\x07E\u036A\nE\fE\x0EE\u036D\vE\x03F\x03F\x03F\x05F\u0372\nF\x03" +
		"F\x03F\x03F\x05F\u0377\nF\x03F\x03F\x03F\x05F\u037C\nF\x03F\x03F\x03F" +
		"\x03F\x06F\u0382\nF\rF\x0EF\u0383\x03F\x06F\u0387\nF\rF\x0EF\u0388\x03" +
		"F\x03F\x03F\x03F\x05F\u038F\nF\x03G\x03G\x05G\u0393\nG\x03G\x03G\x03G" +
		"\x03G\x05G\u0399\nG\x07G\u039B\nG\fG\x0EG\u039E\vG\x03G\x05G\u03A1\nG" +
		"\x05G\u03A3\nG\x03H\x03H\x03H\x03H\x03H\x03H\x03H\x05H\u03AC\nH\x03I\x03" +
		"I\x03I\x07I\u03B1\nI\fI\x0EI\u03B4\vI\x03I\x05I\u03B7\nI\x03J\x03J\x05" +
		"J\u03BB\nJ\x03J\x03J\x05J\u03BF\nJ\x03J\x05J\u03C2\nJ\x05J\u03C4\nJ\x03" +
		"K\x03K\x05K\u03C8\nK\x03L\x03L\x05L\u03CC\nL\x03L\x03L\x03L\x05L\u03D1" +
		"\nL\x07L\u03D3\nL\fL\x0EL\u03D6\vL\x03L\x05L\u03D9\nL\x03M\x03M\x03M\x07" +
		"M\u03DE\nM\fM\x0EM\u03E1\vM\x03M\x05M\u03E4\nM\x03N\x03N\x03N\x03N\x03" +
		"N\x03N\x05N\u03EC\nN\x03N\x03N\x03N\x03N\x03N\x03N\x03N\x03N\x05N\u03F6" +
		"\nN\x07N\u03F8\nN\fN\x0EN\u03FB\vN\x03N\x05N\u03FE\nN\x05N\u0400\nN\x03" +
		"N\x03N\x05N\u0404\nN\x03N\x03N\x03N\x03N\x05N\u040A\nN\x07N\u040C\nN\f" +
		"N\x0EN\u040F\vN\x03N\x05N\u0412\nN\x05N\u0414\nN\x05N\u0416\nN\x03O\x03" +
		"O\x03O\x03O\x05O\u041C\nO\x03O\x05O\u041F\nO\x03O\x03O\x03O\x03P\x03P" +
		"\x05P\u0426\nP\x03P\x03P\x03Q\x03Q\x03Q\x07Q\u042D\nQ\fQ\x0EQ\u0430\v" +
		"Q\x03Q\x05Q\u0433\nQ\x03R\x03R\x05R\u0437\nR\x03R\x03R\x03R\x03R\x03R" +
		"\x03R\x03R\x03R\x05R\u0441\nR\x03S\x03S\x05S\u0445\nS\x03T\x05T\u0448" +
		"\nT\x03T\x03T\x03T\x03T\x03T\x05T\u044F\nT\x03U\x03U\x03U\x05U\u0454\n" +
		"U\x03V\x03V\x03W\x03W\x05W\u045A\nW\x03X\x03X\x03X\x05X\u045F\nX\x03Y" +
		"\x03Y\x07Y\u0463\nY\fY\x0EY\u0466\vY\x03Y\x03Y\x03Y\x07Y\u046B\nY\fY\x0E" +
		"Y\u046E\vY\x03Y\x03Y\x03Y\x07Y\u0473\nY\fY\x0EY\u0476\vY\x03Y\x03Y\x03" +
		"Y\x07Y\u047B\nY\fY\x0EY\u047E\vY\x03Y\x05Y\u0481\nY\x03Z\x03Z\x03Z\x03" +
		"Z\x05Z\u0487\nZ\x03Z\x03Z\x05Z\u048B\nZ\x03[\x03[\x03[\x03[\x05[\u0491" +
		"\n[\x03[\x03[\x05[\u0495\n[\x03[\x02\x02\x02\\\x02\x02\x04\x02\x06\x02" +
		"\b\x02\n\x02\f\x02\x0E\x02\x10\x02\x12\x02\x14\x02\x16\x02\x18\x02\x1A" +
		"\x02\x1C\x02\x1E\x02 \x02\"\x02$\x02&\x02(\x02*\x02,\x02.\x020\x022\x02" +
		"4\x026\x028\x02:\x02<\x02>\x02@\x02B\x02D\x02F\x02H\x02J\x02L\x02N\x02" +
		"P\x02R\x02T\x02V\x02X\x02Z\x02\\\x02^\x02`\x02b\x02d\x02f\x02h\x02j\x02" +
		"l\x02n\x02p\x02r\x02t\x02v\x02x\x02z\x02|\x02~\x02\x80\x02\x82\x02\x84" +
		"\x02\x86\x02\x88\x02\x8A\x02\x8C\x02\x8E\x02\x90\x02\x92\x02\x94\x02\x96" +
		"\x02\x98\x02\x9A\x02\x9C\x02\x9E\x02\xA0\x02\xA2\x02\xA4\x02\xA6\x02\xA8" +
		"\x02\xAA\x02\xAC\x02\xAE\x02\xB0\x02\xB2\x02\xB4\x02\x02\b\x03\x02\\h" +
		"\x03\x029:\x03\x02HI\x03\x02JK\x05\x02;;LNZZ\x04\x02JKOO\x02\u051D\x02" +
		"\xBA\x03\x02\x02\x02\x04\xC4\x03\x02\x02\x02\x06\xC6\x03\x02\x02\x02\b" +
		"\xCF\x03\x02\x02\x02\n\xDB\x03\x02\x02\x02\f\xDF\x03\x02\x02\x02\x0E\xE5" +
		"\x03\x02\x02\x02\x10\xE8\x03\x02\x02\x02\x12\xF2\x03\x02\x02\x02\x14\u0149" +
		"\x03\x02\x02\x02\x16\u014B\x03\x02\x02\x02\x18\u01A1\x03\x02\x02\x02\x1A" +
		"\u01A3\x03\x02\x02\x02\x1C\u01A7\x03\x02\x02\x02\x1E\u01A9\x03\x02\x02" +
		"\x02 \u01BE\x03\x02\x02\x02\"\u01C0\x03\x02\x02\x02$\u01D3\x03\x02\x02" +
		"\x02&\u01DB\x03\x02\x02\x02(\u01EA\x03\x02\x02\x02*\u01EC\x03\x02\x02" +
		"\x02,\u01EF\x03\x02\x02\x02.\u01F6\x03\x02\x02\x020\u01F8\x03\x02\x02" +
		"\x022\u01FA\x03\x02\x02\x024\u01FC\x03\x02\x02\x026\u0200\x03\x02\x02" +
		"\x028\u0202\x03\x02\x02\x02:\u020C\x03\x02\x02\x02<\u020E\x03\x02\x02" +
		"\x02>\u0211\x03\x02\x02\x02@\u0229\x03\x02\x02\x02B\u022E\x03\x02\x02" +
		"\x02D\u0233\x03\x02\x02\x02F\u023E\x03\x02\x02\x02H\u0246\x03\x02\x02" +
		"\x02J\u024E\x03\x02\x02\x02L\u0257\x03\x02\x02\x02N\u0260\x03\x02\x02" +
		"\x02P\u026F\x03\x02\x02\x02R\u0271\x03\x02\x02\x02T\u0277\x03\x02\x02" +
		"\x02V\u028A\x03\x02\x02\x02X\u0293\x03\x02\x02\x02Z\u029E\x03\x02\x02" +
		"\x02\\\u02B8\x03\x02\x02\x02^\u02C4\x03\x02\x02\x02`\u02C9\x03\x02\x02" +
		"\x02b\u02DB\x03\x02\x02\x02d\u02E6\x03\x02\x02\x02f\u02EA\x03\x02\x02" +
		"\x02h\u02EC\x03\x02\x02\x02j\u02F3\x03\x02\x02\x02l\u02FA\x03\x02\x02" +
		"\x02n\u0302\x03\x02\x02\x02p\u030D\x03\x02\x02\x02r\u030F\x03\x02\x02" +
		"\x02t\u0325\x03\x02\x02\x02v\u0327\x03\x02\x02\x02x\u032A\x03\x02\x02" +
		"\x02z\u0332\x03\x02\x02\x02|\u033A\x03\x02\x02\x02~\u0342\x03\x02\x02" +
		"\x02\x80\u034A\x03\x02\x02\x02\x82\u0352\x03\x02\x02\x02\x84\u035D\x03" +
		"\x02\x02\x02\x86\u035F\x03\x02\x02\x02\x88\u0365\x03\x02\x02\x02\x8A\u038E" +
		"\x03\x02\x02\x02\x8C\u0392\x03\x02\x02\x02\x8E\u03AB\x03\x02\x02\x02\x90" +
		"\u03AD\x03\x02\x02\x02\x92\u03C3\x03\x02\x02\x02\x94\u03C5\x03\x02\x02" +
		"\x02\x96\u03CB\x03\x02\x02\x02\x98\u03DA\x03\x02\x02\x02\x9A\u0415\x03" +
		"\x02\x02\x02\x9C\u0417\x03\x02\x02\x02\x9E\u0423\x03\x02\x02\x02\xA0\u0429" +
		"\x03\x02\x02\x02\xA2\u0440\x03\x02\x02\x02\xA4\u0444\x03\x02\x02\x02\xA6" +
		"\u0447\x03\x02\x02\x02\xA8\u0450\x03\x02\x02\x02\xAA\u0455\x03\x02\x02" +
		"\x02\xAC\u0457\x03\x02\x02\x02\xAE\u045E\x03\x02\x02\x02\xB0\u0480\x03" +
		"\x02\x02\x02\xB2\u048A\x03\x02\x02\x02\xB4\u0494\x03\x02\x02\x02\xB6\xB9" +
		"\x07/\x02\x02\xB7\xB9\x05\x1C\x0F\x02\xB8\xB6\x03\x02\x02\x02\xB8\xB7" +
		"\x03\x02\x02\x02\xB9\xBC\x03\x02\x02\x02\xBA\xB8\x03\x02\x02\x02\xBA\xBB" +
		"\x03\x02\x02\x02\xBB\xBD\x03\x02\x02\x02\xBC\xBA\x03\x02\x02\x02\xBD\xBE" +
		"\x07\x02\x02\x03\xBE\x03\x03\x02\x02\x02\xBF\xC5\x07/\x02\x02\xC0\xC5" +
		"\x05\x1E\x10\x02\xC1\xC2\x05P)\x02\xC2\xC3\x07/\x02\x02\xC3\xC5\x03\x02" +
		"\x02\x02\xC4\xBF\x03\x02\x02\x02\xC4\xC0\x03\x02\x02\x02\xC4\xC1\x03\x02" +
		"\x02\x02\xC5\x05\x03\x02\x02\x02\xC6\xCA\x05\x98M\x02\xC7\xC9\x07/\x02" +
		"\x02\xC8\xC7\x03\x02\x02\x02\xC9\xCC\x03\x02\x02\x02\xCA\xC8\x03\x02\x02" +
		"\x02\xCA\xCB\x03\x02\x02\x02\xCB\xCD\x03\x02\x02\x02\xCC\xCA\x03\x02\x02" +
		"\x02\xCD\xCE\x07\x02\x02\x03\xCE\x07\x03\x02\x02\x02\xCF\xD0\x07Z\x02" +
		"\x02\xD0\xD6\x05H%\x02\xD1\xD3\x07<\x02\x02\xD2\xD4\x05\xA0Q\x02\xD3\xD2" +
		"\x03\x02\x02\x02\xD3\xD4\x03\x02\x02\x02\xD4\xD5\x03\x02\x02\x02\xD5\xD7" +
		"\x07=\x02\x02\xD6\xD1\x03\x02\x02\x02\xD6\xD7\x03\x02\x02\x02\xD7\xD8" +
		"\x03\x02\x02\x02\xD8\xD9\x07/\x02\x02\xD9\t\x03\x02\x02\x02\xDA\xDC\x05" +
		"\b\x05\x02\xDB\xDA\x03\x02\x02\x02\xDC\xDD\x03\x02\x02\x02\xDD\xDB\x03" +
		"\x02\x02\x02\xDD\xDE\x03\x02\x02\x02\xDE\v\x03\x02\x02\x02\xDF\xE3\x05" +
		"\n\x06\x02\xE0\xE4\x05\x9CO\x02\xE1\xE4\x05\x10\t\x02\xE2\xE4\x05\x0E" +
		"\b\x02\xE3\xE0\x03\x02\x02\x02\xE3\xE1\x03\x02\x02\x02\xE3\xE2\x03\x02" +
		"\x02\x02\xE4\r\x03\x02\x02\x02\xE5\xE6\x07-\x02\x02\xE6\xE7\x05\x10\t" +
		"\x02\xE7\x0F\x03\x02\x02\x02\xE8\xE9\x07\f\x02\x02\xE9\xEA\x070\x02\x02" +
		"\xEA\xED\x05\x12\n\x02\xEB\xEC\x07[\x02\x02\xEC\xEE\x05d3\x02\xED\xEB" +
		"\x03\x02\x02\x02\xED\xEE\x03\x02\x02\x02\xEE\xEF\x03\x02\x02\x02\xEF\xF0" +
		"\x07?\x02\x02\xF0\xF1\x05b2\x02\xF1\x11\x03\x02\x02\x02\xF2\xF4\x07<\x02" +
		"\x02\xF3\xF5\x05\x14\v\x02\xF4\xF3\x03\x02\x02\x02\xF4\xF5\x03\x02\x02" +
		"\x02\xF5\xF6\x03\x02\x02\x02\xF6\xF7\x07=\x02\x02\xF7\x13\x03\x02\x02" +
		"\x02\xF8\xFB\x05\x16\f\x02\xF9\xFA\x07B\x02\x02\xFA\xFC\x05d3\x02\xFB" +
		"\xF9\x03\x02\x02\x02\xFB\xFC\x03\x02\x02\x02\xFC\u0105\x03\x02\x02\x02" +
		"\xFD\xFE\x07>\x02\x02\xFE\u0101\x05\x16\f\x02\xFF\u0100\x07B\x02\x02\u0100" +
		"\u0102\x05d3\x02\u0101\xFF\x03\x02\x02\x02\u0101\u0102\x03\x02\x02\x02" +
		"\u0102\u0104\x03\x02\x02\x02\u0103\xFD\x03\x02\x02\x02\u0104\u0107\x03" +
		"\x02\x02\x02\u0105\u0103\x03\x02\x02\x02\u0105\u0106\x03\x02\x02\x02\u0106" +
		"\u0129\x03\x02\x02\x02\u0107\u0105\x03\x02\x02\x02\u0108\u0127\x07>\x02" +
		"\x02\u0109\u010B\x07;\x02\x02\u010A\u010C\x05\x16\f\x02\u010B\u010A\x03" +
		"\x02\x02\x02\u010B\u010C\x03\x02\x02\x02\u010C\u0115\x03\x02\x02\x02\u010D" +
		"\u010E\x07>\x02\x02\u010E\u0111\x05\x16\f\x02\u010F\u0110\x07B\x02\x02" +
		"\u0110\u0112\x05d3\x02\u0111\u010F\x03\x02\x02\x02\u0111\u0112\x03\x02" +
		"\x02\x02\u0112\u0114\x03\x02\x02\x02\u0113\u010D\x03\x02\x02\x02\u0114" +
		"\u0117\x03\x02\x02\x02\u0115\u0113\x03\x02\x02\x02\u0115\u0116\x03\x02" +
		"\x02\x02\u0116\u0120\x03\x02\x02\x02\u0117\u0115\x03\x02\x02\x02\u0118" +
		"\u011E\x07>\x02\x02\u0119\u011A\x07A\x02\x02\u011A\u011C\x05\x16\f\x02" +
		"\u011B\u011D\x07>\x02\x02\u011C\u011B\x03\x02\x02\x02\u011C\u011D\x03" +
		"\x02\x02\x02\u011D\u011F\x03\x02\x02\x02\u011E\u0119\x03\x02\x02\x02\u011E" +
		"\u011F\x03\x02\x02\x02\u011F\u0121\x03\x02\x02\x02\u0120\u0118\x03\x02" +
		"\x02\x02\u0120\u0121\x03\x02\x02\x02\u0121\u0128\x03\x02\x02\x02\u0122" +
		"\u0123\x07A\x02\x02\u0123\u0125\x05\x16\f\x02\u0124\u0126\x07>\x02\x02" +
		"\u0125\u0124\x03\x02\x02\x02\u0125\u0126\x03\x02\x02\x02\u0126\u0128\x03" +
		"\x02\x02\x02\u0127\u0109\x03\x02\x02\x02\u0127\u0122\x03\x02\x02\x02\u0127" +
		"\u0128\x03\x02\x02\x02\u0128\u012A\x03\x02\x02\x02\u0129\u0108\x03\x02" +
		"\x02\x02\u0129\u012A\x03\x02\x02\x02\u012A\u014A\x03\x02\x02\x02\u012B" +
		"\u012D\x07;\x02\x02\u012C\u012E\x05\x16\f\x02\u012D\u012C\x03\x02\x02" +
		"\x02\u012D\u012E\x03\x02\x02\x02\u012E\u0137\x03\x02\x02\x02\u012F\u0130" +
		"\x07>\x02\x02\u0130\u0133\x05\x16\f\x02\u0131\u0132\x07B\x02\x02\u0132" +
		"\u0134\x05d3\x02\u0133\u0131\x03\x02\x02\x02\u0133\u0134\x03\x02\x02\x02" +
		"\u0134\u0136\x03\x02\x02\x02\u0135\u012F\x03\x02\x02\x02\u0136\u0139\x03" +
		"\x02\x02\x02\u0137\u0135\x03\x02\x02\x02\u0137\u0138\x03\x02\x02\x02\u0138" +
		"\u0142\x03\x02\x02\x02\u0139\u0137\x03\x02\x02\x02\u013A\u0140\x07>\x02" +
		"\x02\u013B\u013C\x07A\x02\x02\u013C\u013E\x05\x16\f\x02\u013D\u013F\x07" +
		">\x02\x02\u013E\u013D\x03\x02\x02\x02\u013E\u013F\x03\x02\x02\x02\u013F" +
		"\u0141\x03\x02\x02\x02\u0140\u013B\x03\x02\x02\x02\u0140\u0141\x03\x02" +
		"\x02\x02\u0141\u0143\x03\x02\x02\x02\u0142\u013A\x03\x02\x02\x02\u0142" +
		"\u0143\x03\x02\x02\x02\u0143\u014A\x03\x02\x02\x02\u0144\u0145\x07A\x02" +
		"\x02\u0145\u0147\x05\x16\f\x02\u0146\u0148\x07>\x02\x02\u0147\u0146\x03" +
		"\x02\x02\x02\u0147\u0148\x03\x02\x02\x02\u0148\u014A\x03\x02\x02\x02\u0149" +
		"\xF8\x03\x02\x02\x02\u0149\u012B\x03\x02\x02\x02\u0149\u0144\x03\x02\x02" +
		"\x02\u014A\x15\x03\x02\x02\x02\u014B\u014E\x070\x02\x02\u014C\u014D\x07" +
		"?\x02\x02\u014D\u014F\x05d3\x02\u014E\u014C\x03\x02\x02\x02\u014E\u014F" +
		"\x03\x02\x02\x02\u014F\x17\x03\x02\x02\x02\u0150\u0153\x05\x1A\x0E\x02" +
		"\u0151\u0152\x07B\x02\x02\u0152\u0154\x05d3\x02\u0153\u0151\x03\x02\x02" +
		"\x02\u0153\u0154\x03\x02\x02\x02\u0154\u015D\x03\x02\x02\x02\u0155\u0156" +
		"\x07>\x02\x02\u0156\u0159\x05\x1A\x0E\x02\u0157\u0158\x07B\x02\x02\u0158" +
		"\u015A\x05d3\x02\u0159\u0157\x03\x02\x02\x02\u0159\u015A\x03\x02\x02\x02" +
		"\u015A\u015C\x03\x02\x02\x02\u015B\u0155\x03\x02\x02\x02\u015C\u015F\x03" +
		"\x02\x02\x02\u015D\u015B\x03\x02\x02\x02\u015D\u015E\x03\x02\x02\x02\u015E" +
		"\u0181\x03\x02\x02\x02\u015F\u015D\x03\x02\x02\x02\u0160\u017F\x07>\x02" +
		"\x02\u0161\u0163\x07;\x02\x02\u0162\u0164\x05\x1A\x0E\x02\u0163\u0162" +
		"\x03\x02\x02\x02\u0163\u0164\x03\x02\x02\x02\u0164\u016D\x03\x02\x02\x02" +
		"\u0165\u0166\x07>\x02\x02\u0166\u0169\x05\x1A\x0E\x02\u0167\u0168\x07" +
		"B\x02\x02\u0168\u016A\x05d3\x02\u0169\u0167\x03\x02\x02\x02\u0169\u016A" +
		"\x03\x02\x02\x02\u016A\u016C\x03\x02\x02\x02\u016B\u0165\x03\x02\x02\x02" +
		"\u016C\u016F\x03\x02\x02\x02\u016D\u016B\x03\x02\x02\x02\u016D\u016E\x03" +
		"\x02\x02\x02\u016E\u0178\x03\x02\x02\x02\u016F\u016D\x03\x02\x02\x02\u0170" +
		"\u0176\x07>\x02\x02\u0171\u0172\x07A\x02\x02\u0172\u0174\x05\x1A\x0E\x02" +
		"\u0173\u0175\x07>\x02\x02\u0174\u0173\x03\x02\x02\x02\u0174\u0175\x03" +
		"\x02\x02\x02\u0175\u0177\x03\x02\x02\x02\u0176\u0171\x03\x02\x02\x02\u0176" +
		"\u0177\x03\x02\x02\x02\u0177\u0179\x03\x02\x02\x02\u0178\u0170\x03\x02" +
		"\x02\x02\u0178\u0179\x03\x02\x02\x02\u0179\u0180\x03\x02\x02\x02\u017A" +
		"\u017B\x07A\x02\x02\u017B\u017D\x05\x1A\x0E\x02\u017C\u017E\x07>\x02\x02" +
		"\u017D\u017C\x03\x02\x02\x02\u017D\u017E\x03\x02\x02\x02\u017E\u0180\x03" +
		"\x02\x02\x02\u017F\u0161\x03\x02\x02\x02\u017F\u017A\x03\x02\x02\x02\u017F" +
		"\u0180\x03\x02\x02\x02\u0180\u0182\x03\x02\x02\x02\u0181\u0160\x03\x02" +
		"\x02\x02\u0181\u0182\x03\x02\x02\x02\u0182\u01A2\x03\x02\x02\x02\u0183" +
		"\u0185\x07;\x02\x02\u0184\u0186\x05\x1A\x0E\x02\u0185\u0184\x03\x02\x02" +
		"\x02\u0185\u0186\x03\x02\x02\x02\u0186\u018F\x03\x02\x02\x02\u0187\u0188" +
		"\x07>\x02\x02\u0188\u018B\x05\x1A\x0E\x02\u0189\u018A\x07B\x02\x02\u018A" +
		"\u018C\x05d3\x02\u018B\u0189\x03\x02\x02\x02\u018B\u018C\x03\x02\x02\x02" +
		"\u018C\u018E\x03\x02\x02\x02\u018D\u0187\x03\x02\x02\x02\u018E\u0191\x03" +
		"\x02\x02\x02\u018F\u018D\x03\x02\x02\x02\u018F\u0190\x03\x02\x02\x02\u0190" +
		"\u019A\x03\x02\x02\x02\u0191\u018F\x03\x02\x02\x02\u0192\u0198\x07>\x02" +
		"\x02\u0193\u0194\x07A\x02\x02\u0194\u0196\x05\x1A\x0E\x02\u0195\u0197" +
		"\x07>\x02\x02\u0196\u0195\x03\x02\x02\x02\u0196\u0197\x03\x02\x02\x02" +
		"\u0197";
	private static readonly _serializedATNSegment1: string =
		"\u0199\x03\x02\x02\x02\u0198\u0193\x03\x02\x02\x02\u0198\u0199\x03\x02" +
		"\x02\x02\u0199\u019B\x03\x02\x02\x02\u019A\u0192\x03\x02\x02\x02\u019A" +
		"\u019B\x03\x02\x02\x02\u019B\u01A2\x03\x02\x02\x02\u019C\u019D\x07A\x02" +
		"\x02\u019D\u019F\x05\x1A\x0E\x02\u019E\u01A0\x07>\x02\x02\u019F\u019E" +
		"\x03\x02\x02\x02\u019F\u01A0\x03\x02\x02\x02\u01A0\u01A2\x03\x02\x02\x02" +
		"\u01A1\u0150\x03\x02\x02\x02\u01A1\u0183\x03\x02\x02\x02\u01A1\u019C\x03" +
		"\x02\x02\x02\u01A2\x19\x03\x02\x02\x02\u01A3\u01A4\x070\x02\x02\u01A4" +
		"\x1B\x03\x02\x02\x02\u01A5\u01A8\x05\x1E\x10\x02\u01A6\u01A8\x05P)\x02" +
		"\u01A7\u01A5\x03\x02\x02\x02\u01A7\u01A6\x03\x02\x02\x02\u01A8\x1D\x03" +
		"\x02\x02\x02\u01A9\u01AE\x05 \x11\x02\u01AA\u01AB\x07@\x02\x02\u01AB\u01AD" +
		"\x05 \x11\x02\u01AC\u01AA\x03\x02\x02\x02\u01AD\u01B0\x03\x02\x02\x02" +
		"\u01AE\u01AC\x03\x02\x02\x02\u01AE\u01AF\x03\x02\x02\x02\u01AF\u01B2\x03" +
		"\x02\x02\x02\u01B0\u01AE\x03\x02\x02\x02\u01B1\u01B3\x07@\x02\x02\u01B2" +
		"\u01B1\x03\x02\x02\x02\u01B2\u01B3\x03\x02\x02\x02\u01B3\u01B4\x03\x02" +
		"\x02\x02\u01B4\u01B5\x07/\x02\x02\u01B5\x1F\x03\x02\x02\x02\u01B6\u01BF" +
		"\x05\"\x12\x02\u01B7\u01BF\x05*\x16\x02\u01B8\u01BF\x05,\x17\x02\u01B9" +
		"\u01BF\x05.\x18\x02\u01BA\u01BF\x05:\x1E\x02\u01BB\u01BF\x05J&\x02\u01BC" +
		"\u01BF\x05L\'\x02\u01BD\u01BF\x05N(\x02\u01BE\u01B6\x03\x02\x02\x02\u01BE" +
		"\u01B7\x03\x02\x02\x02\u01BE\u01B8\x03\x02\x02\x02\u01BE\u01B9\x03\x02" +
		"\x02\x02\u01BE\u01BA\x03\x02\x02\x02\u01BE\u01BB\x03\x02\x02\x02\u01BE" +
		"\u01BC\x03\x02\x02\x02\u01BE\u01BD\x03\x02\x02\x02\u01BF!\x03\x02\x02" +
		"\x02\u01C0\u01D1\x05&\x14\x02\u01C1\u01D2\x05$\x13\x02\u01C2\u01C5\x05" +
		"(\x15\x02\u01C3\u01C6\x05\xACW\x02\u01C4\u01C6\x05\x98M\x02\u01C5\u01C3" +
		"\x03\x02\x02\x02\u01C5\u01C4\x03\x02\x02\x02\u01C6\u01D2\x03\x02\x02\x02" +
		"\u01C7\u01CA\x07B\x02\x02\u01C8\u01CB\x05\xACW\x02\u01C9\u01CB\x05&\x14" +
		"\x02\u01CA\u01C8\x03\x02\x02\x02\u01CA\u01C9\x03\x02\x02\x02\u01CB\u01CD" +
		"\x03\x02\x02\x02\u01CC\u01C7\x03\x02\x02\x02\u01CD\u01D0\x03\x02\x02\x02" +
		"\u01CE\u01CC\x03\x02\x02\x02\u01CE\u01CF\x03\x02\x02\x02\u01CF\u01D2\x03" +
		"\x02\x02\x02\u01D0\u01CE\x03\x02\x02\x02\u01D1\u01C1\x03\x02\x02\x02\u01D1" +
		"\u01C2\x03\x02\x02\x02\u01D1\u01CE\x03\x02\x02\x02\u01D2#\x03\x02\x02" +
		"\x02\u01D3\u01D4\x07?\x02\x02\u01D4\u01D7\x05d3\x02\u01D5\u01D6\x07B\x02" +
		"\x02\u01D6\u01D8\x05d3\x02\u01D7\u01D5\x03\x02\x02\x02\u01D7\u01D8\x03" +
		"\x02\x02\x02\u01D8%\x03\x02\x02\x02\u01D9\u01DC\x05d3\x02\u01DA\u01DC" +
		"\x05v<\x02\u01DB\u01D9\x03\x02\x02\x02\u01DB\u01DA\x03\x02\x02\x02\u01DC" +
		"\u01E4\x03\x02\x02\x02\u01DD\u01E0\x07>\x02\x02\u01DE\u01E1\x05d3\x02" +
		"\u01DF\u01E1\x05v<\x02\u01E0\u01DE\x03\x02\x02\x02\u01E0\u01DF\x03\x02" +
		"\x02\x02\u01E1\u01E3\x03\x02\x02\x02\u01E2\u01DD\x03\x02\x02\x02\u01E3" +
		"\u01E6\x03\x02\x02\x02\u01E4\u01E2\x03\x02\x02\x02\u01E4\u01E5\x03\x02" +
		"\x02\x02\u01E5\u01E8\x03\x02\x02\x02\u01E6\u01E4\x03\x02\x02\x02\u01E7" +
		"\u01E9\x07>\x02\x02\u01E8\u01E7\x03\x02\x02\x02\u01E8\u01E9\x03\x02\x02" +
		"\x02\u01E9\'\x03\x02\x02\x02\u01EA\u01EB\t\x02\x02\x02\u01EB)\x03\x02" +
		"\x02\x02\u01EC\u01ED\x07)\x02\x02\u01ED\u01EE\x05\x96L\x02\u01EE+\x03" +
		"\x02\x02\x02\u01EF\u01F0\x07*\x02\x02\u01F0-\x03\x02\x02\x02\u01F1\u01F7" +
		"\x050\x19\x02\u01F2\u01F7\x052\x1A\x02\u01F3\u01F7\x054\x1B\x02\u01F4" +
		"\u01F7\x058\x1D\x02\u01F5\u01F7\x056\x1C\x02\u01F6\u01F1\x03\x02\x02\x02" +
		"\u01F6\u01F2\x03\x02\x02\x02\u01F6\u01F3\x03\x02\x02\x02\u01F6\u01F4\x03" +
		"\x02\x02\x02\u01F6\u01F5\x03\x02\x02\x02\u01F7/\x03\x02\x02\x02\u01F8" +
		"\u01F9\x07,\x02\x02\u01F91\x03\x02\x02\x02\u01FA\u01FB\x07+\x02\x02\u01FB" +
		"3\x03\x02\x02\x02\u01FC\u01FE\x07\r\x02\x02\u01FD\u01FF\x05\x98M\x02\u01FE" +
		"\u01FD\x03\x02\x02\x02\u01FE\u01FF\x03\x02\x02\x02\u01FF5\x03\x02\x02" +
		"\x02\u0200\u0201\x05\xACW\x02\u02017\x03\x02\x02\x02\u0202\u0208\x07\x0E" +
		"\x02\x02\u0203\u0206\x05d3\x02\u0204\u0205\x07\x0F\x02\x02\u0205\u0207" +
		"\x05d3\x02\u0206\u0204\x03\x02\x02\x02\u0206\u0207\x03\x02\x02\x02\u0207" +
		"\u0209\x03\x02\x02\x02\u0208\u0203\x03\x02\x02\x02\u0208\u0209\x03\x02" +
		"\x02\x02\u02099\x03\x02\x02\x02\u020A\u020D\x05<\x1F\x02\u020B\u020D\x05" +
		"> \x02\u020C\u020A\x03\x02\x02\x02\u020C\u020B\x03\x02\x02\x02\u020D;" +
		"\x03\x02\x02\x02\u020E\u020F\x07\x10\x02\x02\u020F\u0210\x05F$\x02\u0210" +
		"=\x03\x02\x02\x02\u0211\u021E\x07\x0F\x02\x02\u0212\u0214\t\x03\x02\x02" +
		"\u0213\u0212\x03\x02\x02\x02\u0214\u0217\x03\x02\x02\x02\u0215\u0213\x03" +
		"\x02\x02\x02\u0215\u0216\x03\x02\x02\x02\u0216\u0218\x03\x02\x02\x02\u0217" +
		"\u0215\x03\x02\x02\x02\u0218\u021F\x05H%\x02\u0219\u021B\t\x03\x02\x02" +
		"\u021A\u0219\x03\x02\x02\x02\u021B\u021C\x03\x02\x02\x02\u021C\u021A\x03" +
		"\x02\x02\x02\u021C\u021D\x03\x02\x02\x02\u021D\u021F\x03\x02\x02\x02\u021E" +
		"\u0215\x03\x02\x02\x02\u021E\u021A\x03\x02\x02\x02\u021F\u0220\x03\x02" +
		"\x02\x02\u0220\u0227\x07\x10\x02\x02\u0221\u0228\x07;\x02\x02\u0222\u0223" +
		"\x07<\x02\x02\u0223\u0224\x05D#\x02\u0224\u0225\x07=\x02\x02\u0225\u0228" +
		"\x03\x02\x02\x02\u0226\u0228\x05D#\x02\u0227\u0221\x03\x02\x02\x02\u0227" +
		"\u0222\x03\x02\x02\x02\u0227\u0226\x03\x02\x02\x02\u0228?\x03\x02\x02" +
		"\x02\u0229\u022C\x070\x02\x02\u022A\u022B\x07\x11\x02\x02\u022B\u022D" +
		"\x070\x02\x02\u022C\u022A\x03\x02\x02\x02\u022C\u022D\x03\x02\x02\x02" +
		"\u022DA\x03\x02\x02\x02\u022E\u0231\x05H%\x02\u022F\u0230\x07\x11\x02" +
		"\x02\u0230\u0232\x070\x02\x02\u0231\u022F\x03\x02\x02\x02\u0231\u0232" +
		"\x03\x02\x02\x02\u0232C\x03\x02\x02\x02\u0233\u0238\x05@!\x02\u0234\u0235" +
		"\x07>\x02\x02\u0235\u0237\x05@!\x02\u0236\u0234\x03\x02\x02\x02\u0237" +
		"\u023A\x03\x02\x02\x02\u0238\u0236\x03\x02\x02\x02\u0238\u0239\x03\x02" +
		"\x02\x02\u0239\u023C\x03\x02\x02\x02\u023A\u0238\x03\x02\x02\x02\u023B" +
		"\u023D\x07>\x02\x02\u023C\u023B\x03\x02\x02\x02\u023C\u023D\x03\x02\x02" +
		"\x02\u023DE\x03\x02\x02\x02\u023E\u0243\x05B\"\x02\u023F\u0240\x07>\x02" +
		"\x02\u0240\u0242\x05B\"\x02\u0241\u023F\x03\x02\x02\x02\u0242\u0245\x03" +
		"\x02\x02\x02\u0243\u0241\x03\x02\x02\x02\u0243\u0244\x03\x02\x02\x02\u0244" +
		"G\x03\x02\x02\x02\u0245\u0243\x03\x02\x02\x02\u0246\u024B\x070\x02\x02" +
		"\u0247\u0248\x079\x02\x02\u0248\u024A\x070\x02\x02\u0249\u0247\x03\x02" +
		"\x02\x02\u024A\u024D\x03\x02\x02\x02\u024B\u0249\x03\x02\x02\x02\u024B" +
		"\u024C\x03\x02\x02\x02\u024CI\x03\x02\x02\x02\u024D\u024B\x03\x02\x02" +
		"\x02\u024E\u024F\x07\x12\x02\x02\u024F\u0254\x070\x02\x02\u0250\u0251" +
		"\x07>\x02\x02\u0251\u0253\x070\x02\x02\u0252\u0250\x03\x02\x02\x02\u0253" +
		"\u0256\x03\x02\x02\x02\u0254\u0252\x03\x02\x02\x02\u0254\u0255\x03\x02" +
		"\x02\x02\u0255K\x03\x02\x02\x02\u0256\u0254\x03\x02\x02\x02\u0257\u0258" +
		"\x07\x13\x02\x02\u0258\u025D\x070\x02\x02\u0259\u025A\x07>\x02\x02\u025A" +
		"\u025C\x070\x02\x02\u025B\u0259\x03\x02\x02\x02\u025C\u025F\x03\x02\x02" +
		"\x02\u025D\u025B\x03\x02\x02\x02\u025D\u025E\x03\x02\x02\x02\u025EM\x03" +
		"\x02\x02\x02\u025F\u025D\x03\x02\x02\x02\u0260\u0261\x07\x14\x02\x02\u0261" +
		"\u0264\x05d3\x02\u0262\u0263\x07>\x02\x02\u0263\u0265\x05d3\x02\u0264" +
		"\u0262\x03\x02\x02\x02\u0264\u0265\x03\x02\x02\x02\u0265O\x03\x02\x02" +
		"\x02\u0266\u0270\x05T+\x02\u0267\u0270\x05V,\x02\u0268\u0270\x05X-\x02" +
		"\u0269\u0270\x05Z.\x02\u026A\u0270\x05\\/\x02\u026B\u0270\x05\x10\t\x02" +
		"\u026C\u0270\x05\x9CO\x02\u026D\u0270\x05\f\x07\x02\u026E\u0270\x05R*" +
		"\x02\u026F\u0266\x03\x02\x02\x02\u026F\u0267\x03\x02\x02\x02\u026F\u0268" +
		"\x03\x02\x02\x02\u026F\u0269\x03\x02\x02\x02\u026F\u026A\x03\x02\x02\x02" +
		"\u026F\u026B\x03\x02\x02\x02\u026F\u026C\x03\x02\x02\x02\u026F\u026D\x03" +
		"\x02\x02\x02\u026F\u026E\x03\x02\x02\x02\u0270Q\x03\x02\x02\x02\u0271" +
		"\u0275\x07-\x02\x02\u0272\u0276\x05\x10\t\x02\u0273\u0276\x05\\/\x02\u0274" +
		"\u0276\x05X-\x02\u0275\u0272\x03\x02\x02\x02\u0275\u0273\x03\x02\x02\x02" +
		"\u0275\u0274\x03\x02\x02\x02\u0276S\x03\x02\x02\x02\u0277\u0278\x07\x15" +
		"\x02\x02\u0278\u0279\x05d3\x02\u0279\u027A\x07?\x02\x02\u027A\u0282\x05" +
		"b2\x02\u027B\u027C\x07\x16\x02\x02\u027C\u027D\x05d3\x02\u027D\u027E\x07" +
		"?\x02\x02\u027E\u027F\x05b2\x02\u027F\u0281\x03\x02\x02\x02\u0280\u027B" +
		"\x03\x02\x02\x02\u0281\u0284\x03\x02\x02\x02\u0282\u0280\x03\x02\x02\x02" +
		"\u0282\u0283\x03\x02\x02\x02\u0283\u0288\x03\x02\x02\x02\u0284\u0282\x03" +
		"\x02\x02\x02\u0285\u0286\x07\x17\x02\x02\u0286\u0287\x07?\x02\x02\u0287" +
		"\u0289\x05b2\x02\u0288\u0285\x03\x02\x02\x02\u0288\u0289\x03\x02\x02\x02" +
		"\u0289U\x03\x02\x02\x02\u028A\u028B\x07\x18\x02\x02\u028B\u028C\x05d3" +
		"\x02\u028C\u028D\x07?\x02\x02\u028D\u0291\x05b2\x02\u028E\u028F\x07\x17" +
		"\x02\x02\u028F\u0290\x07?\x02\x02\u0290\u0292\x05b2\x02\u0291\u028E\x03" +
		"\x02\x02\x02\u0291\u0292\x03\x02\x02\x02\u0292W\x03\x02\x02\x02\u0293" +
		"\u0294\x07\x19\x02\x02\u0294\u0295\x05\x96L\x02\u0295\u0296\x07\x1A\x02" +
		"\x02\u0296\u0297\x05\x98M\x02\u0297\u0298\x07?\x02\x02\u0298\u029C\x05" +
		"b2\x02\u0299\u029A\x07\x17\x02\x02\u029A\u029B\x07?\x02\x02\u029B\u029D" +
		"\x05b2\x02\u029C\u0299\x03\x02\x02\x02\u029C\u029D\x03\x02\x02\x02\u029D" +
		"Y\x03\x02\x02\x02\u029E\u029F\x07\x1B\x02\x02\u029F\u02A0\x07?\x02\x02" +
		"\u02A0\u02B6\x05b2\x02\u02A1\u02A2\x05`1\x02\u02A2\u02A3\x07?\x02\x02" +
		"\u02A3\u02A4\x05b2\x02\u02A4\u02A6\x03\x02\x02\x02\u02A5\u02A1\x03\x02" +
		"\x02\x02\u02A6\u02A7\x03\x02\x02\x02\u02A7\u02A5\x03\x02\x02\x02\u02A7" +
		"\u02A8\x03\x02\x02\x02\u02A8\u02AC\x03\x02\x02\x02\u02A9\u02AA\x07\x17" +
		"\x02\x02\u02AA\u02AB\x07?\x02\x02\u02AB\u02AD\x05b2\x02\u02AC\u02A9\x03" +
		"\x02\x02\x02\u02AC\u02AD\x03\x02\x02\x02\u02AD\u02B1\x03\x02\x02\x02\u02AE" +
		"\u02AF\x07\x1C\x02\x02\u02AF\u02B0\x07?\x02\x02\u02B0\u02B2\x05b2\x02" +
		"\u02B1\u02AE\x03\x02\x02\x02\u02B1\u02B2\x03\x02\x02\x02\u02B2\u02B7\x03" +
		"\x02\x02\x02\u02B3\u02B4\x07\x1C\x02\x02\u02B4\u02B5\x07?\x02\x02\u02B5" +
		"\u02B7\x05b2\x02\u02B6\u02A5\x03\x02\x02\x02\u02B6\u02B3\x03\x02\x02\x02" +
		"\u02B7[\x03\x02\x02\x02\u02B8\u02B9\x07\x1D\x02\x02\u02B9\u02BE\x05^0" +
		"\x02\u02BA\u02BB\x07>\x02\x02\u02BB\u02BD\x05^0\x02\u02BC\u02BA\x03\x02" +
		"\x02\x02\u02BD\u02C0\x03\x02\x02\x02\u02BE\u02BC\x03\x02\x02\x02\u02BE" +
		"\u02BF\x03\x02\x02\x02\u02BF\u02C1\x03\x02\x02\x02\u02C0\u02BE\x03\x02" +
		"\x02\x02\u02C1\u02C2\x07?\x02\x02\u02C2\u02C3\x05b2\x02\u02C3]\x03\x02" +
		"\x02\x02\u02C4\u02C7\x05d3\x02\u02C5\u02C6\x07\x11\x02\x02\u02C6\u02C8" +
		"\x05x=\x02\u02C7\u02C5\x03\x02\x02\x02\u02C7\u02C8\x03\x02\x02\x02\u02C8" +
		"_\x03\x02\x02\x02\u02C9\u02CF\x07\x1E\x02\x02\u02CA\u02CD\x05d3\x02\u02CB" +
		"\u02CC\x07\x11\x02\x02\u02CC\u02CE\x070\x02\x02\u02CD\u02CB\x03\x02\x02" +
		"\x02\u02CD\u02CE\x03\x02\x02\x02\u02CE\u02D0\x03\x02\x02\x02\u02CF\u02CA" +
		"\x03\x02\x02\x02\u02CF\u02D0\x03\x02\x02\x02\u02D0a\x03\x02\x02\x02\u02D1" +
		"\u02DC\x05\x1E\x10\x02\u02D2\u02D3\x07/\x02\x02\u02D3\u02D5\x07\x03\x02" +
		"\x02\u02D4\u02D6\x05\x1C\x0F\x02\u02D5\u02D4\x03\x02\x02\x02\u02D6\u02D7" +
		"\x03\x02\x02\x02\u02D7\u02D5\x03\x02\x02\x02\u02D7\u02D8\x03\x02\x02\x02" +
		"\u02D8\u02D9\x03\x02\x02\x02\u02D9\u02DA\x07\x04\x02\x02\u02DA\u02DC\x03" +
		"\x02\x02\x02\u02DB\u02D1\x03\x02\x02\x02\u02DB\u02D2\x03\x02\x02\x02\u02DC" +
		"c\x03\x02\x02\x02\u02DD\u02E3\x05l7\x02\u02DE\u02DF\x07\x15\x02\x02\u02DF" +
		"\u02E0\x05l7\x02\u02E0\u02E1\x07\x17\x02\x02\u02E1\u02E2\x05d3\x02\u02E2" +
		"\u02E4\x03\x02\x02\x02\u02E3\u02DE\x03\x02\x02\x02\u02E3\u02E4\x03\x02" +
		"\x02\x02\u02E4\u02E7\x03\x02\x02\x02\u02E5\u02E7\x05h5\x02\u02E6\u02DD" +
		"\x03\x02\x02\x02\u02E6\u02E5\x03\x02\x02\x02\u02E7e\x03\x02\x02\x02\u02E8" +
		"\u02EB\x05l7\x02\u02E9\u02EB\x05j6\x02\u02EA\u02E8\x03\x02\x02\x02\u02EA" +
		"\u02E9\x03\x02\x02\x02\u02EBg\x03\x02\x02\x02\u02EC\u02EE\x07\x1F\x02" +
		"\x02\u02ED\u02EF\x05\x18\r\x02\u02EE\u02ED\x03\x02\x02\x02\u02EE\u02EF" +
		"\x03\x02\x02\x02\u02EF\u02F0\x03\x02\x02\x02\u02F0\u02F1\x07?\x02\x02" +
		"\u02F1\u02F2\x05d3\x02\u02F2i\x03\x02\x02\x02\u02F3\u02F5\x07\x1F\x02" +
		"\x02\u02F4\u02F6\x05\x18\r\x02\u02F5\u02F4\x03\x02\x02\x02\u02F5\u02F6" +
		"\x03\x02\x02\x02\u02F6\u02F7\x03\x02\x02\x02\u02F7\u02F8\x07?\x02\x02" +
		"\u02F8\u02F9\x05f4\x02\u02F9k\x03\x02\x02\x02\u02FA\u02FF\x05n8\x02\u02FB" +
		"\u02FC\x07 \x02\x02\u02FC\u02FE\x05n8\x02\u02FD\u02FB\x03\x02\x02\x02" +
		"\u02FE\u0301\x03\x02\x02\x02\u02FF\u02FD\x03\x02\x02\x02\u02FF\u0300\x03" +
		"\x02\x02\x02\u0300m\x03\x02\x02\x02\u0301\u02FF\x03\x02\x02\x02\u0302" +
		"\u0307\x05p9\x02\u0303\u0304\x07!\x02\x02\u0304\u0306\x05p9\x02\u0305" +
		"\u0303\x03\x02\x02\x02\u0306\u0309\x03\x02\x02\x02\u0307\u0305\x03\x02" +
		"\x02\x02\u0307\u0308\x03\x02\x02\x02\u0308o\x03\x02\x02\x02\u0309\u0307" +
		"\x03\x02\x02\x02\u030A\u030B\x07\"\x02\x02\u030B\u030E\x05p9\x02\u030C" +
		"\u030E\x05r:\x02\u030D\u030A\x03\x02\x02\x02\u030D\u030C\x03\x02\x02\x02" +
		"\u030Eq\x03\x02\x02\x02\u030F\u0315\x05x=\x02\u0310\u0311\x05t;\x02\u0311" +
		"\u0312\x05x=\x02\u0312\u0314\x03\x02\x02\x02\u0313\u0310\x03\x02\x02\x02" +
		"\u0314\u0317\x03\x02\x02\x02\u0315\u0313\x03\x02\x02\x02\u0315\u0316\x03" +
		"\x02\x02\x02\u0316s\x03\x02\x02\x02\u0317\u0315\x03\x02\x02\x02\u0318" +
		"\u0326\x07S\x02\x02\u0319\u0326\x07T\x02\x02\u031A\u0326\x07U\x02\x02" +
		"\u031B\u0326\x07V\x02\x02\u031C\u0326\x07W\x02\x02\u031D\u0326\x07X\x02" +
		"\x02\u031E\u0326\x07Y\x02\x02\u031F\u0326\x07\x1A\x02\x02\u0320\u0321" +
		"\x07\"\x02\x02\u0321\u0326\x07\x1A\x02\x02\u0322\u0326\x07#\x02\x02\u0323" +
		"\u0324\x07#\x02\x02\u0324\u0326\x07\"\x02\x02\u0325\u0318\x03\x02\x02" +
		"\x02\u0325\u0319\x03\x02\x02\x02\u0325\u031A\x03\x02\x02\x02\u0325\u031B" +
		"\x03\x02\x02\x02\u0325\u031C\x03\x02\x02\x02\u0325\u031D\x03\x02\x02\x02" +
		"\u0325\u031E\x03\x02\x02\x02\u0325\u031F\x03\x02\x02\x02\u0325\u0320\x03" +
		"\x02\x02\x02\u0325\u0322\x03\x02\x02\x02\u0325\u0323\x03\x02\x02\x02\u0326" +
		"u\x03\x02\x02\x02\u0327\u0328\x07;\x02\x02\u0328\u0329\x05x=\x02\u0329" +
		"w\x03\x02\x02\x02\u032A\u032F\x05z>\x02\u032B\u032C\x07E\x02\x02\u032C" +
		"\u032E\x05z>\x02\u032D\u032B\x03\x02\x02\x02\u032E\u0331\x03\x02\x02\x02" +
		"\u032F\u032D\x03\x02\x02\x02\u032F\u0330\x03\x02\x02\x02\u0330y\x03\x02" +
		"\x02\x02\u0331\u032F\x03\x02\x02\x02\u0332\u0337\x05|?\x02\u0333\u0334" +
		"\x07F\x02\x02\u0334\u0336\x05|?\x02\u0335\u0333\x03\x02\x02\x02\u0336" +
		"\u0339\x03\x02\x02\x02\u0337\u0335\x03\x02\x02\x02\u0337\u0338\x03\x02" +
		"\x02\x02\u0338{\x03\x02\x02\x02\u0339\u0337\x03\x02\x02\x02\u033A\u033F" +
		"\x05~@\x02\u033B\u033C\x07G\x02\x02\u033C\u033E\x05~@\x02\u033D\u033B" +
		"\x03\x02\x02\x02\u033E\u0341\x03\x02\x02\x02\u033F\u033D\x03\x02\x02\x02" +
		"\u033F\u0340\x03\x02\x02\x02\u0340}\x03\x02\x02\x02\u0341\u033F\x03\x02" +
		"\x02\x02\u0342\u0347\x05\x80A\x02\u0343\u0344\t\x04\x02\x02\u0344\u0346" +
		"\x05\x80A\x02\u0345\u0343\x03\x02\x02\x02\u0346\u0349\x03\x02\x02\x02" +
		"\u0347\u0345\x03\x02\x02\x02\u0347\u0348\x03\x02\x02\x02\u0348\x7F\x03" +
		"\x02\x02\x02\u0349\u0347\x03\x02\x02\x02\u034A\u034F\x05\x82B\x02\u034B" +
		"\u034C\t\x05\x02\x02\u034C\u034E\x05\x82B\x02\u034D\u034B\x03\x02\x02" +
		"\x02\u034E\u0351\x03\x02\x02\x02\u034F\u034D\x03\x02\x02\x02\u034F\u0350" +
		"\x03\x02\x02\x02\u0350\x81\x03\x02\x02\x02\u0351\u034F\x03\x02\x02\x02" +
		"\u0352\u0357\x05\x84C\x02\u0353\u0354\t\x06\x02\x02\u0354\u0356\x05\x84" +
		"C\x02\u0355\u0353\x03\x02\x02\x02\u0356\u0359\x03\x02\x02\x02\u0357\u0355" +
		"\x03\x02\x02\x02\u0357\u0358\x03\x02\x02\x02\u0358\x83\x03\x02\x02\x02" +
		"\u0359\u0357\x03\x02\x02\x02\u035A\u035B\t\x07\x02\x02\u035B\u035E\x05" +
		"\x84C\x02\u035C\u035E\x05\x86D\x02\u035D\u035A\x03\x02\x02\x02\u035D\u035C" +
		"\x03\x02\x02\x02\u035E\x85\x03\x02\x02\x02\u035F\u0362\x05\x88E\x02\u0360" +
		"\u0361\x07A\x02\x02\u0361\u0363\x05\x84C\x02\u0362\u0360\x03\x02\x02\x02" +
		"\u0362\u0363\x03\x02\x02\x02\u0363\x87\x03\x02\x02\x02\u0364\u0366\x07" +
		".\x02\x02\u0365\u0364\x03\x02\x02\x02\u0365\u0366\x03\x02\x02\x02\u0366" +
		"\u0367\x03\x02\x02\x02\u0367\u036B\x05\x8AF\x02\u0368\u036A\x05\x8EH\x02" +
		"\u0369\u0368\x03\x02\x02\x02\u036A\u036D\x03\x02\x02\x02\u036B\u0369\x03" +
		"\x02\x02\x02\u036B\u036C\x03\x02\x02\x02\u036C\x89\x03\x02\x02\x02\u036D" +
		"\u036B\x03\x02\x02\x02\u036E\u0371\x07<\x02\x02\u036F\u0372\x05\xACW\x02" +
		"\u0370\u0372\x05\x8CG\x02\u0371\u036F\x03\x02\x02\x02\u0371\u0370\x03" +
		"\x02\x02\x02\u0371\u0372\x03\x02\x02\x02\u0372\u0373\x03\x02\x02\x02\u0373" +
		"\u038F\x07=\x02\x02\u0374\u0376\x07C\x02\x02\u0375\u0377\x05\x8CG\x02" +
		"\u0376\u0375\x03\x02\x02\x02\u0376\u0377\x03\x02\x02\x02\u0377\u0378\x03" +
		"\x02\x02\x02\u0378\u038F\x07D\x02\x02\u0379\u037B\x07P\x02\x02\u037A\u037C" +
		"\x05\x9AN\x02\u037B\u037A\x03\x02\x02\x02\u037B\u037C\x03\x02\x02\x02" +
		"\u037C\u037D\x03\x02\x02\x02\u037D\u038F\x07R\x02\x02\u037E\u038F\x07" +
		"0\x02\x02\u037F\u038F\x07\n\x02\x02\u0380\u0382\x05\xB0Y\x02\u0381\u0380" +
		"\x03\x02\x02\x02\u0382\u0383\x03\x02\x02\x02\u0383\u0381\x03\x02\x02\x02" +
		"\u0383\u0384\x03\x02\x02\x02\u0384\u038F\x03\x02\x02\x02\u0385\u0387\x07" +
		"\t\x02\x02\u0386\u0385\x03\x02\x02\x02\u0387\u0388\x03\x02\x02\x02\u0388" +
		"\u0386\x03\x02\x02\x02\u0388\u0389\x03\x02\x02\x02\u0389\u038F\x03\x02" +
		"\x02\x02\u038A\u038F\x07:\x02\x02\u038B\u038F\x07$\x02\x02\u038C\u038F" +
		"\x07%\x02\x02\u038D\u038F\x07&\x02\x02\u038E\u036E\x03\x02\x02\x02\u038E" +
		"\u0374\x03\x02\x02\x02\u038E\u0379\x03\x02\x02\x02\u038E\u037E\x03\x02" +
		"\x02\x02\u038E\u037F\x03\x02\x02\x02\u038E\u0381\x03\x02\x02\x02\u038E" +
		"\u0386\x03\x02\x02\x02\u038E\u038A\x03\x02\x02\x02\u038E\u038B\x03\x02" +
		"\x02\x02\u038E\u038C\x03\x02\x02\x02\u038E\u038D\x03\x02\x02\x02\u038F" +
		"\x8B\x03\x02\x02\x02\u0390\u0393\x05d3\x02\u0391\u0393\x05v<\x02\u0392" +
		"\u0390\x03\x02\x02\x02\u0392\u0391\x03\x02\x02\x02\u0393\u03A2\x03\x02" +
		"\x02\x02\u0394\u03A3\x05\xA6T\x02\u0395\u0398\x07>\x02\x02\u0396\u0399" +
		"\x05d3\x02\u0397\u0399\x05v<\x02\u0398\u0396\x03\x02\x02\x02\u0398\u0397" +
		"\x03\x02\x02\x02\u0399\u039B\x03\x02\x02\x02\u039A\u0395\x03\x02\x02\x02" +
		"\u039B\u039E\x03\x02\x02\x02\u039C\u039A\x03\x02\x02\x02\u039C\u039D\x03" +
		"\x02\x02\x02\u039D\u03A0\x03\x02\x02\x02\u039E\u039C\x03\x02\x02\x02\u039F" +
		"\u03A1\x07>\x02\x02\u03A0\u039F\x03\x02\x02\x02\u03A0\u03A1\x03\x02\x02" +
		"\x02\u03A1\u03A3\x03\x02\x02\x02\u03A2\u0394\x03\x02\x02\x02\u03A2\u039C" +
		"\x03\x02\x02\x02\u03A3\x8D\x03\x02\x02\x02\u03A4\u03AC\x05\x9EP\x02\u03A5" +
		"\u03A6\x07C\x02\x02\u03A6\u03A7\x05\x90I\x02\u03A7\u03A8\x07D\x02\x02" +
		"\u03A8\u03AC\x03\x02\x02\x02\u03A9\u03AA\x079\x02\x02\u03AA\u03AC\x07" +
		"0\x02\x02\u03AB\u03A4\x03\x02\x02\x02\u03AB\u03A5\x03\x02\x02\x02\u03AB" +
		"\u03A9\x03\x02\x02\x02\u03AC\x8F\x03\x02\x02\x02\u03AD\u03B2\x05\x92J" +
		"\x02\u03AE\u03AF\x07>\x02\x02\u03AF\u03B1\x05\x92J\x02\u03B0\u03AE\x03" +
		"\x02\x02\x02\u03B1\u03B4\x03\x02\x02\x02\u03B2\u03B0\x03\x02\x02\x02\u03B2" +
		"\u03B3\x03\x02\x02\x02\u03B3\u03B6\x03\x02\x02\x02\u03B4\u03B2\x03\x02" +
		"\x02\x02\u03B5\u03B7\x07>\x02\x02\u03B6\u03B5\x03\x02\x02\x02\u03B6\u03B7" +
		"\x03\x02\x02\x02\u03B7\x91\x03\x02\x02\x02\u03B8\u03C4\x05d3\x02\u03B9" +
		"\u03BB\x05d3\x02\u03BA\u03B9\x03\x02\x02\x02\u03BA\u03BB\x03\x02\x02\x02" +
		"\u03BB\u03BC\x03\x02\x02\x02\u03BC\u03BE\x07?\x02\x02\u03BD\u03BF\x05" +
		"d3\x02\u03BE\u03BD\x03\x02\x02\x02\u03BE\u03BF\x03\x02\x02\x02\u03BF\u03C1" +
		"\x03\x02\x02\x02\u03C0\u03C2\x05\x94K\x02\u03C1\u03C0\x03\x02\x02\x02" +
		"\u03C1\u03C2\x03\x02\x02\x02\u03C2\u03C4\x03\x02\x02\x02\u03C3\u03B8\x03" +
		"\x02\x02\x02\u03C3\u03BA\x03\x02\x02\x02\u03C4\x93\x03\x02\x02\x02\u03C5" +
		"\u03C7\x07?\x02\x02\u03C6\u03C8\x05d3\x02\u03C7\u03C6\x03\x02\x02\x02" +
		"\u03C7\u03C8\x03\x02\x02\x02\u03C8\x95\x03\x02\x02\x02\u03C9\u03CC\x05" +
		"x=\x02\u03CA\u03CC\x05v<\x02\u03CB\u03C9\x03\x02\x02\x02\u03CB\u03CA\x03" +
		"\x02\x02\x02\u03CC\u03D4\x03\x02\x02\x02\u03CD\u03D0\x07>\x02\x02\u03CE" +
		"\u03D1\x05x=\x02\u03CF\u03D1\x05v<\x02\u03D0\u03CE\x03\x02\x02\x02\u03D0" +
		"\u03CF\x03\x02\x02\x02\u03D1\u03D3\x03\x02\x02\x02\u03D2\u03CD\x03\x02" +
		"\x02\x02\u03D3\u03D6\x03\x02\x02\x02\u03D4\u03D2\x03\x02\x02\x02\u03D4" +
		"\u03D5\x03\x02\x02\x02\u03D5\u03D8\x03\x02\x02\x02\u03D6\u03D4\x03\x02" +
		"\x02\x02\u03D7\u03D9\x07>\x02\x02\u03D8\u03D7\x03\x02\x02\x02\u03D8\u03D9" +
		"\x03\x02\x02\x02\u03D9\x97\x03\x02\x02\x02\u03DA\u03DF\x05d3\x02\u03DB" +
		"\u03DC\x07>\x02\x02\u03DC\u03DE\x05d3\x02\u03DD\u03DB\x03\x02\x02\x02" +
		"\u03DE\u03E1\x03\x02\x02\x02\u03DF\u03DD\x03\x02\x02\x02\u03DF\u03E0\x03" +
		"\x02\x02\x02\u03E0\u03E3\x03\x02\x02\x02\u03E1\u03DF\x03\x02\x02\x02\u03E2" +
		"\u03E4\x07>\x02\x02\u03E3\u03E2\x03\x02\x02\x02\u03E3\u03E4\x03\x02\x02" +
		"\x02\u03E4\x99\x03\x02\x02\x02\u03E5\u03E6\x05d3\x02\u03E6\u03E7\x07?" +
		"\x02\x02\u03E7\u03E8\x05d3\x02\u03E8\u03EC\x03\x02\x02\x02\u03E9\u03EA" +
		"\x07A\x02\x02\u03EA\u03EC\x05x=\x02\u03EB\u03E5\x03\x02\x02\x02\u03EB" +
		"\u03E9\x03\x02\x02\x02\u03EC\u03FF\x03\x02\x02\x02\u03ED\u0400\x05\xA6" +
		"T\x02\u03EE\u03F5\x07>\x02\x02\u03EF\u03F0\x05d3\x02\u03F0\u03F1\x07?" +
		"\x02\x02\u03F1\u03F2\x05d3\x02\u03F2\u03F6\x03\x02\x02\x02\u03F3\u03F4" +
		"\x07A\x02\x02\u03F4\u03F6\x05x=\x02\u03F5\u03EF\x03\x02\x02\x02\u03F5" +
		"\u03F3\x03\x02\x02\x02\u03F6\u03F8\x03\x02\x02\x02\u03F7\u03EE\x03\x02" +
		"\x02\x02\u03F8\u03FB\x03\x02\x02\x02\u03F9\u03F7\x03\x02\x02\x02\u03F9" +
		"\u03FA\x03\x02\x02\x02\u03FA\u03FD\x03\x02\x02\x02\u03FB\u03F9\x03\x02" +
		"\x02\x02\u03FC\u03FE\x07>\x02\x02\u03FD\u03FC\x03\x02\x02\x02\u03FD\u03FE" +
		"\x03\x02\x02\x02\u03FE\u0400\x03\x02\x02\x02\u03FF\u03ED\x03\x02\x02\x02" +
		"\u03FF\u03F9\x03\x02\x02\x02\u0400\u0416\x03\x02\x02\x02\u0401\u0404\x05" +
		"d3\x02\u0402\u0404\x05v<\x02\u0403\u0401\x03\x02\x02\x02\u0403\u0402\x03" +
		"\x02\x02\x02\u0404\u0413\x03\x02\x02\x02\u0405\u0414\x05\xA6T\x02\u0406" +
		"\u0409\x07>\x02\x02\u0407\u040A\x05d3\x02\u0408\u040A\x05v<\x02\u0409" +
		"\u0407\x03\x02\x02\x02\u0409\u0408\x03\x02\x02\x02\u040A\u040C\x03\x02" +
		"\x02\x02\u040B\u0406\x03\x02\x02\x02\u040C\u040F\x03\x02\x02\x02\u040D" +
		"\u040B\x03\x02\x02\x02\u040D\u040E\x03\x02\x02\x02\u040E\u0411\x03\x02" +
		"\x02\x02\u040F\u040D\x03\x02\x02\x02\u0410\u0412\x07>\x02\x02\u0411\u0410" +
		"\x03\x02\x02\x02\u0411\u0412\x03\x02\x02\x02\u0412\u0414\x03\x02\x02\x02" +
		"\u0413\u0405\x03\x02\x02\x02\u0413\u040D\x03\x02\x02\x02\u0414\u0416\x03" +
		"\x02\x02\x02\u0415\u03EB\x03\x02\x02\x02\u0415\u0403\x03\x02\x02\x02\u0416" +
		"\x9B\x03\x02\x02\x02\u0417\u0418\x07\'\x02\x02\u0418\u041E\x070\x02\x02" +
		"\u0419\u041B\x07<\x02\x02\u041A\u041C\x05\xA0Q\x02\u041B\u041A\x03\x02" +
		"\x02\x02\u041B\u041C\x03\x02\x02\x02\u041C\u041D\x03\x02\x02\x02\u041D" +
		"\u041F\x07=\x02\x02\u041E\u0419\x03\x02\x02\x02\u041E\u041F\x03\x02\x02" +
		"\x02\u041F\u0420\x03\x02\x02\x02\u0420\u0421\x07?\x02\x02\u0421\u0422" +
		"\x05b2\x02\u0422\x9D\x03\x02\x02\x02\u0423\u0425\x07<\x02\x02\u0424\u0426" +
		"\x05\xA0Q\x02\u0425\u0424\x03\x02\x02\x02\u0425\u0426\x03\x02\x02\x02" +
		"\u0426\u0427\x03\x02\x02\x02\u0427\u0428\x07=\x02\x02\u0428\x9F\x03\x02" +
		"\x02\x02\u0429\u042E\x05\xA2R\x02\u042A\u042B\x07>\x02\x02\u042B\u042D" +
		"\x05\xA2R\x02\u042C\u042A\x03\x02\x02\x02\u042D\u0430\x03\x02\x02\x02" +
		"\u042E\u042C\x03\x02\x02\x02\u042E\u042F\x03\x02\x02\x02\u042F\u0432\x03" +
		"\x02\x02\x02\u0430\u042E\x03\x02\x02\x02\u0431\u0433\x07>\x02\x02\u0432" +
		"\u0431\x03\x02\x02\x02\u0432\u0433\x03\x02\x02\x02\u0433\xA1\x03\x02\x02" +
		"\x02\u0434\u0436\x05d3\x02\u0435\u0437\x05\xA6T\x02\u0436\u0435\x03\x02" +
		"\x02\x02\u0436\u0437\x03\x02\x02\x02\u0437\u0441\x03\x02\x02\x02\u0438" +
		"\u0439\x05d3\x02\u0439\u043A\x07B\x02\x02\u043A\u043B\x05d3\x02\u043B" +
		"\u0441\x03\x02\x02\x02\u043C\u043D\x07A\x02\x02\u043D\u0441\x05d3\x02" +
		"\u043E\u043F\x07;\x02\x02\u043F\u0441\x05";
	private static readonly _serializedATNSegment2: string =
		"d3\x02\u0440\u0434\x03\x02\x02\x02\u0440\u0438\x03\x02\x02\x02\u0440\u043C" +
		"\x03\x02\x02\x02\u0440\u043E\x03\x02\x02\x02\u0441\xA3\x03\x02\x02\x02" +
		"\u0442\u0445\x05\xA6T\x02\u0443\u0445\x05\xA8U\x02\u0444\u0442\x03\x02" +
		"\x02\x02\u0444\u0443\x03\x02\x02\x02\u0445\xA5\x03\x02\x02\x02\u0446\u0448" +
		"\x07-\x02\x02\u0447\u0446\x03\x02\x02\x02\u0447\u0448\x03\x02\x02\x02" +
		"\u0448\u0449\x03\x02\x02\x02\u0449\u044A\x07\x19\x02\x02\u044A\u044B\x05" +
		"\x96L\x02\u044B\u044C\x07\x1A\x02\x02\u044C\u044E\x05l7\x02\u044D\u044F" +
		"\x05\xA4S\x02\u044E\u044D\x03\x02\x02\x02\u044E\u044F\x03\x02\x02\x02" +
		"\u044F\xA7\x03\x02\x02\x02\u0450\u0451\x07\x15\x02\x02\u0451\u0453\x05" +
		"f4\x02\u0452\u0454\x05\xA4S\x02\u0453\u0452\x03\x02\x02\x02\u0453\u0454" +
		"\x03\x02\x02\x02\u0454\xA9\x03\x02\x02\x02\u0455\u0456\x070\x02\x02\u0456" +
		"\xAB\x03\x02\x02\x02\u0457\u0459\x07(\x02\x02\u0458\u045A\x05\xAEX\x02" +
		"\u0459\u0458\x03\x02\x02\x02\u0459\u045A\x03\x02\x02\x02\u045A\xAD\x03" +
		"\x02\x02\x02\u045B\u045C\x07\x0F\x02\x02\u045C\u045F\x05d3\x02\u045D\u045F" +
		"\x05\x98M\x02\u045E\u045B\x03\x02\x02\x02\u045E\u045D\x03\x02\x02\x02" +
		"\u045F\xAF\x03\x02\x02\x02\u0460\u0464\x07\x05\x02\x02\u0461\u0463\x05" +
		"\xB2Z\x02\u0462\u0461\x03\x02\x02\x02\u0463\u0466\x03\x02\x02\x02\u0464" +
		"\u0462\x03\x02\x02\x02\u0464\u0465\x03\x02\x02\x02\u0465\u0467\x03\x02" +
		"\x02\x02\u0466\u0464\x03\x02\x02\x02\u0467\u0481\x07m\x02\x02\u0468\u046C" +
		"\x07\x07\x02\x02\u0469\u046B\x05\xB2Z\x02\u046A\u0469\x03\x02\x02\x02" +
		"\u046B\u046E\x03\x02\x02\x02\u046C\u046A\x03\x02\x02\x02\u046C\u046D\x03" +
		"\x02\x02\x02\u046D\u046F\x03\x02\x02\x02\u046E\u046C\x03\x02\x02\x02\u046F" +
		"\u0481\x07n\x02\x02\u0470\u0474\x07\x06\x02\x02\u0471\u0473\x05\xB4[\x02" +
		"\u0472\u0471\x03\x02\x02\x02\u0473\u0476\x03\x02\x02\x02\u0474\u0472\x03" +
		"\x02\x02\x02\u0474\u0475\x03\x02\x02\x02\u0475\u0477\x03\x02\x02\x02\u0476" +
		"\u0474\x03\x02\x02\x02\u0477\u0481\x07p\x02\x02\u0478\u047C\x07\b\x02" +
		"\x02\u0479\u047B\x05\xB4[\x02\u047A\u0479\x03\x02\x02\x02\u047B\u047E" +
		"\x03\x02\x02\x02\u047C\u047A\x03\x02\x02\x02\u047C\u047D\x03\x02\x02\x02" +
		"\u047D\u047F\x03\x02\x02\x02\u047E\u047C\x03\x02\x02\x02\u047F\u0481\x07" +
		"q\x02\x02\u0480\u0460\x03\x02\x02\x02\u0480\u0468\x03\x02\x02\x02\u0480" +
		"\u0470\x03\x02\x02\x02\u0480\u0478\x03\x02\x02\x02\u0481\xB1\x03\x02\x02" +
		"\x02\u0482\u048B\x07o\x02\x02\u0483\u0486\x07P\x02\x02\u0484\u0487\x05" +
		"d3\x02\u0485\u0487\x05v<\x02\u0486\u0484\x03\x02\x02\x02\u0486\u0485\x03" +
		"\x02\x02\x02\u0487\u0488\x03\x02\x02\x02\u0488\u0489\x07Q\x02\x02\u0489" +
		"\u048B\x03\x02\x02\x02\u048A\u0482\x03\x02\x02\x02\u048A\u0483\x03\x02" +
		"\x02\x02\u048B\xB3\x03\x02\x02\x02\u048C\u0495\x07r\x02\x02\u048D\u0490" +
		"\x07P\x02\x02\u048E\u0491\x05d3\x02\u048F\u0491\x05v<\x02\u0490\u048E" +
		"\x03\x02\x02\x02\u0490\u048F\x03\x02\x02\x02\u0491\u0492\x03\x02\x02\x02" +
		"\u0492\u0493\x07Q\x02\x02\u0493\u0495\x03\x02\x02\x02\u0494\u048C\x03" +
		"\x02\x02\x02\u0494\u048D\x03\x02\x02\x02\u0495\xB5\x03\x02\x02\x02\xB2" +
		"\xB8\xBA\xC4\xCA\xD3\xD6\xDD\xE3\xED\xF4\xFB\u0101\u0105\u010B\u0111\u0115" +
		"\u011C\u011E\u0120\u0125\u0127\u0129\u012D\u0133\u0137\u013E\u0140\u0142" +
		"\u0147\u0149\u014E\u0153\u0159\u015D\u0163\u0169\u016D\u0174\u0176\u0178" +
		"\u017D\u017F\u0181\u0185\u018B\u018F\u0196\u0198\u019A\u019F\u01A1\u01A7" +
		"\u01AE\u01B2\u01BE\u01C5\u01CA\u01CE\u01D1\u01D7\u01DB\u01E0\u01E4\u01E8" +
		"\u01F6\u01FE\u0206\u0208\u020C\u0215\u021C\u021E\u0227\u022C\u0231\u0238" +
		"\u023C\u0243\u024B\u0254\u025D\u0264\u026F\u0275\u0282\u0288\u0291\u029C" +
		"\u02A7\u02AC\u02B1\u02B6\u02BE\u02C7\u02CD\u02CF\u02D7\u02DB\u02E3\u02E6" +
		"\u02EA\u02EE\u02F5\u02FF\u0307\u030D\u0315\u0325\u032F\u0337\u033F\u0347" +
		"\u034F\u0357\u035D\u0362\u0365\u036B\u0371\u0376\u037B\u0383\u0388\u038E" +
		"\u0392\u0398\u039C\u03A0\u03A2\u03AB\u03B2\u03B6\u03BA\u03BE\u03C1\u03C3" +
		"\u03C7\u03CB\u03D0\u03D4\u03D8\u03DF\u03E3\u03EB\u03F5\u03F9\u03FD\u03FF" +
		"\u0403\u0409\u040D\u0411\u0413\u0415\u041B\u041E\u0425\u042E\u0432\u0436" +
		"\u0440\u0444\u0447\u044E\u0453\u0459\u045E\u0464\u046C\u0474\u047C\u0480" +
		"\u0486\u048A\u0490\u0494";
	public static readonly _serializedATN: string = Utils.join(
		[
			Python3Parser._serializedATNSegment0,
			Python3Parser._serializedATNSegment1,
			Python3Parser._serializedATNSegment2,
		],
		"",
	);
	public static __ATN: ATN;
	public static get _ATN(): ATN {
		if (!Python3Parser.__ATN) {
			Python3Parser.__ATN = new ATNDeserializer().deserialize(Utils.toCharArray(Python3Parser._serializedATN));
		}

		return Python3Parser.__ATN;
	}

}

export class File_inputContext extends ParserRuleContext {
	public EOF(): TerminalNode { return this.getToken(Python3Parser.EOF, 0); }
	public NEWLINE(): TerminalNode[];
	public NEWLINE(i: number): TerminalNode;
	public NEWLINE(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.NEWLINE);
		} else {
			return this.getToken(Python3Parser.NEWLINE, i);
		}
	}
	public stmt(): StmtContext[];
	public stmt(i: number): StmtContext;
	public stmt(i?: number): StmtContext | StmtContext[] {
		if (i === undefined) {
			return this.getRuleContexts(StmtContext);
		} else {
			return this.getRuleContext(i, StmtContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_file_input; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterFile_input) {
			listener.enterFile_input(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitFile_input) {
			listener.exitFile_input(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitFile_input) {
			return visitor.visitFile_input(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Single_inputContext extends ParserRuleContext {
	public NEWLINE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NEWLINE, 0); }
	public simple_stmt(): Simple_stmtContext | undefined {
		return this.tryGetRuleContext(0, Simple_stmtContext);
	}
	public compound_stmt(): Compound_stmtContext | undefined {
		return this.tryGetRuleContext(0, Compound_stmtContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_single_input; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSingle_input) {
			listener.enterSingle_input(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSingle_input) {
			listener.exitSingle_input(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSingle_input) {
			return visitor.visitSingle_input(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Eval_inputContext extends ParserRuleContext {
	public testlist(): TestlistContext {
		return this.getRuleContext(0, TestlistContext);
	}
	public EOF(): TerminalNode { return this.getToken(Python3Parser.EOF, 0); }
	public NEWLINE(): TerminalNode[];
	public NEWLINE(i: number): TerminalNode;
	public NEWLINE(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.NEWLINE);
		} else {
			return this.getToken(Python3Parser.NEWLINE, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_eval_input; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterEval_input) {
			listener.enterEval_input(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitEval_input) {
			listener.exitEval_input(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitEval_input) {
			return visitor.visitEval_input(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class DecoratorContext extends ParserRuleContext {
	public AT(): TerminalNode { return this.getToken(Python3Parser.AT, 0); }
	public dotted_name(): Dotted_nameContext {
		return this.getRuleContext(0, Dotted_nameContext);
	}
	public NEWLINE(): TerminalNode { return this.getToken(Python3Parser.NEWLINE, 0); }
	public OPEN_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_PAREN, 0); }
	public CLOSE_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_PAREN, 0); }
	public arglist(): ArglistContext | undefined {
		return this.tryGetRuleContext(0, ArglistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_decorator; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDecorator) {
			listener.enterDecorator(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDecorator) {
			listener.exitDecorator(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDecorator) {
			return visitor.visitDecorator(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class DecoratorsContext extends ParserRuleContext {
	public decorator(): DecoratorContext[];
	public decorator(i: number): DecoratorContext;
	public decorator(i?: number): DecoratorContext | DecoratorContext[] {
		if (i === undefined) {
			return this.getRuleContexts(DecoratorContext);
		} else {
			return this.getRuleContext(i, DecoratorContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_decorators; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDecorators) {
			listener.enterDecorators(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDecorators) {
			listener.exitDecorators(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDecorators) {
			return visitor.visitDecorators(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class DecoratedContext extends ParserRuleContext {
	public decorators(): DecoratorsContext {
		return this.getRuleContext(0, DecoratorsContext);
	}
	public classdef(): ClassdefContext | undefined {
		return this.tryGetRuleContext(0, ClassdefContext);
	}
	public funcdef(): FuncdefContext | undefined {
		return this.tryGetRuleContext(0, FuncdefContext);
	}
	public async_funcdef(): Async_funcdefContext | undefined {
		return this.tryGetRuleContext(0, Async_funcdefContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_decorated; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDecorated) {
			listener.enterDecorated(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDecorated) {
			listener.exitDecorated(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDecorated) {
			return visitor.visitDecorated(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Async_funcdefContext extends ParserRuleContext {
	public ASYNC(): TerminalNode { return this.getToken(Python3Parser.ASYNC, 0); }
	public funcdef(): FuncdefContext {
		return this.getRuleContext(0, FuncdefContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_async_funcdef; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAsync_funcdef) {
			listener.enterAsync_funcdef(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAsync_funcdef) {
			listener.exitAsync_funcdef(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAsync_funcdef) {
			return visitor.visitAsync_funcdef(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FuncdefContext extends ParserRuleContext {
	public DEF(): TerminalNode { return this.getToken(Python3Parser.DEF, 0); }
	public NAME(): TerminalNode { return this.getToken(Python3Parser.NAME, 0); }
	public parameters(): ParametersContext {
		return this.getRuleContext(0, ParametersContext);
	}
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public suite(): SuiteContext {
		return this.getRuleContext(0, SuiteContext);
	}
	public ARROW(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ARROW, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_funcdef; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterFuncdef) {
			listener.enterFuncdef(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitFuncdef) {
			listener.exitFuncdef(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitFuncdef) {
			return visitor.visitFuncdef(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ParametersContext extends ParserRuleContext {
	public OPEN_PAREN(): TerminalNode { return this.getToken(Python3Parser.OPEN_PAREN, 0); }
	public CLOSE_PAREN(): TerminalNode { return this.getToken(Python3Parser.CLOSE_PAREN, 0); }
	public typedargslist(): TypedargslistContext | undefined {
		return this.tryGetRuleContext(0, TypedargslistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_parameters; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterParameters) {
			listener.enterParameters(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitParameters) {
			listener.exitParameters(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitParameters) {
			return visitor.visitParameters(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TypedargslistContext extends ParserRuleContext {
	public tfpdef(): TfpdefContext[];
	public tfpdef(i: number): TfpdefContext;
	public tfpdef(i?: number): TfpdefContext | TfpdefContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TfpdefContext);
		} else {
			return this.getRuleContext(i, TfpdefContext);
		}
	}
	public STAR(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.STAR, 0); }
	public POWER(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.POWER, 0); }
	public ASSIGN(): TerminalNode[];
	public ASSIGN(i: number): TerminalNode;
	public ASSIGN(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.ASSIGN);
		} else {
			return this.getToken(Python3Parser.ASSIGN, i);
		}
	}
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_typedargslist; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTypedargslist) {
			listener.enterTypedargslist(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTypedargslist) {
			listener.exitTypedargslist(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTypedargslist) {
			return visitor.visitTypedargslist(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TfpdefContext extends ParserRuleContext {
	public NAME(): TerminalNode { return this.getToken(Python3Parser.NAME, 0); }
	public COLON(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.COLON, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_tfpdef; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTfpdef) {
			listener.enterTfpdef(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTfpdef) {
			listener.exitTfpdef(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTfpdef) {
			return visitor.visitTfpdef(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VarargslistContext extends ParserRuleContext {
	public vfpdef(): VfpdefContext[];
	public vfpdef(i: number): VfpdefContext;
	public vfpdef(i?: number): VfpdefContext | VfpdefContext[] {
		if (i === undefined) {
			return this.getRuleContexts(VfpdefContext);
		} else {
			return this.getRuleContext(i, VfpdefContext);
		}
	}
	public STAR(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.STAR, 0); }
	public POWER(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.POWER, 0); }
	public ASSIGN(): TerminalNode[];
	public ASSIGN(i: number): TerminalNode;
	public ASSIGN(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.ASSIGN);
		} else {
			return this.getToken(Python3Parser.ASSIGN, i);
		}
	}
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_varargslist; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterVarargslist) {
			listener.enterVarargslist(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitVarargslist) {
			listener.exitVarargslist(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitVarargslist) {
			return visitor.visitVarargslist(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class VfpdefContext extends ParserRuleContext {
	public NAME(): TerminalNode { return this.getToken(Python3Parser.NAME, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_vfpdef; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterVfpdef) {
			listener.enterVfpdef(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitVfpdef) {
			listener.exitVfpdef(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitVfpdef) {
			return visitor.visitVfpdef(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class StmtContext extends ParserRuleContext {
	public simple_stmt(): Simple_stmtContext | undefined {
		return this.tryGetRuleContext(0, Simple_stmtContext);
	}
	public compound_stmt(): Compound_stmtContext | undefined {
		return this.tryGetRuleContext(0, Compound_stmtContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterStmt) {
			listener.enterStmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitStmt) {
			listener.exitStmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitStmt) {
			return visitor.visitStmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Simple_stmtContext extends ParserRuleContext {
	public small_stmt(): Small_stmtContext[];
	public small_stmt(i: number): Small_stmtContext;
	public small_stmt(i?: number): Small_stmtContext | Small_stmtContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Small_stmtContext);
		} else {
			return this.getRuleContext(i, Small_stmtContext);
		}
	}
	public NEWLINE(): TerminalNode { return this.getToken(Python3Parser.NEWLINE, 0); }
	public SEMI_COLON(): TerminalNode[];
	public SEMI_COLON(i: number): TerminalNode;
	public SEMI_COLON(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.SEMI_COLON);
		} else {
			return this.getToken(Python3Parser.SEMI_COLON, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_simple_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSimple_stmt) {
			listener.enterSimple_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSimple_stmt) {
			listener.exitSimple_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSimple_stmt) {
			return visitor.visitSimple_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Small_stmtContext extends ParserRuleContext {
	public expr_stmt(): Expr_stmtContext | undefined {
		return this.tryGetRuleContext(0, Expr_stmtContext);
	}
	public del_stmt(): Del_stmtContext | undefined {
		return this.tryGetRuleContext(0, Del_stmtContext);
	}
	public pass_stmt(): Pass_stmtContext | undefined {
		return this.tryGetRuleContext(0, Pass_stmtContext);
	}
	public flow_stmt(): Flow_stmtContext | undefined {
		return this.tryGetRuleContext(0, Flow_stmtContext);
	}
	public import_stmt(): Import_stmtContext | undefined {
		return this.tryGetRuleContext(0, Import_stmtContext);
	}
	public global_stmt(): Global_stmtContext | undefined {
		return this.tryGetRuleContext(0, Global_stmtContext);
	}
	public nonlocal_stmt(): Nonlocal_stmtContext | undefined {
		return this.tryGetRuleContext(0, Nonlocal_stmtContext);
	}
	public assert_stmt(): Assert_stmtContext | undefined {
		return this.tryGetRuleContext(0, Assert_stmtContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_small_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSmall_stmt) {
			listener.enterSmall_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSmall_stmt) {
			listener.exitSmall_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSmall_stmt) {
			return visitor.visitSmall_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Expr_stmtContext extends ParserRuleContext {
	public testlist_star_expr(): Testlist_star_exprContext[];
	public testlist_star_expr(i: number): Testlist_star_exprContext;
	public testlist_star_expr(i?: number): Testlist_star_exprContext | Testlist_star_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Testlist_star_exprContext);
		} else {
			return this.getRuleContext(i, Testlist_star_exprContext);
		}
	}
	public annassign(): AnnassignContext | undefined {
		return this.tryGetRuleContext(0, AnnassignContext);
	}
	public augassign(): AugassignContext | undefined {
		return this.tryGetRuleContext(0, AugassignContext);
	}
	public yield_expr(): Yield_exprContext[];
	public yield_expr(i: number): Yield_exprContext;
	public yield_expr(i?: number): Yield_exprContext | Yield_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Yield_exprContext);
		} else {
			return this.getRuleContext(i, Yield_exprContext);
		}
	}
	public testlist(): TestlistContext | undefined {
		return this.tryGetRuleContext(0, TestlistContext);
	}
	public ASSIGN(): TerminalNode[];
	public ASSIGN(i: number): TerminalNode;
	public ASSIGN(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.ASSIGN);
		} else {
			return this.getToken(Python3Parser.ASSIGN, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_expr_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterExpr_stmt) {
			listener.enterExpr_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitExpr_stmt) {
			listener.exitExpr_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitExpr_stmt) {
			return visitor.visitExpr_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AnnassignContext extends ParserRuleContext {
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ASSIGN, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_annassign; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAnnassign) {
			listener.enterAnnassign(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAnnassign) {
			listener.exitAnnassign(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAnnassign) {
			return visitor.visitAnnassign(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Testlist_star_exprContext extends ParserRuleContext {
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public star_expr(): Star_exprContext[];
	public star_expr(i: number): Star_exprContext;
	public star_expr(i?: number): Star_exprContext | Star_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Star_exprContext);
		} else {
			return this.getRuleContext(i, Star_exprContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_testlist_star_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTestlist_star_expr) {
			listener.enterTestlist_star_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTestlist_star_expr) {
			listener.exitTestlist_star_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTestlist_star_expr) {
			return visitor.visitTestlist_star_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AugassignContext extends ParserRuleContext {
	public ADD_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ADD_ASSIGN, 0); }
	public SUB_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.SUB_ASSIGN, 0); }
	public MULT_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.MULT_ASSIGN, 0); }
	public AT_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AT_ASSIGN, 0); }
	public DIV_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DIV_ASSIGN, 0); }
	public MOD_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.MOD_ASSIGN, 0); }
	public AND_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AND_ASSIGN, 0); }
	public OR_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OR_ASSIGN, 0); }
	public XOR_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.XOR_ASSIGN, 0); }
	public LEFT_SHIFT_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.LEFT_SHIFT_ASSIGN, 0); }
	public RIGHT_SHIFT_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.RIGHT_SHIFT_ASSIGN, 0); }
	public POWER_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.POWER_ASSIGN, 0); }
	public IDIV_ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.IDIV_ASSIGN, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_augassign; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAugassign) {
			listener.enterAugassign(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAugassign) {
			listener.exitAugassign(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAugassign) {
			return visitor.visitAugassign(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Del_stmtContext extends ParserRuleContext {
	public DEL(): TerminalNode { return this.getToken(Python3Parser.DEL, 0); }
	public exprlist(): ExprlistContext {
		return this.getRuleContext(0, ExprlistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_del_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDel_stmt) {
			listener.enterDel_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDel_stmt) {
			listener.exitDel_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDel_stmt) {
			return visitor.visitDel_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Pass_stmtContext extends ParserRuleContext {
	public PASS(): TerminalNode { return this.getToken(Python3Parser.PASS, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_pass_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterPass_stmt) {
			listener.enterPass_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitPass_stmt) {
			listener.exitPass_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitPass_stmt) {
			return visitor.visitPass_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Flow_stmtContext extends ParserRuleContext {
	public break_stmt(): Break_stmtContext | undefined {
		return this.tryGetRuleContext(0, Break_stmtContext);
	}
	public continue_stmt(): Continue_stmtContext | undefined {
		return this.tryGetRuleContext(0, Continue_stmtContext);
	}
	public return_stmt(): Return_stmtContext | undefined {
		return this.tryGetRuleContext(0, Return_stmtContext);
	}
	public raise_stmt(): Raise_stmtContext | undefined {
		return this.tryGetRuleContext(0, Raise_stmtContext);
	}
	public yield_stmt(): Yield_stmtContext | undefined {
		return this.tryGetRuleContext(0, Yield_stmtContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_flow_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterFlow_stmt) {
			listener.enterFlow_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitFlow_stmt) {
			listener.exitFlow_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitFlow_stmt) {
			return visitor.visitFlow_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Break_stmtContext extends ParserRuleContext {
	public BREAK(): TerminalNode { return this.getToken(Python3Parser.BREAK, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_break_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterBreak_stmt) {
			listener.enterBreak_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitBreak_stmt) {
			listener.exitBreak_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitBreak_stmt) {
			return visitor.visitBreak_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Continue_stmtContext extends ParserRuleContext {
	public CONTINUE(): TerminalNode { return this.getToken(Python3Parser.CONTINUE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_continue_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterContinue_stmt) {
			listener.enterContinue_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitContinue_stmt) {
			listener.exitContinue_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitContinue_stmt) {
			return visitor.visitContinue_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Return_stmtContext extends ParserRuleContext {
	public RETURN(): TerminalNode { return this.getToken(Python3Parser.RETURN, 0); }
	public testlist(): TestlistContext | undefined {
		return this.tryGetRuleContext(0, TestlistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_return_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterReturn_stmt) {
			listener.enterReturn_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitReturn_stmt) {
			listener.exitReturn_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitReturn_stmt) {
			return visitor.visitReturn_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Yield_stmtContext extends ParserRuleContext {
	public yield_expr(): Yield_exprContext {
		return this.getRuleContext(0, Yield_exprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_yield_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterYield_stmt) {
			listener.enterYield_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitYield_stmt) {
			listener.exitYield_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitYield_stmt) {
			return visitor.visitYield_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Raise_stmtContext extends ParserRuleContext {
	public RAISE(): TerminalNode { return this.getToken(Python3Parser.RAISE, 0); }
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public FROM(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.FROM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_raise_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterRaise_stmt) {
			listener.enterRaise_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitRaise_stmt) {
			listener.exitRaise_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitRaise_stmt) {
			return visitor.visitRaise_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Import_stmtContext extends ParserRuleContext {
	public import_name(): Import_nameContext | undefined {
		return this.tryGetRuleContext(0, Import_nameContext);
	}
	public import_from(): Import_fromContext | undefined {
		return this.tryGetRuleContext(0, Import_fromContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_import_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterImport_stmt) {
			listener.enterImport_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitImport_stmt) {
			listener.exitImport_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitImport_stmt) {
			return visitor.visitImport_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Import_nameContext extends ParserRuleContext {
	public IMPORT(): TerminalNode { return this.getToken(Python3Parser.IMPORT, 0); }
	public dotted_as_names(): Dotted_as_namesContext {
		return this.getRuleContext(0, Dotted_as_namesContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_import_name; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterImport_name) {
			listener.enterImport_name(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitImport_name) {
			listener.exitImport_name(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitImport_name) {
			return visitor.visitImport_name(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Import_fromContext extends ParserRuleContext {
	public FROM(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.FROM, 0); }
	public IMPORT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.IMPORT, 0); }
	public dotted_name(): Dotted_nameContext | undefined {
		return this.tryGetRuleContext(0, Dotted_nameContext);
	}
	public STAR(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.STAR, 0); }
	public OPEN_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_PAREN, 0); }
	public import_as_names(): Import_as_namesContext | undefined {
		return this.tryGetRuleContext(0, Import_as_namesContext);
	}
	public CLOSE_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_PAREN, 0); }
	public DOT(): TerminalNode[];
	public DOT(i: number): TerminalNode;
	public DOT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.DOT);
		} else {
			return this.getToken(Python3Parser.DOT, i);
		}
	}
	public ELLIPSIS(): TerminalNode[];
	public ELLIPSIS(i: number): TerminalNode;
	public ELLIPSIS(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.ELLIPSIS);
		} else {
			return this.getToken(Python3Parser.ELLIPSIS, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_import_from; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterImport_from) {
			listener.enterImport_from(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitImport_from) {
			listener.exitImport_from(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitImport_from) {
			return visitor.visitImport_from(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Import_as_nameContext extends ParserRuleContext {
	public NAME(): TerminalNode[];
	public NAME(i: number): TerminalNode;
	public NAME(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.NAME);
		} else {
			return this.getToken(Python3Parser.NAME, i);
		}
	}
	public AS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AS, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_import_as_name; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterImport_as_name) {
			listener.enterImport_as_name(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitImport_as_name) {
			listener.exitImport_as_name(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitImport_as_name) {
			return visitor.visitImport_as_name(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Dotted_as_nameContext extends ParserRuleContext {
	public dotted_name(): Dotted_nameContext {
		return this.getRuleContext(0, Dotted_nameContext);
	}
	public AS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AS, 0); }
	public NAME(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NAME, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_dotted_as_name; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDotted_as_name) {
			listener.enterDotted_as_name(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDotted_as_name) {
			listener.exitDotted_as_name(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDotted_as_name) {
			return visitor.visitDotted_as_name(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Import_as_namesContext extends ParserRuleContext {
	public import_as_name(): Import_as_nameContext[];
	public import_as_name(i: number): Import_as_nameContext;
	public import_as_name(i?: number): Import_as_nameContext | Import_as_nameContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Import_as_nameContext);
		} else {
			return this.getRuleContext(i, Import_as_nameContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_import_as_names; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterImport_as_names) {
			listener.enterImport_as_names(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitImport_as_names) {
			listener.exitImport_as_names(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitImport_as_names) {
			return visitor.visitImport_as_names(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Dotted_as_namesContext extends ParserRuleContext {
	public dotted_as_name(): Dotted_as_nameContext[];
	public dotted_as_name(i: number): Dotted_as_nameContext;
	public dotted_as_name(i?: number): Dotted_as_nameContext | Dotted_as_nameContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Dotted_as_nameContext);
		} else {
			return this.getRuleContext(i, Dotted_as_nameContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_dotted_as_names; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDotted_as_names) {
			listener.enterDotted_as_names(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDotted_as_names) {
			listener.exitDotted_as_names(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDotted_as_names) {
			return visitor.visitDotted_as_names(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Dotted_nameContext extends ParserRuleContext {
	public NAME(): TerminalNode[];
	public NAME(i: number): TerminalNode;
	public NAME(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.NAME);
		} else {
			return this.getToken(Python3Parser.NAME, i);
		}
	}
	public DOT(): TerminalNode[];
	public DOT(i: number): TerminalNode;
	public DOT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.DOT);
		} else {
			return this.getToken(Python3Parser.DOT, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_dotted_name; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDotted_name) {
			listener.enterDotted_name(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDotted_name) {
			listener.exitDotted_name(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDotted_name) {
			return visitor.visitDotted_name(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Global_stmtContext extends ParserRuleContext {
	public GLOBAL(): TerminalNode { return this.getToken(Python3Parser.GLOBAL, 0); }
	public NAME(): TerminalNode[];
	public NAME(i: number): TerminalNode;
	public NAME(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.NAME);
		} else {
			return this.getToken(Python3Parser.NAME, i);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_global_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterGlobal_stmt) {
			listener.enterGlobal_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitGlobal_stmt) {
			listener.exitGlobal_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitGlobal_stmt) {
			return visitor.visitGlobal_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Nonlocal_stmtContext extends ParserRuleContext {
	public NONLOCAL(): TerminalNode { return this.getToken(Python3Parser.NONLOCAL, 0); }
	public NAME(): TerminalNode[];
	public NAME(i: number): TerminalNode;
	public NAME(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.NAME);
		} else {
			return this.getToken(Python3Parser.NAME, i);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_nonlocal_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterNonlocal_stmt) {
			listener.enterNonlocal_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitNonlocal_stmt) {
			listener.exitNonlocal_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitNonlocal_stmt) {
			return visitor.visitNonlocal_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Assert_stmtContext extends ParserRuleContext {
	public ASSERT(): TerminalNode { return this.getToken(Python3Parser.ASSERT, 0); }
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COMMA(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.COMMA, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_assert_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAssert_stmt) {
			listener.enterAssert_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAssert_stmt) {
			listener.exitAssert_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAssert_stmt) {
			return visitor.visitAssert_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Compound_stmtContext extends ParserRuleContext {
	public if_stmt(): If_stmtContext | undefined {
		return this.tryGetRuleContext(0, If_stmtContext);
	}
	public while_stmt(): While_stmtContext | undefined {
		return this.tryGetRuleContext(0, While_stmtContext);
	}
	public for_stmt(): For_stmtContext | undefined {
		return this.tryGetRuleContext(0, For_stmtContext);
	}
	public try_stmt(): Try_stmtContext | undefined {
		return this.tryGetRuleContext(0, Try_stmtContext);
	}
	public with_stmt(): With_stmtContext | undefined {
		return this.tryGetRuleContext(0, With_stmtContext);
	}
	public funcdef(): FuncdefContext | undefined {
		return this.tryGetRuleContext(0, FuncdefContext);
	}
	public classdef(): ClassdefContext | undefined {
		return this.tryGetRuleContext(0, ClassdefContext);
	}
	public decorated(): DecoratedContext | undefined {
		return this.tryGetRuleContext(0, DecoratedContext);
	}
	public async_stmt(): Async_stmtContext | undefined {
		return this.tryGetRuleContext(0, Async_stmtContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_compound_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterCompound_stmt) {
			listener.enterCompound_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitCompound_stmt) {
			listener.exitCompound_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitCompound_stmt) {
			return visitor.visitCompound_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Async_stmtContext extends ParserRuleContext {
	public ASYNC(): TerminalNode { return this.getToken(Python3Parser.ASYNC, 0); }
	public funcdef(): FuncdefContext | undefined {
		return this.tryGetRuleContext(0, FuncdefContext);
	}
	public with_stmt(): With_stmtContext | undefined {
		return this.tryGetRuleContext(0, With_stmtContext);
	}
	public for_stmt(): For_stmtContext | undefined {
		return this.tryGetRuleContext(0, For_stmtContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_async_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAsync_stmt) {
			listener.enterAsync_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAsync_stmt) {
			listener.exitAsync_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAsync_stmt) {
			return visitor.visitAsync_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class If_stmtContext extends ParserRuleContext {
	public IF(): TerminalNode { return this.getToken(Python3Parser.IF, 0); }
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COLON(): TerminalNode[];
	public COLON(i: number): TerminalNode;
	public COLON(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COLON);
		} else {
			return this.getToken(Python3Parser.COLON, i);
		}
	}
	public suite(): SuiteContext[];
	public suite(i: number): SuiteContext;
	public suite(i?: number): SuiteContext | SuiteContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SuiteContext);
		} else {
			return this.getRuleContext(i, SuiteContext);
		}
	}
	public ELIF(): TerminalNode[];
	public ELIF(i: number): TerminalNode;
	public ELIF(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.ELIF);
		} else {
			return this.getToken(Python3Parser.ELIF, i);
		}
	}
	public ELSE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ELSE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_if_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterIf_stmt) {
			listener.enterIf_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitIf_stmt) {
			listener.exitIf_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitIf_stmt) {
			return visitor.visitIf_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class While_stmtContext extends ParserRuleContext {
	public WHILE(): TerminalNode { return this.getToken(Python3Parser.WHILE, 0); }
	public test(): TestContext {
		return this.getRuleContext(0, TestContext);
	}
	public COLON(): TerminalNode[];
	public COLON(i: number): TerminalNode;
	public COLON(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COLON);
		} else {
			return this.getToken(Python3Parser.COLON, i);
		}
	}
	public suite(): SuiteContext[];
	public suite(i: number): SuiteContext;
	public suite(i?: number): SuiteContext | SuiteContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SuiteContext);
		} else {
			return this.getRuleContext(i, SuiteContext);
		}
	}
	public ELSE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ELSE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_while_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterWhile_stmt) {
			listener.enterWhile_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitWhile_stmt) {
			listener.exitWhile_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitWhile_stmt) {
			return visitor.visitWhile_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class For_stmtContext extends ParserRuleContext {
	public FOR(): TerminalNode { return this.getToken(Python3Parser.FOR, 0); }
	public exprlist(): ExprlistContext {
		return this.getRuleContext(0, ExprlistContext);
	}
	public IN(): TerminalNode { return this.getToken(Python3Parser.IN, 0); }
	public testlist(): TestlistContext {
		return this.getRuleContext(0, TestlistContext);
	}
	public COLON(): TerminalNode[];
	public COLON(i: number): TerminalNode;
	public COLON(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COLON);
		} else {
			return this.getToken(Python3Parser.COLON, i);
		}
	}
	public suite(): SuiteContext[];
	public suite(i: number): SuiteContext;
	public suite(i?: number): SuiteContext | SuiteContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SuiteContext);
		} else {
			return this.getRuleContext(i, SuiteContext);
		}
	}
	public ELSE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ELSE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_for_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterFor_stmt) {
			listener.enterFor_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitFor_stmt) {
			listener.exitFor_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitFor_stmt) {
			return visitor.visitFor_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Try_stmtContext extends ParserRuleContext {
	public TRY(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.TRY, 0); }
	public COLON(): TerminalNode[];
	public COLON(i: number): TerminalNode;
	public COLON(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COLON);
		} else {
			return this.getToken(Python3Parser.COLON, i);
		}
	}
	public suite(): SuiteContext[];
	public suite(i: number): SuiteContext;
	public suite(i?: number): SuiteContext | SuiteContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SuiteContext);
		} else {
			return this.getRuleContext(i, SuiteContext);
		}
	}
	public FINALLY(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.FINALLY, 0); }
	public except_clause(): Except_clauseContext[];
	public except_clause(i: number): Except_clauseContext;
	public except_clause(i?: number): Except_clauseContext | Except_clauseContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Except_clauseContext);
		} else {
			return this.getRuleContext(i, Except_clauseContext);
		}
	}
	public ELSE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ELSE, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_try_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTry_stmt) {
			listener.enterTry_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTry_stmt) {
			listener.exitTry_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTry_stmt) {
			return visitor.visitTry_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class With_stmtContext extends ParserRuleContext {
	public WITH(): TerminalNode { return this.getToken(Python3Parser.WITH, 0); }
	public with_item(): With_itemContext[];
	public with_item(i: number): With_itemContext;
	public with_item(i?: number): With_itemContext | With_itemContext[] {
		if (i === undefined) {
			return this.getRuleContexts(With_itemContext);
		} else {
			return this.getRuleContext(i, With_itemContext);
		}
	}
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public suite(): SuiteContext {
		return this.getRuleContext(0, SuiteContext);
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_with_stmt; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterWith_stmt) {
			listener.enterWith_stmt(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitWith_stmt) {
			listener.exitWith_stmt(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitWith_stmt) {
			return visitor.visitWith_stmt(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class With_itemContext extends ParserRuleContext {
	public test(): TestContext {
		return this.getRuleContext(0, TestContext);
	}
	public AS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AS, 0); }
	public expr(): ExprContext | undefined {
		return this.tryGetRuleContext(0, ExprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_with_item; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterWith_item) {
			listener.enterWith_item(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitWith_item) {
			listener.exitWith_item(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitWith_item) {
			return visitor.visitWith_item(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Except_clauseContext extends ParserRuleContext {
	public EXCEPT(): TerminalNode { return this.getToken(Python3Parser.EXCEPT, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	public AS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AS, 0); }
	public NAME(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NAME, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_except_clause; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterExcept_clause) {
			listener.enterExcept_clause(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitExcept_clause) {
			listener.exitExcept_clause(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitExcept_clause) {
			return visitor.visitExcept_clause(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SuiteContext extends ParserRuleContext {
	public simple_stmt(): Simple_stmtContext | undefined {
		return this.tryGetRuleContext(0, Simple_stmtContext);
	}
	public NEWLINE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NEWLINE, 0); }
	public INDENT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.INDENT, 0); }
	public DEDENT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DEDENT, 0); }
	public stmt(): StmtContext[];
	public stmt(i: number): StmtContext;
	public stmt(i?: number): StmtContext | StmtContext[] {
		if (i === undefined) {
			return this.getRuleContexts(StmtContext);
		} else {
			return this.getRuleContext(i, StmtContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_suite; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSuite) {
			listener.enterSuite(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSuite) {
			listener.exitSuite(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSuite) {
			return visitor.visitSuite(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TestContext extends ParserRuleContext {
	public or_test(): Or_testContext[];
	public or_test(i: number): Or_testContext;
	public or_test(i?: number): Or_testContext | Or_testContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Or_testContext);
		} else {
			return this.getRuleContext(i, Or_testContext);
		}
	}
	public IF(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.IF, 0); }
	public ELSE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ELSE, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	public lambdef(): LambdefContext | undefined {
		return this.tryGetRuleContext(0, LambdefContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_test; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTest) {
			listener.enterTest(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTest) {
			listener.exitTest(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTest) {
			return visitor.visitTest(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Test_nocondContext extends ParserRuleContext {
	public or_test(): Or_testContext | undefined {
		return this.tryGetRuleContext(0, Or_testContext);
	}
	public lambdef_nocond(): Lambdef_nocondContext | undefined {
		return this.tryGetRuleContext(0, Lambdef_nocondContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_test_nocond; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTest_nocond) {
			listener.enterTest_nocond(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTest_nocond) {
			listener.exitTest_nocond(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTest_nocond) {
			return visitor.visitTest_nocond(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class LambdefContext extends ParserRuleContext {
	public LAMBDA(): TerminalNode { return this.getToken(Python3Parser.LAMBDA, 0); }
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public test(): TestContext {
		return this.getRuleContext(0, TestContext);
	}
	public varargslist(): VarargslistContext | undefined {
		return this.tryGetRuleContext(0, VarargslistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_lambdef; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterLambdef) {
			listener.enterLambdef(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitLambdef) {
			listener.exitLambdef(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitLambdef) {
			return visitor.visitLambdef(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Lambdef_nocondContext extends ParserRuleContext {
	public LAMBDA(): TerminalNode { return this.getToken(Python3Parser.LAMBDA, 0); }
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public test_nocond(): Test_nocondContext {
		return this.getRuleContext(0, Test_nocondContext);
	}
	public varargslist(): VarargslistContext | undefined {
		return this.tryGetRuleContext(0, VarargslistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_lambdef_nocond; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterLambdef_nocond) {
			listener.enterLambdef_nocond(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitLambdef_nocond) {
			listener.exitLambdef_nocond(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitLambdef_nocond) {
			return visitor.visitLambdef_nocond(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Or_testContext extends ParserRuleContext {
	public and_test(): And_testContext[];
	public and_test(i: number): And_testContext;
	public and_test(i?: number): And_testContext | And_testContext[] {
		if (i === undefined) {
			return this.getRuleContexts(And_testContext);
		} else {
			return this.getRuleContext(i, And_testContext);
		}
	}
	public OR(): TerminalNode[];
	public OR(i: number): TerminalNode;
	public OR(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.OR);
		} else {
			return this.getToken(Python3Parser.OR, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_or_test; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterOr_test) {
			listener.enterOr_test(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitOr_test) {
			listener.exitOr_test(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitOr_test) {
			return visitor.visitOr_test(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class And_testContext extends ParserRuleContext {
	public not_test(): Not_testContext[];
	public not_test(i: number): Not_testContext;
	public not_test(i?: number): Not_testContext | Not_testContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Not_testContext);
		} else {
			return this.getRuleContext(i, Not_testContext);
		}
	}
	public AND(): TerminalNode[];
	public AND(i: number): TerminalNode;
	public AND(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.AND);
		} else {
			return this.getToken(Python3Parser.AND, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_and_test; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAnd_test) {
			listener.enterAnd_test(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAnd_test) {
			listener.exitAnd_test(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAnd_test) {
			return visitor.visitAnd_test(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Not_testContext extends ParserRuleContext {
	public NOT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NOT, 0); }
	public not_test(): Not_testContext | undefined {
		return this.tryGetRuleContext(0, Not_testContext);
	}
	public comparison(): ComparisonContext | undefined {
		return this.tryGetRuleContext(0, ComparisonContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_not_test; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterNot_test) {
			listener.enterNot_test(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitNot_test) {
			listener.exitNot_test(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitNot_test) {
			return visitor.visitNot_test(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ComparisonContext extends ParserRuleContext {
	public expr(): ExprContext[];
	public expr(i: number): ExprContext;
	public expr(i?: number): ExprContext | ExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprContext);
		} else {
			return this.getRuleContext(i, ExprContext);
		}
	}
	public comp_op(): Comp_opContext[];
	public comp_op(i: number): Comp_opContext;
	public comp_op(i?: number): Comp_opContext | Comp_opContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Comp_opContext);
		} else {
			return this.getRuleContext(i, Comp_opContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_comparison; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterComparison) {
			listener.enterComparison(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitComparison) {
			listener.exitComparison(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitComparison) {
			return visitor.visitComparison(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Comp_opContext extends ParserRuleContext {
	public LESS_THAN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.LESS_THAN, 0); }
	public GREATER_THAN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.GREATER_THAN, 0); }
	public EQUALS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.EQUALS, 0); }
	public GT_EQ(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.GT_EQ, 0); }
	public LT_EQ(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.LT_EQ, 0); }
	public NOT_EQ_1(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NOT_EQ_1, 0); }
	public NOT_EQ_2(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NOT_EQ_2, 0); }
	public IN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.IN, 0); }
	public NOT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NOT, 0); }
	public IS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.IS, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_comp_op; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterComp_op) {
			listener.enterComp_op(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitComp_op) {
			listener.exitComp_op(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitComp_op) {
			return visitor.visitComp_op(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Star_exprContext extends ParserRuleContext {
	public STAR(): TerminalNode { return this.getToken(Python3Parser.STAR, 0); }
	public expr(): ExprContext {
		return this.getRuleContext(0, ExprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_star_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterStar_expr) {
			listener.enterStar_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitStar_expr) {
			listener.exitStar_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitStar_expr) {
			return visitor.visitStar_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ExprContext extends ParserRuleContext {
	public xor_expr(): Xor_exprContext[];
	public xor_expr(i: number): Xor_exprContext;
	public xor_expr(i?: number): Xor_exprContext | Xor_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Xor_exprContext);
		} else {
			return this.getRuleContext(i, Xor_exprContext);
		}
	}
	public OR_OP(): TerminalNode[];
	public OR_OP(i: number): TerminalNode;
	public OR_OP(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.OR_OP);
		} else {
			return this.getToken(Python3Parser.OR_OP, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterExpr) {
			listener.enterExpr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitExpr) {
			listener.exitExpr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitExpr) {
			return visitor.visitExpr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Xor_exprContext extends ParserRuleContext {
	public and_expr(): And_exprContext[];
	public and_expr(i: number): And_exprContext;
	public and_expr(i?: number): And_exprContext | And_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(And_exprContext);
		} else {
			return this.getRuleContext(i, And_exprContext);
		}
	}
	public XOR(): TerminalNode[];
	public XOR(i: number): TerminalNode;
	public XOR(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.XOR);
		} else {
			return this.getToken(Python3Parser.XOR, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_xor_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterXor_expr) {
			listener.enterXor_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitXor_expr) {
			listener.exitXor_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitXor_expr) {
			return visitor.visitXor_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class And_exprContext extends ParserRuleContext {
	public shift_expr(): Shift_exprContext[];
	public shift_expr(i: number): Shift_exprContext;
	public shift_expr(i?: number): Shift_exprContext | Shift_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Shift_exprContext);
		} else {
			return this.getRuleContext(i, Shift_exprContext);
		}
	}
	public AND_OP(): TerminalNode[];
	public AND_OP(i: number): TerminalNode;
	public AND_OP(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.AND_OP);
		} else {
			return this.getToken(Python3Parser.AND_OP, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_and_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAnd_expr) {
			listener.enterAnd_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAnd_expr) {
			listener.exitAnd_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAnd_expr) {
			return visitor.visitAnd_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Shift_exprContext extends ParserRuleContext {
	public arith_expr(): Arith_exprContext[];
	public arith_expr(i: number): Arith_exprContext;
	public arith_expr(i?: number): Arith_exprContext | Arith_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Arith_exprContext);
		} else {
			return this.getRuleContext(i, Arith_exprContext);
		}
	}
	public LEFT_SHIFT(): TerminalNode[];
	public LEFT_SHIFT(i: number): TerminalNode;
	public LEFT_SHIFT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.LEFT_SHIFT);
		} else {
			return this.getToken(Python3Parser.LEFT_SHIFT, i);
		}
	}
	public RIGHT_SHIFT(): TerminalNode[];
	public RIGHT_SHIFT(i: number): TerminalNode;
	public RIGHT_SHIFT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.RIGHT_SHIFT);
		} else {
			return this.getToken(Python3Parser.RIGHT_SHIFT, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_shift_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterShift_expr) {
			listener.enterShift_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitShift_expr) {
			listener.exitShift_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitShift_expr) {
			return visitor.visitShift_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Arith_exprContext extends ParserRuleContext {
	public term(): TermContext[];
	public term(i: number): TermContext;
	public term(i?: number): TermContext | TermContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TermContext);
		} else {
			return this.getRuleContext(i, TermContext);
		}
	}
	public ADD(): TerminalNode[];
	public ADD(i: number): TerminalNode;
	public ADD(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.ADD);
		} else {
			return this.getToken(Python3Parser.ADD, i);
		}
	}
	public MINUS(): TerminalNode[];
	public MINUS(i: number): TerminalNode;
	public MINUS(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.MINUS);
		} else {
			return this.getToken(Python3Parser.MINUS, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_arith_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterArith_expr) {
			listener.enterArith_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitArith_expr) {
			listener.exitArith_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitArith_expr) {
			return visitor.visitArith_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TermContext extends ParserRuleContext {
	public factor(): FactorContext[];
	public factor(i: number): FactorContext;
	public factor(i?: number): FactorContext | FactorContext[] {
		if (i === undefined) {
			return this.getRuleContexts(FactorContext);
		} else {
			return this.getRuleContext(i, FactorContext);
		}
	}
	public STAR(): TerminalNode[];
	public STAR(i: number): TerminalNode;
	public STAR(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.STAR);
		} else {
			return this.getToken(Python3Parser.STAR, i);
		}
	}
	public AT(): TerminalNode[];
	public AT(i: number): TerminalNode;
	public AT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.AT);
		} else {
			return this.getToken(Python3Parser.AT, i);
		}
	}
	public DIV(): TerminalNode[];
	public DIV(i: number): TerminalNode;
	public DIV(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.DIV);
		} else {
			return this.getToken(Python3Parser.DIV, i);
		}
	}
	public MOD(): TerminalNode[];
	public MOD(i: number): TerminalNode;
	public MOD(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.MOD);
		} else {
			return this.getToken(Python3Parser.MOD, i);
		}
	}
	public IDIV(): TerminalNode[];
	public IDIV(i: number): TerminalNode;
	public IDIV(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.IDIV);
		} else {
			return this.getToken(Python3Parser.IDIV, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_term; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTerm) {
			listener.enterTerm(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTerm) {
			listener.exitTerm(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTerm) {
			return visitor.visitTerm(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FactorContext extends ParserRuleContext {
	public factor(): FactorContext | undefined {
		return this.tryGetRuleContext(0, FactorContext);
	}
	public ADD(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ADD, 0); }
	public MINUS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.MINUS, 0); }
	public NOT_OP(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NOT_OP, 0); }
	public power(): PowerContext | undefined {
		return this.tryGetRuleContext(0, PowerContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_factor; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterFactor) {
			listener.enterFactor(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitFactor) {
			listener.exitFactor(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitFactor) {
			return visitor.visitFactor(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class PowerContext extends ParserRuleContext {
	public atom_expr(): Atom_exprContext {
		return this.getRuleContext(0, Atom_exprContext);
	}
	public POWER(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.POWER, 0); }
	public factor(): FactorContext | undefined {
		return this.tryGetRuleContext(0, FactorContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_power; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterPower) {
			listener.enterPower(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitPower) {
			listener.exitPower(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitPower) {
			return visitor.visitPower(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Atom_exprContext extends ParserRuleContext {
	public atom(): AtomContext {
		return this.getRuleContext(0, AtomContext);
	}
	public AWAIT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.AWAIT, 0); }
	public trailer(): TrailerContext[];
	public trailer(i: number): TrailerContext;
	public trailer(i?: number): TrailerContext | TrailerContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TrailerContext);
		} else {
			return this.getRuleContext(i, TrailerContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_atom_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAtom_expr) {
			listener.enterAtom_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAtom_expr) {
			listener.exitAtom_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAtom_expr) {
			return visitor.visitAtom_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AtomContext extends ParserRuleContext {
	public OPEN_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_PAREN, 0); }
	public CLOSE_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_PAREN, 0); }
	public OPEN_BRACK(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_BRACK, 0); }
	public CLOSE_BRACK(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_BRACK, 0); }
	public OPEN_BRACE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_BRACE, 0); }
	public CLOSE_BRACE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_BRACE, 0); }
	public NAME(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NAME, 0); }
	public NUMBER(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NUMBER, 0); }
	public ELLIPSIS(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ELLIPSIS, 0); }
	public NONE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NONE, 0); }
	public TRUE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.TRUE, 0); }
	public FALSE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.FALSE, 0); }
	public yield_expr(): Yield_exprContext | undefined {
		return this.tryGetRuleContext(0, Yield_exprContext);
	}
	public testlist_comp(): Testlist_compContext | undefined {
		return this.tryGetRuleContext(0, Testlist_compContext);
	}
	public dictorsetmaker(): DictorsetmakerContext | undefined {
		return this.tryGetRuleContext(0, DictorsetmakerContext);
	}
	public string_template(): String_templateContext[];
	public string_template(i: number): String_templateContext;
	public string_template(i?: number): String_templateContext | String_templateContext[] {
		if (i === undefined) {
			return this.getRuleContexts(String_templateContext);
		} else {
			return this.getRuleContext(i, String_templateContext);
		}
	}
	public STRING(): TerminalNode[];
	public STRING(i: number): TerminalNode;
	public STRING(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.STRING);
		} else {
			return this.getToken(Python3Parser.STRING, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_atom; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterAtom) {
			listener.enterAtom(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitAtom) {
			listener.exitAtom(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitAtom) {
			return visitor.visitAtom(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Testlist_compContext extends ParserRuleContext {
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public star_expr(): Star_exprContext[];
	public star_expr(i: number): Star_exprContext;
	public star_expr(i?: number): Star_exprContext | Star_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Star_exprContext);
		} else {
			return this.getRuleContext(i, Star_exprContext);
		}
	}
	public comp_for(): Comp_forContext | undefined {
		return this.tryGetRuleContext(0, Comp_forContext);
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_testlist_comp; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTestlist_comp) {
			listener.enterTestlist_comp(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTestlist_comp) {
			listener.exitTestlist_comp(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTestlist_comp) {
			return visitor.visitTestlist_comp(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TrailerContext extends ParserRuleContext {
	public callArguments(): CallArgumentsContext | undefined {
		return this.tryGetRuleContext(0, CallArgumentsContext);
	}
	public OPEN_BRACK(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_BRACK, 0); }
	public subscriptlist(): SubscriptlistContext | undefined {
		return this.tryGetRuleContext(0, SubscriptlistContext);
	}
	public CLOSE_BRACK(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_BRACK, 0); }
	public DOT(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DOT, 0); }
	public NAME(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.NAME, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_trailer; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTrailer) {
			listener.enterTrailer(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTrailer) {
			listener.exitTrailer(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTrailer) {
			return visitor.visitTrailer(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SubscriptlistContext extends ParserRuleContext {
	public subscript(): SubscriptContext[];
	public subscript(i: number): SubscriptContext;
	public subscript(i?: number): SubscriptContext | SubscriptContext[] {
		if (i === undefined) {
			return this.getRuleContexts(SubscriptContext);
		} else {
			return this.getRuleContext(i, SubscriptContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_subscriptlist; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSubscriptlist) {
			listener.enterSubscriptlist(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSubscriptlist) {
			listener.exitSubscriptlist(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSubscriptlist) {
			return visitor.visitSubscriptlist(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SubscriptContext extends ParserRuleContext {
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COLON(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.COLON, 0); }
	public sliceop(): SliceopContext | undefined {
		return this.tryGetRuleContext(0, SliceopContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_subscript; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSubscript) {
			listener.enterSubscript(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSubscript) {
			listener.exitSubscript(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSubscript) {
			return visitor.visitSubscript(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SliceopContext extends ParserRuleContext {
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_sliceop; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSliceop) {
			listener.enterSliceop(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSliceop) {
			listener.exitSliceop(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSliceop) {
			return visitor.visitSliceop(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ExprlistContext extends ParserRuleContext {
	public expr(): ExprContext[];
	public expr(i: number): ExprContext;
	public expr(i?: number): ExprContext | ExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprContext);
		} else {
			return this.getRuleContext(i, ExprContext);
		}
	}
	public star_expr(): Star_exprContext[];
	public star_expr(i: number): Star_exprContext;
	public star_expr(i?: number): Star_exprContext | Star_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Star_exprContext);
		} else {
			return this.getRuleContext(i, Star_exprContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_exprlist; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterExprlist) {
			listener.enterExprlist(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitExprlist) {
			listener.exitExprlist(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitExprlist) {
			return visitor.visitExprlist(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class TestlistContext extends ParserRuleContext {
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_testlist; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterTestlist) {
			listener.enterTestlist(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitTestlist) {
			listener.exitTestlist(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitTestlist) {
			return visitor.visitTestlist(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class DictorsetmakerContext extends ParserRuleContext {
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public COLON(): TerminalNode[];
	public COLON(i: number): TerminalNode;
	public COLON(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COLON);
		} else {
			return this.getToken(Python3Parser.COLON, i);
		}
	}
	public POWER(): TerminalNode[];
	public POWER(i: number): TerminalNode;
	public POWER(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.POWER);
		} else {
			return this.getToken(Python3Parser.POWER, i);
		}
	}
	public expr(): ExprContext[];
	public expr(i: number): ExprContext;
	public expr(i?: number): ExprContext | ExprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExprContext);
		} else {
			return this.getRuleContext(i, ExprContext);
		}
	}
	public comp_for(): Comp_forContext | undefined {
		return this.tryGetRuleContext(0, Comp_forContext);
	}
	public star_expr(): Star_exprContext[];
	public star_expr(i: number): Star_exprContext;
	public star_expr(i?: number): Star_exprContext | Star_exprContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Star_exprContext);
		} else {
			return this.getRuleContext(i, Star_exprContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_dictorsetmaker; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDictorsetmaker) {
			listener.enterDictorsetmaker(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDictorsetmaker) {
			listener.exitDictorsetmaker(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDictorsetmaker) {
			return visitor.visitDictorsetmaker(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ClassdefContext extends ParserRuleContext {
	public CLASS(): TerminalNode { return this.getToken(Python3Parser.CLASS, 0); }
	public NAME(): TerminalNode { return this.getToken(Python3Parser.NAME, 0); }
	public COLON(): TerminalNode { return this.getToken(Python3Parser.COLON, 0); }
	public suite(): SuiteContext {
		return this.getRuleContext(0, SuiteContext);
	}
	public OPEN_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_PAREN, 0); }
	public CLOSE_PAREN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.CLOSE_PAREN, 0); }
	public arglist(): ArglistContext | undefined {
		return this.tryGetRuleContext(0, ArglistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_classdef; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterClassdef) {
			listener.enterClassdef(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitClassdef) {
			listener.exitClassdef(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitClassdef) {
			return visitor.visitClassdef(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class CallArgumentsContext extends ParserRuleContext {
	public OPEN_PAREN(): TerminalNode { return this.getToken(Python3Parser.OPEN_PAREN, 0); }
	public CLOSE_PAREN(): TerminalNode { return this.getToken(Python3Parser.CLOSE_PAREN, 0); }
	public arglist(): ArglistContext | undefined {
		return this.tryGetRuleContext(0, ArglistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_callArguments; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterCallArguments) {
			listener.enterCallArguments(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitCallArguments) {
			listener.exitCallArguments(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitCallArguments) {
			return visitor.visitCallArguments(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ArglistContext extends ParserRuleContext {
	public argument(): ArgumentContext[];
	public argument(i: number): ArgumentContext;
	public argument(i?: number): ArgumentContext | ArgumentContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ArgumentContext);
		} else {
			return this.getRuleContext(i, ArgumentContext);
		}
	}
	public COMMA(): TerminalNode[];
	public COMMA(i: number): TerminalNode;
	public COMMA(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(Python3Parser.COMMA);
		} else {
			return this.getToken(Python3Parser.COMMA, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_arglist; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterArglist) {
			listener.enterArglist(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitArglist) {
			listener.exitArglist(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitArglist) {
			return visitor.visitArglist(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ArgumentContext extends ParserRuleContext {
	public test(): TestContext[];
	public test(i: number): TestContext;
	public test(i?: number): TestContext | TestContext[] {
		if (i === undefined) {
			return this.getRuleContexts(TestContext);
		} else {
			return this.getRuleContext(i, TestContext);
		}
	}
	public ASSIGN(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ASSIGN, 0); }
	public POWER(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.POWER, 0); }
	public STAR(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.STAR, 0); }
	public comp_for(): Comp_forContext | undefined {
		return this.tryGetRuleContext(0, Comp_forContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_argument; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterArgument) {
			listener.enterArgument(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitArgument) {
			listener.exitArgument(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitArgument) {
			return visitor.visitArgument(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Comp_iterContext extends ParserRuleContext {
	public comp_for(): Comp_forContext | undefined {
		return this.tryGetRuleContext(0, Comp_forContext);
	}
	public comp_if(): Comp_ifContext | undefined {
		return this.tryGetRuleContext(0, Comp_ifContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_comp_iter; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterComp_iter) {
			listener.enterComp_iter(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitComp_iter) {
			listener.exitComp_iter(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitComp_iter) {
			return visitor.visitComp_iter(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Comp_forContext extends ParserRuleContext {
	public FOR(): TerminalNode { return this.getToken(Python3Parser.FOR, 0); }
	public exprlist(): ExprlistContext {
		return this.getRuleContext(0, ExprlistContext);
	}
	public IN(): TerminalNode { return this.getToken(Python3Parser.IN, 0); }
	public or_test(): Or_testContext {
		return this.getRuleContext(0, Or_testContext);
	}
	public ASYNC(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.ASYNC, 0); }
	public comp_iter(): Comp_iterContext | undefined {
		return this.tryGetRuleContext(0, Comp_iterContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_comp_for; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterComp_for) {
			listener.enterComp_for(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitComp_for) {
			listener.exitComp_for(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitComp_for) {
			return visitor.visitComp_for(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Comp_ifContext extends ParserRuleContext {
	public IF(): TerminalNode { return this.getToken(Python3Parser.IF, 0); }
	public test_nocond(): Test_nocondContext {
		return this.getRuleContext(0, Test_nocondContext);
	}
	public comp_iter(): Comp_iterContext | undefined {
		return this.tryGetRuleContext(0, Comp_iterContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_comp_if; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterComp_if) {
			listener.enterComp_if(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitComp_if) {
			listener.exitComp_if(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitComp_if) {
			return visitor.visitComp_if(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Encoding_declContext extends ParserRuleContext {
	public NAME(): TerminalNode { return this.getToken(Python3Parser.NAME, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_encoding_decl; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterEncoding_decl) {
			listener.enterEncoding_decl(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitEncoding_decl) {
			listener.exitEncoding_decl(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitEncoding_decl) {
			return visitor.visitEncoding_decl(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Yield_exprContext extends ParserRuleContext {
	public YIELD(): TerminalNode { return this.getToken(Python3Parser.YIELD, 0); }
	public yield_arg(): Yield_argContext | undefined {
		return this.tryGetRuleContext(0, Yield_argContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_yield_expr; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterYield_expr) {
			listener.enterYield_expr(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitYield_expr) {
			listener.exitYield_expr(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitYield_expr) {
			return visitor.visitYield_expr(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Yield_argContext extends ParserRuleContext {
	public FROM(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.FROM, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	public testlist(): TestlistContext | undefined {
		return this.tryGetRuleContext(0, TestlistContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_yield_arg; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterYield_arg) {
			listener.enterYield_arg(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitYield_arg) {
			listener.exitYield_arg(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitYield_arg) {
			return visitor.visitYield_arg(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class String_templateContext extends ParserRuleContext {
	public SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START, 0); }
	public SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END, 0); }
	public single_string_template_atom(): Single_string_template_atomContext[];
	public single_string_template_atom(i: number): Single_string_template_atomContext;
	public single_string_template_atom(i?: number): Single_string_template_atomContext | Single_string_template_atomContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Single_string_template_atomContext);
		} else {
			return this.getRuleContext(i, Single_string_template_atomContext);
		}
	}
	public SINGLE_QUOTE_LONG_TEMPLATE_STRING_START(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START, 0); }
	public SINGLE_QUOTE_LONG_TEMPLATE_STRING_END(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_END, 0); }
	public DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START, 0); }
	public DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END, 0); }
	public double_string_template_atom(): Double_string_template_atomContext[];
	public double_string_template_atom(i: number): Double_string_template_atomContext;
	public double_string_template_atom(i?: number): Double_string_template_atomContext | Double_string_template_atomContext[] {
		if (i === undefined) {
			return this.getRuleContexts(Double_string_template_atomContext);
		} else {
			return this.getRuleContext(i, Double_string_template_atomContext);
		}
	}
	public DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START, 0); }
	public DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_string_template; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterString_template) {
			listener.enterString_template(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitString_template) {
			listener.exitString_template(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitString_template) {
			return visitor.visitString_template(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Single_string_template_atomContext extends ParserRuleContext {
	public SINGLE_QUOTE_STRING_ATOM(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.SINGLE_QUOTE_STRING_ATOM, 0); }
	public OPEN_BRACE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_BRACE, 0); }
	public TEMPLATE_CLOSE_BRACE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.TEMPLATE_CLOSE_BRACE, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	public star_expr(): Star_exprContext | undefined {
		return this.tryGetRuleContext(0, Star_exprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_single_string_template_atom; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterSingle_string_template_atom) {
			listener.enterSingle_string_template_atom(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitSingle_string_template_atom) {
			listener.exitSingle_string_template_atom(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitSingle_string_template_atom) {
			return visitor.visitSingle_string_template_atom(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class Double_string_template_atomContext extends ParserRuleContext {
	public DOUBLE_QUOTE_STRING_ATOM(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.DOUBLE_QUOTE_STRING_ATOM, 0); }
	public OPEN_BRACE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.OPEN_BRACE, 0); }
	public TEMPLATE_CLOSE_BRACE(): TerminalNode | undefined { return this.tryGetToken(Python3Parser.TEMPLATE_CLOSE_BRACE, 0); }
	public test(): TestContext | undefined {
		return this.tryGetRuleContext(0, TestContext);
	}
	public star_expr(): Star_exprContext | undefined {
		return this.tryGetRuleContext(0, Star_exprContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return Python3Parser.RULE_double_string_template_atom; }
	// @Override
	public enterRule(listener: Python3ParserListener): void {
		if (listener.enterDouble_string_template_atom) {
			listener.enterDouble_string_template_atom(this);
		}
	}
	// @Override
	public exitRule(listener: Python3ParserListener): void {
		if (listener.exitDouble_string_template_atom) {
			listener.exitDouble_string_template_atom(this);
		}
	}
	// @Override
	public accept<Result>(visitor: Python3ParserVisitor<Result>): Result {
		if (visitor.visitDouble_string_template_atom) {
			return visitor.visitDouble_string_template_atom(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


