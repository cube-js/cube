// Generated from src/parser/Python3Parser.g4 by ANTLR 4.13.2
// noinspection ES6UnusedImports,JSUnusedGlobalSymbols,JSUnusedLocalSymbols
// @ts-nocheck

import {
	ATN,
	ATNDeserializer, DecisionState, DFA, FailedPredicateException,
	RecognitionException, NoViableAltException, BailErrorStrategy,
	Parser, ParserATNSimulator,
	RuleContext, ParserRuleContext, PredictionMode, PredictionContextCache,
	TerminalNode, RuleNode,
	Token, TokenStream,
	Interval, IntervalSet
} from 'antlr4';
import Python3ParserListener from "./Python3ParserListener.js";
import Python3ParserVisitor from "./Python3ParserVisitor.js";

// for running tests with parameters, TODO: discuss strategy for typed parameters in CI
// eslint-disable-next-line no-unused-vars
type int = number;

export default class Python3Parser extends Parser {
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
	public static override readonly EOF = Token.EOF;
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
	public get grammarFileName(): string { return "Python3Parser.g4"; }
	public get literalNames(): (string | null)[] { return Python3Parser.literalNames; }
	public get symbolicNames(): (string | null)[] { return Python3Parser.symbolicNames; }
	public get ruleNames(): string[] { return Python3Parser.ruleNames; }
	public get serializedATN(): number[] { return Python3Parser._serializedATN; }

	protected createFailedPredicateException(predicate?: string, message?: string): FailedPredicateException {
		return new FailedPredicateException(this, predicate, message);
	}

	constructor(input: TokenStream) {
		super(input);
		this._interp = new ParserATNSimulator(this, Python3Parser._ATN, Python3Parser.DecisionsToDFA, new PredictionContextCache());
	}
	// @RuleVersion(0)
	public file_input(): File_inputContext {
		let localctx: File_inputContext = new File_inputContext(this, this._ctx, this.state);
		this.enterRule(localctx, 0, Python3Parser.RULE_file_input);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 184;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while ((((_la) & ~0x1F) === 0 && ((1 << _la) & 718241272) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 117473277) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 8401281) !== 0)) {
				{
				this.state = 182;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case 45:
					{
					this.state = 180;
					this.match(Python3Parser.NEWLINE);
					}
					break;
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 10:
				case 11:
				case 12:
				case 13:
				case 14:
				case 16:
				case 17:
				case 18:
				case 19:
				case 22:
				case 23:
				case 25:
				case 27:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 37:
				case 38:
				case 39:
				case 40:
				case 41:
				case 42:
				case 43:
				case 44:
				case 46:
				case 56:
				case 57:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
				case 88:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public single_input(): Single_inputContext {
		let localctx: Single_inputContext = new Single_inputContext(this, this._ctx, this.state);
		this.enterRule(localctx, 2, Python3Parser.RULE_single_input);
		try {
			this.state = 194;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 45:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 189;
				this.match(Python3Parser.NEWLINE);
				}
				break;
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 11:
			case 12:
			case 13:
			case 14:
			case 16:
			case 17:
			case 18:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 38:
			case 39:
			case 40:
			case 41:
			case 42:
			case 44:
			case 46:
			case 56:
			case 57:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 190;
				this.simple_stmt();
				}
				break;
			case 10:
			case 19:
			case 22:
			case 23:
			case 25:
			case 27:
			case 37:
			case 43:
			case 88:
				this.enterOuterAlt(localctx, 3);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public eval_input(): Eval_inputContext {
		let localctx: Eval_inputContext = new Eval_inputContext(this, this._ctx, this.state);
		this.enterRule(localctx, 4, Python3Parser.RULE_eval_input);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 196;
			this.testlist();
			this.state = 200;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===45) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public decorator(): DecoratorContext {
		let localctx: DecoratorContext = new DecoratorContext(this, this._ctx, this.state);
		this.enterRule(localctx, 6, Python3Parser.RULE_decorator);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 205;
			this.match(Python3Parser.AT);
			this.state = 206;
			this.dotted_name();
			this.state = 212;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===58) {
				{
				this.state = 207;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 209;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 2264944669) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public decorators(): DecoratorsContext {
		let localctx: DecoratorsContext = new DecoratorsContext(this, this._ctx, this.state);
		this.enterRule(localctx, 8, Python3Parser.RULE_decorators);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
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
			} while (_la===88);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public decorated(): DecoratedContext {
		let localctx: DecoratedContext = new DecoratedContext(this, this._ctx, this.state);
		this.enterRule(localctx, 10, Python3Parser.RULE_decorated);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 221;
			this.decorators();
			this.state = 225;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 37:
				{
				this.state = 222;
				this.classdef();
				}
				break;
			case 10:
				{
				this.state = 223;
				this.funcdef();
				}
				break;
			case 43:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public async_funcdef(): Async_funcdefContext {
		let localctx: Async_funcdefContext = new Async_funcdefContext(this, this._ctx, this.state);
		this.enterRule(localctx, 12, Python3Parser.RULE_async_funcdef);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 227;
			this.match(Python3Parser.ASYNC);
			this.state = 228;
			this.funcdef();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public funcdef(): FuncdefContext {
		let localctx: FuncdefContext = new FuncdefContext(this, this._ctx, this.state);
		this.enterRule(localctx, 14, Python3Parser.RULE_funcdef);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
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
			if (_la===89) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public parameters(): ParametersContext {
		let localctx: ParametersContext = new ParametersContext(this, this._ctx, this.state);
		this.enterRule(localctx, 16, Python3Parser.RULE_parameters);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 240;
			this.match(Python3Parser.OPEN_PAREN);
			this.state = 242;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 46)) & ~0x1F) === 0 && ((1 << (_la - 46)) & 133121) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public typedargslist(): TypedargslistContext {
		let localctx: TypedargslistContext = new TypedargslistContext(this, this._ctx, this.state);
		this.enterRule(localctx, 18, Python3Parser.RULE_typedargslist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 327;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 46:
				{
				this.state = 246;
				this.tfpdef();
				this.state = 249;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===64) {
					{
					this.state = 247;
					this.match(Python3Parser.ASSIGN);
					this.state = 248;
					this.test();
					}
				}

				this.state = 259;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 12, this._ctx);
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
						if (_la===64) {
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
					_alt = this._interp.adaptivePredict(this._input, 12, this._ctx);
				}
				this.state = 295;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
					{
					this.state = 262;
					this.match(Python3Parser.COMMA);
					this.state = 293;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case 57:
						{
						this.state = 263;
						this.match(Python3Parser.STAR);
						this.state = 265;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===46) {
							{
							this.state = 264;
							this.tfpdef();
							}
						}

						this.state = 275;
						this._errHandler.sync(this);
						_alt = this._interp.adaptivePredict(this._input, 15, this._ctx);
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
								if (_la===64) {
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
							_alt = this._interp.adaptivePredict(this._input, 15, this._ctx);
						}
						this.state = 286;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===60) {
							{
							this.state = 278;
							this.match(Python3Parser.COMMA);
							this.state = 284;
							this._errHandler.sync(this);
							_la = this._input.LA(1);
							if (_la===63) {
								{
								this.state = 279;
								this.match(Python3Parser.POWER);
								this.state = 280;
								this.tfpdef();
								this.state = 282;
								this._errHandler.sync(this);
								_la = this._input.LA(1);
								if (_la===60) {
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
					case 63:
						{
						this.state = 288;
						this.match(Python3Parser.POWER);
						this.state = 289;
						this.tfpdef();
						this.state = 291;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===60) {
							{
							this.state = 290;
							this.match(Python3Parser.COMMA);
							}
						}

						}
						break;
					case 59:
						break;
					default:
						break;
					}
					}
				}

				}
				break;
			case 57:
				{
				this.state = 297;
				this.match(Python3Parser.STAR);
				this.state = 299;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===46) {
					{
					this.state = 298;
					this.tfpdef();
					}
				}

				this.state = 309;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 24, this._ctx);
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
						if (_la===64) {
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
					_alt = this._interp.adaptivePredict(this._input, 24, this._ctx);
				}
				this.state = 320;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
					{
					this.state = 312;
					this.match(Python3Parser.COMMA);
					this.state = 318;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la===63) {
						{
						this.state = 313;
						this.match(Python3Parser.POWER);
						this.state = 314;
						this.tfpdef();
						this.state = 316;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===60) {
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
			case 63:
				{
				this.state = 322;
				this.match(Python3Parser.POWER);
				this.state = 323;
				this.tfpdef();
				this.state = 325;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public tfpdef(): TfpdefContext {
		let localctx: TfpdefContext = new TfpdefContext(this, this._ctx, this.state);
		this.enterRule(localctx, 20, Python3Parser.RULE_tfpdef);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 329;
			this.match(Python3Parser.NAME);
			this.state = 332;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===61) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public varargslist(): VarargslistContext {
		let localctx: VarargslistContext = new VarargslistContext(this, this._ctx, this.state);
		this.enterRule(localctx, 22, Python3Parser.RULE_varargslist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 415;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 46:
				{
				this.state = 334;
				this.vfpdef();
				this.state = 337;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===64) {
					{
					this.state = 335;
					this.match(Python3Parser.ASSIGN);
					this.state = 336;
					this.test();
					}
				}

				this.state = 347;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 33, this._ctx);
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
						if (_la===64) {
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
					_alt = this._interp.adaptivePredict(this._input, 33, this._ctx);
				}
				this.state = 383;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
					{
					this.state = 350;
					this.match(Python3Parser.COMMA);
					this.state = 381;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case 57:
						{
						this.state = 351;
						this.match(Python3Parser.STAR);
						this.state = 353;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===46) {
							{
							this.state = 352;
							this.vfpdef();
							}
						}

						this.state = 363;
						this._errHandler.sync(this);
						_alt = this._interp.adaptivePredict(this._input, 36, this._ctx);
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
								if (_la===64) {
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
							_alt = this._interp.adaptivePredict(this._input, 36, this._ctx);
						}
						this.state = 374;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===60) {
							{
							this.state = 366;
							this.match(Python3Parser.COMMA);
							this.state = 372;
							this._errHandler.sync(this);
							_la = this._input.LA(1);
							if (_la===63) {
								{
								this.state = 367;
								this.match(Python3Parser.POWER);
								this.state = 368;
								this.vfpdef();
								this.state = 370;
								this._errHandler.sync(this);
								_la = this._input.LA(1);
								if (_la===60) {
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
					case 63:
						{
						this.state = 376;
						this.match(Python3Parser.POWER);
						this.state = 377;
						this.vfpdef();
						this.state = 379;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===60) {
							{
							this.state = 378;
							this.match(Python3Parser.COMMA);
							}
						}

						}
						break;
					case 61:
						break;
					default:
						break;
					}
					}
				}

				}
				break;
			case 57:
				{
				this.state = 385;
				this.match(Python3Parser.STAR);
				this.state = 387;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===46) {
					{
					this.state = 386;
					this.vfpdef();
					}
				}

				this.state = 397;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 45, this._ctx);
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
						if (_la===64) {
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
					_alt = this._interp.adaptivePredict(this._input, 45, this._ctx);
				}
				this.state = 408;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
					{
					this.state = 400;
					this.match(Python3Parser.COMMA);
					this.state = 406;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la===63) {
						{
						this.state = 401;
						this.match(Python3Parser.POWER);
						this.state = 402;
						this.vfpdef();
						this.state = 404;
						this._errHandler.sync(this);
						_la = this._input.LA(1);
						if (_la===60) {
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
			case 63:
				{
				this.state = 410;
				this.match(Python3Parser.POWER);
				this.state = 411;
				this.vfpdef();
				this.state = 413;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public vfpdef(): VfpdefContext {
		let localctx: VfpdefContext = new VfpdefContext(this, this._ctx, this.state);
		this.enterRule(localctx, 24, Python3Parser.RULE_vfpdef);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 417;
			this.match(Python3Parser.NAME);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public stmt(): StmtContext {
		let localctx: StmtContext = new StmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 26, Python3Parser.RULE_stmt);
		try {
			this.state = 421;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 11:
			case 12:
			case 13:
			case 14:
			case 16:
			case 17:
			case 18:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 38:
			case 39:
			case 40:
			case 41:
			case 42:
			case 44:
			case 46:
			case 56:
			case 57:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 419;
				this.simple_stmt();
				}
				break;
			case 10:
			case 19:
			case 22:
			case 23:
			case 25:
			case 27:
			case 37:
			case 43:
			case 88:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public simple_stmt(): Simple_stmtContext {
		let localctx: Simple_stmtContext = new Simple_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 28, Python3Parser.RULE_simple_stmt);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 423;
			this.small_stmt();
			this.state = 428;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 52, this._ctx);
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
				_alt = this._interp.adaptivePredict(this._input, 52, this._ctx);
			}
			this.state = 432;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===62) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public small_stmt(): Small_stmtContext {
		let localctx: Small_stmtContext = new Small_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 30, Python3Parser.RULE_small_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 444;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 57:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				{
				this.state = 436;
				this.expr_stmt();
				}
				break;
			case 39:
				{
				this.state = 437;
				this.del_stmt();
				}
				break;
			case 40:
				{
				this.state = 438;
				this.pass_stmt();
				}
				break;
			case 11:
			case 12:
			case 38:
			case 41:
			case 42:
				{
				this.state = 439;
				this.flow_stmt();
				}
				break;
			case 13:
			case 14:
				{
				this.state = 440;
				this.import_stmt();
				}
				break;
			case 16:
				{
				this.state = 441;
				this.global_stmt();
				}
				break;
			case 17:
				{
				this.state = 442;
				this.nonlocal_stmt();
				}
				break;
			case 18:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public expr_stmt(): Expr_stmtContext {
		let localctx: Expr_stmtContext = new Expr_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 32, Python3Parser.RULE_expr_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 446;
			this.testlist_star_expr();
			this.state = 463;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 61:
				{
				this.state = 447;
				this.annassign();
				}
				break;
			case 90:
			case 91:
			case 92:
			case 93:
			case 94:
			case 95:
			case 96:
			case 97:
			case 98:
			case 99:
			case 100:
			case 101:
			case 102:
				{
				this.state = 448;
				this.augassign();
				this.state = 451;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case 38:
					{
					this.state = 449;
					this.yield_expr();
					}
					break;
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 44:
				case 46:
				case 56:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
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
			case 45:
			case 62:
			case 64:
				{
				this.state = 460;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===64) {
					{
					{
					this.state = 453;
					this.match(Python3Parser.ASSIGN);
					this.state = 456;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case 38:
						{
						this.state = 454;
						this.yield_expr();
						}
						break;
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
					case 8:
					case 29:
					case 32:
					case 34:
					case 35:
					case 36:
					case 44:
					case 46:
					case 56:
					case 57:
					case 58:
					case 65:
					case 72:
					case 73:
					case 77:
					case 78:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public annassign(): AnnassignContext {
		let localctx: AnnassignContext = new AnnassignContext(this, this._ctx, this.state);
		this.enterRule(localctx, 34, Python3Parser.RULE_annassign);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 465;
			this.match(Python3Parser.COLON);
			this.state = 466;
			this.test();
			this.state = 469;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===64) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public testlist_star_expr(): Testlist_star_exprContext {
		let localctx: Testlist_star_exprContext = new Testlist_star_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 36, Python3Parser.RULE_testlist_star_expr);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 473;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				{
				this.state = 471;
				this.test();
				}
				break;
			case 57:
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
			_alt = this._interp.adaptivePredict(this._input, 62, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 475;
					this.match(Python3Parser.COMMA);
					this.state = 478;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
					case 8:
					case 29:
					case 32:
					case 34:
					case 35:
					case 36:
					case 44:
					case 46:
					case 56:
					case 58:
					case 65:
					case 72:
					case 73:
					case 77:
					case 78:
						{
						this.state = 476;
						this.test();
						}
						break;
					case 57:
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
				_alt = this._interp.adaptivePredict(this._input, 62, this._ctx);
			}
			this.state = 486;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
				{
				this.state = 485;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public augassign(): AugassignContext {
		let localctx: AugassignContext = new AugassignContext(this, this._ctx, this.state);
		this.enterRule(localctx, 38, Python3Parser.RULE_augassign);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 488;
			_la = this._input.LA(1);
			if(!(((((_la - 90)) & ~0x1F) === 0 && ((1 << (_la - 90)) & 8191) !== 0))) {
			this._errHandler.recoverInline(this);
			}
			else {
				this._errHandler.reportMatch(this);
			    this.consume();
			}
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public del_stmt(): Del_stmtContext {
		let localctx: Del_stmtContext = new Del_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 40, Python3Parser.RULE_del_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 490;
			this.match(Python3Parser.DEL);
			this.state = 491;
			this.exprlist();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public pass_stmt(): Pass_stmtContext {
		let localctx: Pass_stmtContext = new Pass_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 42, Python3Parser.RULE_pass_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 493;
			this.match(Python3Parser.PASS);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public flow_stmt(): Flow_stmtContext {
		let localctx: Flow_stmtContext = new Flow_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 44, Python3Parser.RULE_flow_stmt);
		try {
			this.state = 500;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 42:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 495;
				this.break_stmt();
				}
				break;
			case 41:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 496;
				this.continue_stmt();
				}
				break;
			case 11:
				this.enterOuterAlt(localctx, 3);
				{
				this.state = 497;
				this.return_stmt();
				}
				break;
			case 12:
				this.enterOuterAlt(localctx, 4);
				{
				this.state = 498;
				this.raise_stmt();
				}
				break;
			case 38:
				this.enterOuterAlt(localctx, 5);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public break_stmt(): Break_stmtContext {
		let localctx: Break_stmtContext = new Break_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 46, Python3Parser.RULE_break_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 502;
			this.match(Python3Parser.BREAK);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public continue_stmt(): Continue_stmtContext {
		let localctx: Continue_stmtContext = new Continue_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 48, Python3Parser.RULE_continue_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 504;
			this.match(Python3Parser.CONTINUE);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public return_stmt(): Return_stmtContext {
		let localctx: Return_stmtContext = new Return_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 50, Python3Parser.RULE_return_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 506;
			this.match(Python3Parser.RETURN);
			this.state = 508;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
				{
				this.state = 507;
				this.testlist();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public yield_stmt(): Yield_stmtContext {
		let localctx: Yield_stmtContext = new Yield_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 52, Python3Parser.RULE_yield_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 510;
			this.yield_expr();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public raise_stmt(): Raise_stmtContext {
		let localctx: Raise_stmtContext = new Raise_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 54, Python3Parser.RULE_raise_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 512;
			this.match(Python3Parser.RAISE);
			this.state = 518;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
				{
				this.state = 513;
				this.test();
				this.state = 516;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===13) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public import_stmt(): Import_stmtContext {
		let localctx: Import_stmtContext = new Import_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 56, Python3Parser.RULE_import_stmt);
		try {
			this.state = 522;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 14:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 520;
				this.import_name();
				}
				break;
			case 13:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public import_name(): Import_nameContext {
		let localctx: Import_nameContext = new Import_nameContext(this, this._ctx, this.state);
		this.enterRule(localctx, 58, Python3Parser.RULE_import_name);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 524;
			this.match(Python3Parser.IMPORT);
			this.state = 525;
			this.dotted_as_names();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public import_from(): Import_fromContext {
		let localctx: Import_fromContext = new Import_fromContext(this, this._ctx, this.state);
		this.enterRule(localctx, 60, Python3Parser.RULE_import_from);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			{
			this.state = 527;
			this.match(Python3Parser.FROM);
			this.state = 540;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 71, this._ctx) ) {
			case 1:
				{
				this.state = 531;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===55 || _la===56) {
					{
					{
					this.state = 528;
					_la = this._input.LA(1);
					if(!(_la===55 || _la===56)) {
					this._errHandler.recoverInline(this);
					}
					else {
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
					if(!(_la===55 || _la===56)) {
					this._errHandler.recoverInline(this);
					}
					else {
						this._errHandler.reportMatch(this);
					    this.consume();
					}
					}
					}
					this.state = 538;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				} while (_la===55 || _la===56);
				}
				break;
			}
			this.state = 542;
			this.match(Python3Parser.IMPORT);
			this.state = 549;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 57:
				{
				this.state = 543;
				this.match(Python3Parser.STAR);
				}
				break;
			case 58:
				{
				this.state = 544;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 545;
				this.import_as_names();
				this.state = 546;
				this.match(Python3Parser.CLOSE_PAREN);
				}
				break;
			case 46:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public import_as_name(): Import_as_nameContext {
		let localctx: Import_as_nameContext = new Import_as_nameContext(this, this._ctx, this.state);
		this.enterRule(localctx, 62, Python3Parser.RULE_import_as_name);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 551;
			this.match(Python3Parser.NAME);
			this.state = 554;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===15) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public dotted_as_name(): Dotted_as_nameContext {
		let localctx: Dotted_as_nameContext = new Dotted_as_nameContext(this, this._ctx, this.state);
		this.enterRule(localctx, 64, Python3Parser.RULE_dotted_as_name);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 556;
			this.dotted_name();
			this.state = 559;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===15) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public import_as_names(): Import_as_namesContext {
		let localctx: Import_as_namesContext = new Import_as_namesContext(this, this._ctx, this.state);
		this.enterRule(localctx, 66, Python3Parser.RULE_import_as_names);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 561;
			this.import_as_name();
			this.state = 566;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 75, this._ctx);
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
				_alt = this._interp.adaptivePredict(this._input, 75, this._ctx);
			}
			this.state = 570;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
				{
				this.state = 569;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public dotted_as_names(): Dotted_as_namesContext {
		let localctx: Dotted_as_namesContext = new Dotted_as_namesContext(this, this._ctx, this.state);
		this.enterRule(localctx, 68, Python3Parser.RULE_dotted_as_names);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 572;
			this.dotted_as_name();
			this.state = 577;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public dotted_name(): Dotted_nameContext {
		let localctx: Dotted_nameContext = new Dotted_nameContext(this, this._ctx, this.state);
		this.enterRule(localctx, 70, Python3Parser.RULE_dotted_name);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 580;
			this.match(Python3Parser.NAME);
			this.state = 585;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===55) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public global_stmt(): Global_stmtContext {
		let localctx: Global_stmtContext = new Global_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 72, Python3Parser.RULE_global_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 588;
			this.match(Python3Parser.GLOBAL);
			this.state = 589;
			this.match(Python3Parser.NAME);
			this.state = 594;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public nonlocal_stmt(): Nonlocal_stmtContext {
		let localctx: Nonlocal_stmtContext = new Nonlocal_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 74, Python3Parser.RULE_nonlocal_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 597;
			this.match(Python3Parser.NONLOCAL);
			this.state = 598;
			this.match(Python3Parser.NAME);
			this.state = 603;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public assert_stmt(): Assert_stmtContext {
		let localctx: Assert_stmtContext = new Assert_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 76, Python3Parser.RULE_assert_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 606;
			this.match(Python3Parser.ASSERT);
			this.state = 607;
			this.test();
			this.state = 610;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public compound_stmt(): Compound_stmtContext {
		let localctx: Compound_stmtContext = new Compound_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 78, Python3Parser.RULE_compound_stmt);
		try {
			this.state = 621;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 19:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 612;
				this.if_stmt();
				}
				break;
			case 22:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 613;
				this.while_stmt();
				}
				break;
			case 23:
				this.enterOuterAlt(localctx, 3);
				{
				this.state = 614;
				this.for_stmt();
				}
				break;
			case 25:
				this.enterOuterAlt(localctx, 4);
				{
				this.state = 615;
				this.try_stmt();
				}
				break;
			case 27:
				this.enterOuterAlt(localctx, 5);
				{
				this.state = 616;
				this.with_stmt();
				}
				break;
			case 10:
				this.enterOuterAlt(localctx, 6);
				{
				this.state = 617;
				this.funcdef();
				}
				break;
			case 37:
				this.enterOuterAlt(localctx, 7);
				{
				this.state = 618;
				this.classdef();
				}
				break;
			case 88:
				this.enterOuterAlt(localctx, 8);
				{
				this.state = 619;
				this.decorated();
				}
				break;
			case 43:
				this.enterOuterAlt(localctx, 9);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public async_stmt(): Async_stmtContext {
		let localctx: Async_stmtContext = new Async_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 80, Python3Parser.RULE_async_stmt);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 623;
			this.match(Python3Parser.ASYNC);
			this.state = 627;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 10:
				{
				this.state = 624;
				this.funcdef();
				}
				break;
			case 27:
				{
				this.state = 625;
				this.with_stmt();
				}
				break;
			case 23:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public if_stmt(): If_stmtContext {
		let localctx: If_stmtContext = new If_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 82, Python3Parser.RULE_if_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
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
			while (_la===20) {
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
			if (_la===21) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public while_stmt(): While_stmtContext {
		let localctx: While_stmtContext = new While_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 84, Python3Parser.RULE_while_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
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
			if (_la===21) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public for_stmt(): For_stmtContext {
		let localctx: For_stmtContext = new For_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 86, Python3Parser.RULE_for_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
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
			if (_la===21) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public try_stmt(): Try_stmtContext {
		let localctx: Try_stmtContext = new Try_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 88, Python3Parser.RULE_try_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
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
			case 28:
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
				} while (_la===28);
				this.state = 682;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===21) {
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
				if (_la===26) {
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
			case 26:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public with_stmt(): With_stmtContext {
		let localctx: With_stmtContext = new With_stmtContext(this, this._ctx, this.state);
		this.enterRule(localctx, 90, Python3Parser.RULE_with_stmt);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 694;
			this.match(Python3Parser.WITH);
			this.state = 695;
			this.with_item();
			this.state = 700;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public with_item(): With_itemContext {
		let localctx: With_itemContext = new With_itemContext(this, this._ctx, this.state);
		this.enterRule(localctx, 92, Python3Parser.RULE_with_item);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 706;
			this.test();
			this.state = 709;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===15) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public except_clause(): Except_clauseContext {
		let localctx: Except_clauseContext = new Except_clauseContext(this, this._ctx, this.state);
		this.enterRule(localctx, 94, Python3Parser.RULE_except_clause);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 711;
			this.match(Python3Parser.EXCEPT);
			this.state = 717;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
				{
				this.state = 712;
				this.test();
				this.state = 715;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===15) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public suite(): SuiteContext {
		let localctx: SuiteContext = new SuiteContext(this, this._ctx, this.state);
		this.enterRule(localctx, 96, Python3Parser.RULE_suite);
		let _la: number;
		try {
			this.state = 729;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 11:
			case 12:
			case 13:
			case 14:
			case 16:
			case 17:
			case 18:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 38:
			case 39:
			case 40:
			case 41:
			case 42:
			case 44:
			case 46:
			case 56:
			case 57:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 719;
				this.simple_stmt();
				}
				break;
			case 45:
				this.enterOuterAlt(localctx, 2);
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
				} while ((((_la) & ~0x1F) === 0 && ((1 << _la) & 718241272) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 117465085) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 8401281) !== 0));
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public test(): TestContext {
		let localctx: TestContext = new TestContext(this, this._ctx, this.state);
		this.enterRule(localctx, 98, Python3Parser.RULE_test);
		let _la: number;
		try {
			this.state = 740;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 32:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 731;
				this.or_test();
				this.state = 737;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===19) {
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
			case 29:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public test_nocond(): Test_nocondContext {
		let localctx: Test_nocondContext = new Test_nocondContext(this, this._ctx, this.state);
		this.enterRule(localctx, 100, Python3Parser.RULE_test_nocond);
		try {
			this.state = 744;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 32:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 742;
				this.or_test();
				}
				break;
			case 29:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public lambdef(): LambdefContext {
		let localctx: LambdefContext = new LambdefContext(this, this._ctx, this.state);
		this.enterRule(localctx, 102, Python3Parser.RULE_lambdef);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 746;
			this.match(Python3Parser.LAMBDA);
			this.state = 748;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 46)) & ~0x1F) === 0 && ((1 << (_la - 46)) & 133121) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public lambdef_nocond(): Lambdef_nocondContext {
		let localctx: Lambdef_nocondContext = new Lambdef_nocondContext(this, this._ctx, this.state);
		this.enterRule(localctx, 104, Python3Parser.RULE_lambdef_nocond);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 753;
			this.match(Python3Parser.LAMBDA);
			this.state = 755;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 46)) & ~0x1F) === 0 && ((1 << (_la - 46)) & 133121) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public or_test(): Or_testContext {
		let localctx: Or_testContext = new Or_testContext(this, this._ctx, this.state);
		this.enterRule(localctx, 106, Python3Parser.RULE_or_test);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 760;
			this.and_test();
			this.state = 765;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===30) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public and_test(): And_testContext {
		let localctx: And_testContext = new And_testContext(this, this._ctx, this.state);
		this.enterRule(localctx, 108, Python3Parser.RULE_and_test);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 768;
			this.not_test();
			this.state = 773;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===31) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public not_test(): Not_testContext {
		let localctx: Not_testContext = new Not_testContext(this, this._ctx, this.state);
		this.enterRule(localctx, 110, Python3Parser.RULE_not_test);
		try {
			this.state = 779;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 32:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 776;
				this.match(Python3Parser.NOT);
				this.state = 777;
				this.not_test();
				}
				break;
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public comparison(): ComparisonContext {
		let localctx: ComparisonContext = new ComparisonContext(this, this._ctx, this.state);
		this.enterRule(localctx, 112, Python3Parser.RULE_comparison);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 781;
			this.expr();
			this.state = 787;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (((((_la - 24)) & ~0x1F) === 0 && ((1 << (_la - 24)) & 769) !== 0) || ((((_la - 81)) & ~0x1F) === 0 && ((1 << (_la - 81)) & 127) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public comp_op(): Comp_opContext {
		let localctx: Comp_opContext = new Comp_opContext(this, this._ctx, this.state);
		this.enterRule(localctx, 114, Python3Parser.RULE_comp_op);
		try {
			this.state = 803;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 107, this._ctx) ) {
			case 1:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 790;
				this.match(Python3Parser.LESS_THAN);
				}
				break;
			case 2:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 791;
				this.match(Python3Parser.GREATER_THAN);
				}
				break;
			case 3:
				this.enterOuterAlt(localctx, 3);
				{
				this.state = 792;
				this.match(Python3Parser.EQUALS);
				}
				break;
			case 4:
				this.enterOuterAlt(localctx, 4);
				{
				this.state = 793;
				this.match(Python3Parser.GT_EQ);
				}
				break;
			case 5:
				this.enterOuterAlt(localctx, 5);
				{
				this.state = 794;
				this.match(Python3Parser.LT_EQ);
				}
				break;
			case 6:
				this.enterOuterAlt(localctx, 6);
				{
				this.state = 795;
				this.match(Python3Parser.NOT_EQ_1);
				}
				break;
			case 7:
				this.enterOuterAlt(localctx, 7);
				{
				this.state = 796;
				this.match(Python3Parser.NOT_EQ_2);
				}
				break;
			case 8:
				this.enterOuterAlt(localctx, 8);
				{
				this.state = 797;
				this.match(Python3Parser.IN);
				}
				break;
			case 9:
				this.enterOuterAlt(localctx, 9);
				{
				this.state = 798;
				this.match(Python3Parser.NOT);
				this.state = 799;
				this.match(Python3Parser.IN);
				}
				break;
			case 10:
				this.enterOuterAlt(localctx, 10);
				{
				this.state = 800;
				this.match(Python3Parser.IS);
				}
				break;
			case 11:
				this.enterOuterAlt(localctx, 11);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public star_expr(): Star_exprContext {
		let localctx: Star_exprContext = new Star_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 116, Python3Parser.RULE_star_expr);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 805;
			this.match(Python3Parser.STAR);
			this.state = 806;
			this.expr();
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public expr(): ExprContext {
		let localctx: ExprContext = new ExprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 118, Python3Parser.RULE_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 808;
			this.xor_expr();
			this.state = 813;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===67) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public xor_expr(): Xor_exprContext {
		let localctx: Xor_exprContext = new Xor_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 120, Python3Parser.RULE_xor_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 816;
			this.and_expr();
			this.state = 821;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===68) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public and_expr(): And_exprContext {
		let localctx: And_exprContext = new And_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 122, Python3Parser.RULE_and_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 824;
			this.shift_expr();
			this.state = 829;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===69) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public shift_expr(): Shift_exprContext {
		let localctx: Shift_exprContext = new Shift_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 124, Python3Parser.RULE_shift_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 832;
			this.arith_expr();
			this.state = 837;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===70 || _la===71) {
				{
				{
				this.state = 833;
				_la = this._input.LA(1);
				if(!(_la===70 || _la===71)) {
				this._errHandler.recoverInline(this);
				}
				else {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public arith_expr(): Arith_exprContext {
		let localctx: Arith_exprContext = new Arith_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 126, Python3Parser.RULE_arith_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 840;
			this.term();
			this.state = 845;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===72 || _la===73) {
				{
				{
				this.state = 841;
				_la = this._input.LA(1);
				if(!(_la===72 || _la===73)) {
				this._errHandler.recoverInline(this);
				}
				else {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public term(): TermContext {
		let localctx: TermContext = new TermContext(this, this._ctx, this.state);
		this.enterRule(localctx, 128, Python3Parser.RULE_term);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 848;
			this.factor();
			this.state = 853;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (((((_la - 57)) & ~0x1F) === 0 && ((1 << (_la - 57)) & 2148401153) !== 0)) {
				{
				{
				this.state = 849;
				_la = this._input.LA(1);
				if(!(((((_la - 57)) & ~0x1F) === 0 && ((1 << (_la - 57)) & 2148401153) !== 0))) {
				this._errHandler.recoverInline(this);
				}
				else {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public factor(): FactorContext {
		let localctx: FactorContext = new FactorContext(this, this._ctx, this.state);
		this.enterRule(localctx, 130, Python3Parser.RULE_factor);
		let _la: number;
		try {
			this.state = 859;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 72:
			case 73:
			case 77:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 856;
				_la = this._input.LA(1);
				if(!(((((_la - 72)) & ~0x1F) === 0 && ((1 << (_la - 72)) & 35) !== 0))) {
				this._errHandler.recoverInline(this);
				}
				else {
					this._errHandler.reportMatch(this);
				    this.consume();
				}
				this.state = 857;
				this.factor();
				}
				break;
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 78:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public power(): PowerContext {
		let localctx: PowerContext = new PowerContext(this, this._ctx, this.state);
		this.enterRule(localctx, 132, Python3Parser.RULE_power);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 861;
			this.atom_expr();
			this.state = 864;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===63) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public atom_expr(): Atom_exprContext {
		let localctx: Atom_exprContext = new Atom_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 134, Python3Parser.RULE_atom_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 867;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===44) {
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
			while (((((_la - 55)) & ~0x1F) === 0 && ((1 << (_la - 55)) & 1033) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public atom(): AtomContext {
		let localctx: AtomContext = new AtomContext(this, this._ctx, this.state);
		this.enterRule(localctx, 136, Python3Parser.RULE_atom);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 908;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 58:
				{
				this.state = 876;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 879;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case 38:
					{
					this.state = 877;
					this.yield_expr();
					}
					break;
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 44:
				case 46:
				case 56:
				case 57:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
					{
					this.state = 878;
					this.testlist_comp();
					}
					break;
				case 59:
					break;
				default:
					break;
				}
				this.state = 881;
				this.match(Python3Parser.CLOSE_PAREN);
				}
				break;
			case 65:
				{
				this.state = 882;
				this.match(Python3Parser.OPEN_BRACK);
				this.state = 884;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 117461021) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
					{
					this.state = 883;
					this.testlist_comp();
					}
				}

				this.state = 886;
				this.match(Python3Parser.CLOSE_BRACK);
				}
				break;
			case 78:
				{
				this.state = 887;
				this.match(Python3Parser.OPEN_BRACE);
				this.state = 889;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 2264944669) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
					{
					this.state = 888;
					this.dictorsetmaker();
					}
				}

				this.state = 891;
				this.match(Python3Parser.CLOSE_BRACE);
				}
				break;
			case 46:
				{
				this.state = 892;
				this.match(Python3Parser.NAME);
				}
				break;
			case 8:
				{
				this.state = 893;
				this.match(Python3Parser.NUMBER);
				}
				break;
			case 3:
			case 4:
			case 5:
			case 6:
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
				} while ((((_la) & ~0x1F) === 0 && ((1 << _la) & 120) !== 0));
				}
				break;
			case 7:
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
				} while (_la===7);
				}
				break;
			case 56:
				{
				this.state = 904;
				this.match(Python3Parser.ELLIPSIS);
				}
				break;
			case 34:
				{
				this.state = 905;
				this.match(Python3Parser.NONE);
				}
				break;
			case 35:
				{
				this.state = 906;
				this.match(Python3Parser.TRUE);
				}
				break;
			case 36:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public testlist_comp(): Testlist_compContext {
		let localctx: Testlist_compContext = new Testlist_compContext(this, this._ctx, this.state);
		this.enterRule(localctx, 138, Python3Parser.RULE_testlist_comp);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 912;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				{
				this.state = 910;
				this.test();
				}
				break;
			case 57:
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
			case 23:
			case 43:
				{
				this.state = 914;
				this.comp_for();
				}
				break;
			case 59:
			case 60:
			case 66:
				{
				this.state = 922;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 126, this._ctx);
				while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
					if (_alt === 1) {
						{
						{
						this.state = 915;
						this.match(Python3Parser.COMMA);
						this.state = 918;
						this._errHandler.sync(this);
						switch (this._input.LA(1)) {
						case 3:
						case 4:
						case 5:
						case 6:
						case 7:
						case 8:
						case 29:
						case 32:
						case 34:
						case 35:
						case 36:
						case 44:
						case 46:
						case 56:
						case 58:
						case 65:
						case 72:
						case 73:
						case 77:
						case 78:
							{
							this.state = 916;
							this.test();
							}
							break;
						case 57:
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
					_alt = this._interp.adaptivePredict(this._input, 126, this._ctx);
				}
				this.state = 926;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public trailer(): TrailerContext {
		let localctx: TrailerContext = new TrailerContext(this, this._ctx, this.state);
		this.enterRule(localctx, 140, Python3Parser.RULE_trailer);
		try {
			this.state = 937;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 58:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 930;
				this.callArguments();
				}
				break;
			case 65:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 931;
				this.match(Python3Parser.OPEN_BRACK);
				this.state = 932;
				this.subscriptlist();
				this.state = 933;
				this.match(Python3Parser.CLOSE_BRACK);
				}
				break;
			case 55:
				this.enterOuterAlt(localctx, 3);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public subscriptlist(): SubscriptlistContext {
		let localctx: SubscriptlistContext = new SubscriptlistContext(this, this._ctx, this.state);
		this.enterRule(localctx, 142, Python3Parser.RULE_subscriptlist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 939;
			this.subscript();
			this.state = 944;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 130, this._ctx);
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
				_alt = this._interp.adaptivePredict(this._input, 130, this._ctx);
			}
			this.state = 948;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
				{
				this.state = 947;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public subscript(): SubscriptContext {
		let localctx: SubscriptContext = new SubscriptContext(this, this._ctx, this.state);
		this.enterRule(localctx, 144, Python3Parser.RULE_subscript);
		let _la: number;
		try {
			this.state = 961;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 135, this._ctx) ) {
			case 1:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 950;
				this.test();
				}
				break;
			case 2:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 952;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
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
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
					{
					this.state = 955;
					this.test();
					}
				}

				this.state = 959;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===61) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public sliceop(): SliceopContext {
		let localctx: SliceopContext = new SliceopContext(this, this._ctx, this.state);
		this.enterRule(localctx, 146, Python3Parser.RULE_sliceop);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 963;
			this.match(Python3Parser.COLON);
			this.state = 965;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
				{
				this.state = 964;
				this.test();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public exprlist(): ExprlistContext {
		let localctx: ExprlistContext = new ExprlistContext(this, this._ctx, this.state);
		this.enterRule(localctx, 148, Python3Parser.RULE_exprlist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 969;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				{
				this.state = 967;
				this.expr();
				}
				break;
			case 57:
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
			_alt = this._interp.adaptivePredict(this._input, 139, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 971;
					this.match(Python3Parser.COMMA);
					this.state = 974;
					this._errHandler.sync(this);
					switch (this._input.LA(1)) {
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
					case 8:
					case 34:
					case 35:
					case 36:
					case 44:
					case 46:
					case 56:
					case 58:
					case 65:
					case 72:
					case 73:
					case 77:
					case 78:
						{
						this.state = 972;
						this.expr();
						}
						break;
					case 57:
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
				_alt = this._interp.adaptivePredict(this._input, 139, this._ctx);
			}
			this.state = 982;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
				{
				this.state = 981;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public testlist(): TestlistContext {
		let localctx: TestlistContext = new TestlistContext(this, this._ctx, this.state);
		this.enterRule(localctx, 150, Python3Parser.RULE_testlist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 984;
			this.test();
			this.state = 989;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 141, this._ctx);
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
				_alt = this._interp.adaptivePredict(this._input, 141, this._ctx);
			}
			this.state = 993;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
				{
				this.state = 992;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public dictorsetmaker(): DictorsetmakerContext {
		let localctx: DictorsetmakerContext = new DictorsetmakerContext(this, this._ctx, this.state);
		this.enterRule(localctx, 152, Python3Parser.RULE_dictorsetmaker);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1043;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 153, this._ctx) ) {
			case 1:
				{
				{
				this.state = 1001;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 44:
				case 46:
				case 56:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
					{
					this.state = 995;
					this.test();
					this.state = 996;
					this.match(Python3Parser.COLON);
					this.state = 997;
					this.test();
					}
					break;
				case 63:
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
				case 23:
				case 43:
					{
					this.state = 1003;
					this.comp_for();
					}
					break;
				case 60:
				case 80:
					{
					this.state = 1015;
					this._errHandler.sync(this);
					_alt = this._interp.adaptivePredict(this._input, 145, this._ctx);
					while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
						if (_alt === 1) {
							{
							{
							this.state = 1004;
							this.match(Python3Parser.COMMA);
							this.state = 1011;
							this._errHandler.sync(this);
							switch (this._input.LA(1)) {
							case 3:
							case 4:
							case 5:
							case 6:
							case 7:
							case 8:
							case 29:
							case 32:
							case 34:
							case 35:
							case 36:
							case 44:
							case 46:
							case 56:
							case 58:
							case 65:
							case 72:
							case 73:
							case 77:
							case 78:
								{
								this.state = 1005;
								this.test();
								this.state = 1006;
								this.match(Python3Parser.COLON);
								this.state = 1007;
								this.test();
								}
								break;
							case 63:
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
						_alt = this._interp.adaptivePredict(this._input, 145, this._ctx);
					}
					this.state = 1019;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la===60) {
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
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 44:
				case 46:
				case 56:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
					{
					this.state = 1023;
					this.test();
					}
					break;
				case 57:
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
				case 23:
				case 43:
					{
					this.state = 1027;
					this.comp_for();
					}
					break;
				case 60:
				case 80:
					{
					this.state = 1035;
					this._errHandler.sync(this);
					_alt = this._interp.adaptivePredict(this._input, 150, this._ctx);
					while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
						if (_alt === 1) {
							{
							{
							this.state = 1028;
							this.match(Python3Parser.COMMA);
							this.state = 1031;
							this._errHandler.sync(this);
							switch (this._input.LA(1)) {
							case 3:
							case 4:
							case 5:
							case 6:
							case 7:
							case 8:
							case 29:
							case 32:
							case 34:
							case 35:
							case 36:
							case 44:
							case 46:
							case 56:
							case 58:
							case 65:
							case 72:
							case 73:
							case 77:
							case 78:
								{
								this.state = 1029;
								this.test();
								}
								break;
							case 57:
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
						_alt = this._interp.adaptivePredict(this._input, 150, this._ctx);
					}
					this.state = 1039;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
					if (_la===60) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public classdef(): ClassdefContext {
		let localctx: ClassdefContext = new ClassdefContext(this, this._ctx, this.state);
		this.enterRule(localctx, 154, Python3Parser.RULE_classdef);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1045;
			this.match(Python3Parser.CLASS);
			this.state = 1046;
			this.match(Python3Parser.NAME);
			this.state = 1052;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===58) {
				{
				this.state = 1047;
				this.match(Python3Parser.OPEN_PAREN);
				this.state = 1049;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 2264944669) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public callArguments(): CallArgumentsContext {
		let localctx: CallArgumentsContext = new CallArgumentsContext(this, this._ctx, this.state);
		this.enterRule(localctx, 156, Python3Parser.RULE_callArguments);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1057;
			this.match(Python3Parser.OPEN_PAREN);
			this.state = 1059;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536871416) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 2264944669) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public arglist(): ArglistContext {
		let localctx: ArglistContext = new ArglistContext(this, this._ctx, this.state);
		this.enterRule(localctx, 158, Python3Parser.RULE_arglist);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1063;
			this.argument();
			this.state = 1068;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 157, this._ctx);
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
				_alt = this._interp.adaptivePredict(this._input, 157, this._ctx);
			}
			this.state = 1072;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===60) {
				{
				this.state = 1071;
				this.match(Python3Parser.COMMA);
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public argument(): ArgumentContext {
		let localctx: ArgumentContext = new ArgumentContext(this, this._ctx, this.state);
		this.enterRule(localctx, 160, Python3Parser.RULE_argument);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1086;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 160, this._ctx) ) {
			case 1:
				{
				this.state = 1074;
				this.test();
				this.state = 1076;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===23 || _la===43) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public comp_iter(): Comp_iterContext {
		let localctx: Comp_iterContext = new Comp_iterContext(this, this._ctx, this.state);
		this.enterRule(localctx, 162, Python3Parser.RULE_comp_iter);
		try {
			this.state = 1090;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 23:
			case 43:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 1088;
				this.comp_for();
				}
				break;
			case 19:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public comp_for(): Comp_forContext {
		let localctx: Comp_forContext = new Comp_forContext(this, this._ctx, this.state);
		this.enterRule(localctx, 164, Python3Parser.RULE_comp_for);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1093;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===43) {
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
			if (((((_la - 19)) & ~0x1F) === 0 && ((1 << (_la - 19)) & 16777233) !== 0)) {
				{
				this.state = 1099;
				this.comp_iter();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public comp_if(): Comp_ifContext {
		let localctx: Comp_ifContext = new Comp_ifContext(this, this._ctx, this.state);
		this.enterRule(localctx, 166, Python3Parser.RULE_comp_if);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1102;
			this.match(Python3Parser.IF);
			this.state = 1103;
			this.test_nocond();
			this.state = 1105;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (((((_la - 19)) & ~0x1F) === 0 && ((1 << (_la - 19)) & 16777233) !== 0)) {
				{
				this.state = 1104;
				this.comp_iter();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public encoding_decl(): Encoding_declContext {
		let localctx: Encoding_declContext = new Encoding_declContext(this, this._ctx, this.state);
		this.enterRule(localctx, 168, Python3Parser.RULE_encoding_decl);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1107;
			this.match(Python3Parser.NAME);
			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public yield_expr(): Yield_exprContext {
		let localctx: Yield_exprContext = new Yield_exprContext(this, this._ctx, this.state);
		this.enterRule(localctx, 170, Python3Parser.RULE_yield_expr);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 1109;
			this.match(Python3Parser.YIELD);
			this.state = 1111;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 536879608) !== 0) || ((((_la - 32)) & ~0x1F) === 0 && ((1 << (_la - 32)) & 83906589) !== 0) || ((((_la - 65)) & ~0x1F) === 0 && ((1 << (_la - 65)) & 12673) !== 0)) {
				{
				this.state = 1110;
				this.yield_arg();
				}
			}

			}
		}
		catch (re) {
			if (re instanceof RecognitionException) {
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public yield_arg(): Yield_argContext {
		let localctx: Yield_argContext = new Yield_argContext(this, this._ctx, this.state);
		this.enterRule(localctx, 172, Python3Parser.RULE_yield_arg);
		try {
			this.state = 1116;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 13:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 1113;
				this.match(Python3Parser.FROM);
				this.state = 1114;
				this.test();
				}
				break;
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 29:
			case 32:
			case 34:
			case 35:
			case 36:
			case 44:
			case 46:
			case 56:
			case 58:
			case 65:
			case 72:
			case 73:
			case 77:
			case 78:
				this.enterOuterAlt(localctx, 2);
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public string_template(): String_templateContext {
		let localctx: String_templateContext = new String_templateContext(this, this._ctx, this.state);
		this.enterRule(localctx, 174, Python3Parser.RULE_string_template);
		let _la: number;
		try {
			this.state = 1150;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 3:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 1118;
				this.match(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START);
				this.state = 1122;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===78 || _la===109) {
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
			case 5:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 1126;
				this.match(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START);
				this.state = 1130;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===78 || _la===109) {
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
			case 4:
				this.enterOuterAlt(localctx, 3);
				{
				this.state = 1134;
				this.match(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START);
				this.state = 1138;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===78 || _la===112) {
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
			case 6:
				this.enterOuterAlt(localctx, 4);
				{
				this.state = 1142;
				this.match(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START);
				this.state = 1146;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===78 || _la===112) {
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public single_string_template_atom(): Single_string_template_atomContext {
		let localctx: Single_string_template_atomContext = new Single_string_template_atomContext(this, this._ctx, this.state);
		this.enterRule(localctx, 176, Python3Parser.RULE_single_string_template_atom);
		try {
			this.state = 1160;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 109:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 1152;
				this.match(Python3Parser.SINGLE_QUOTE_STRING_ATOM);
				}
				break;
			case 78:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 1153;
				this.match(Python3Parser.OPEN_BRACE);
				this.state = 1156;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 44:
				case 46:
				case 56:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
					{
					this.state = 1154;
					this.test();
					}
					break;
				case 57:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}
	// @RuleVersion(0)
	public double_string_template_atom(): Double_string_template_atomContext {
		let localctx: Double_string_template_atomContext = new Double_string_template_atomContext(this, this._ctx, this.state);
		this.enterRule(localctx, 178, Python3Parser.RULE_double_string_template_atom);
		try {
			this.state = 1170;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 112:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 1162;
				this.match(Python3Parser.DOUBLE_QUOTE_STRING_ATOM);
				}
				break;
			case 78:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 1163;
				this.match(Python3Parser.OPEN_BRACE);
				this.state = 1166;
				this._errHandler.sync(this);
				switch (this._input.LA(1)) {
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 29:
				case 32:
				case 34:
				case 35:
				case 36:
				case 44:
				case 46:
				case 56:
				case 58:
				case 65:
				case 72:
				case 73:
				case 77:
				case 78:
					{
					this.state = 1164;
					this.test();
					}
					break;
				case 57:
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
				localctx.exception = re;
				this._errHandler.reportError(this, re);
				this._errHandler.recover(this, re);
			} else {
				throw re;
			}
		}
		finally {
			this.exitRule();
		}
		return localctx;
	}

	public static readonly _serializedATN: number[] = [4,1,112,1173,2,0,7,0,
	2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,7,7,7,2,8,7,8,2,9,7,9,
	2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,14,2,15,7,15,2,16,7,16,2,
	17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,7,21,2,22,7,22,2,23,7,23,2,24,
	7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,7,28,2,29,7,29,2,30,7,30,2,31,7,
	31,2,32,7,32,2,33,7,33,2,34,7,34,2,35,7,35,2,36,7,36,2,37,7,37,2,38,7,38,
	2,39,7,39,2,40,7,40,2,41,7,41,2,42,7,42,2,43,7,43,2,44,7,44,2,45,7,45,2,
	46,7,46,2,47,7,47,2,48,7,48,2,49,7,49,2,50,7,50,2,51,7,51,2,52,7,52,2,53,
	7,53,2,54,7,54,2,55,7,55,2,56,7,56,2,57,7,57,2,58,7,58,2,59,7,59,2,60,7,
	60,2,61,7,61,2,62,7,62,2,63,7,63,2,64,7,64,2,65,7,65,2,66,7,66,2,67,7,67,
	2,68,7,68,2,69,7,69,2,70,7,70,2,71,7,71,2,72,7,72,2,73,7,73,2,74,7,74,2,
	75,7,75,2,76,7,76,2,77,7,77,2,78,7,78,2,79,7,79,2,80,7,80,2,81,7,81,2,82,
	7,82,2,83,7,83,2,84,7,84,2,85,7,85,2,86,7,86,2,87,7,87,2,88,7,88,2,89,7,
	89,1,0,1,0,5,0,183,8,0,10,0,12,0,186,9,0,1,0,1,0,1,1,1,1,1,1,1,1,1,1,3,
	1,195,8,1,1,2,1,2,5,2,199,8,2,10,2,12,2,202,9,2,1,2,1,2,1,3,1,3,1,3,1,3,
	3,3,210,8,3,1,3,3,3,213,8,3,1,3,1,3,1,4,4,4,218,8,4,11,4,12,4,219,1,5,1,
	5,1,5,1,5,3,5,226,8,5,1,6,1,6,1,6,1,7,1,7,1,7,1,7,1,7,3,7,236,8,7,1,7,1,
	7,1,7,1,8,1,8,3,8,243,8,8,1,8,1,8,1,9,1,9,1,9,3,9,250,8,9,1,9,1,9,1,9,1,
	9,3,9,256,8,9,5,9,258,8,9,10,9,12,9,261,9,9,1,9,1,9,1,9,3,9,266,8,9,1,9,
	1,9,1,9,1,9,3,9,272,8,9,5,9,274,8,9,10,9,12,9,277,9,9,1,9,1,9,1,9,1,9,3,
	9,283,8,9,3,9,285,8,9,3,9,287,8,9,1,9,1,9,1,9,3,9,292,8,9,3,9,294,8,9,3,
	9,296,8,9,1,9,1,9,3,9,300,8,9,1,9,1,9,1,9,1,9,3,9,306,8,9,5,9,308,8,9,10,
	9,12,9,311,9,9,1,9,1,9,1,9,1,9,3,9,317,8,9,3,9,319,8,9,3,9,321,8,9,1,9,
	1,9,1,9,3,9,326,8,9,3,9,328,8,9,1,10,1,10,1,10,3,10,333,8,10,1,11,1,11,
	1,11,3,11,338,8,11,1,11,1,11,1,11,1,11,3,11,344,8,11,5,11,346,8,11,10,11,
	12,11,349,9,11,1,11,1,11,1,11,3,11,354,8,11,1,11,1,11,1,11,1,11,3,11,360,
	8,11,5,11,362,8,11,10,11,12,11,365,9,11,1,11,1,11,1,11,1,11,3,11,371,8,
	11,3,11,373,8,11,3,11,375,8,11,1,11,1,11,1,11,3,11,380,8,11,3,11,382,8,
	11,3,11,384,8,11,1,11,1,11,3,11,388,8,11,1,11,1,11,1,11,1,11,3,11,394,8,
	11,5,11,396,8,11,10,11,12,11,399,9,11,1,11,1,11,1,11,1,11,3,11,405,8,11,
	3,11,407,8,11,3,11,409,8,11,1,11,1,11,1,11,3,11,414,8,11,3,11,416,8,11,
	1,12,1,12,1,13,1,13,3,13,422,8,13,1,14,1,14,1,14,5,14,427,8,14,10,14,12,
	14,430,9,14,1,14,3,14,433,8,14,1,14,1,14,1,15,1,15,1,15,1,15,1,15,1,15,
	1,15,1,15,3,15,445,8,15,1,16,1,16,1,16,1,16,1,16,3,16,452,8,16,1,16,1,16,
	1,16,3,16,457,8,16,5,16,459,8,16,10,16,12,16,462,9,16,3,16,464,8,16,1,17,
	1,17,1,17,1,17,3,17,470,8,17,1,18,1,18,3,18,474,8,18,1,18,1,18,1,18,3,18,
	479,8,18,5,18,481,8,18,10,18,12,18,484,9,18,1,18,3,18,487,8,18,1,19,1,19,
	1,20,1,20,1,20,1,21,1,21,1,22,1,22,1,22,1,22,1,22,3,22,501,8,22,1,23,1,
	23,1,24,1,24,1,25,1,25,3,25,509,8,25,1,26,1,26,1,27,1,27,1,27,1,27,3,27,
	517,8,27,3,27,519,8,27,1,28,1,28,3,28,523,8,28,1,29,1,29,1,29,1,30,1,30,
	5,30,530,8,30,10,30,12,30,533,9,30,1,30,1,30,4,30,537,8,30,11,30,12,30,
	538,3,30,541,8,30,1,30,1,30,1,30,1,30,1,30,1,30,1,30,3,30,550,8,30,1,31,
	1,31,1,31,3,31,555,8,31,1,32,1,32,1,32,3,32,560,8,32,1,33,1,33,1,33,5,33,
	565,8,33,10,33,12,33,568,9,33,1,33,3,33,571,8,33,1,34,1,34,1,34,5,34,576,
	8,34,10,34,12,34,579,9,34,1,35,1,35,1,35,5,35,584,8,35,10,35,12,35,587,
	9,35,1,36,1,36,1,36,1,36,5,36,593,8,36,10,36,12,36,596,9,36,1,37,1,37,1,
	37,1,37,5,37,602,8,37,10,37,12,37,605,9,37,1,38,1,38,1,38,1,38,3,38,611,
	8,38,1,39,1,39,1,39,1,39,1,39,1,39,1,39,1,39,1,39,3,39,622,8,39,1,40,1,
	40,1,40,1,40,3,40,628,8,40,1,41,1,41,1,41,1,41,1,41,1,41,1,41,1,41,1,41,
	5,41,639,8,41,10,41,12,41,642,9,41,1,41,1,41,1,41,3,41,647,8,41,1,42,1,
	42,1,42,1,42,1,42,1,42,1,42,3,42,656,8,42,1,43,1,43,1,43,1,43,1,43,1,43,
	1,43,1,43,1,43,3,43,667,8,43,1,44,1,44,1,44,1,44,1,44,1,44,1,44,4,44,676,
	8,44,11,44,12,44,677,1,44,1,44,1,44,3,44,683,8,44,1,44,1,44,1,44,3,44,688,
	8,44,1,44,1,44,1,44,3,44,693,8,44,1,45,1,45,1,45,1,45,5,45,699,8,45,10,
	45,12,45,702,9,45,1,45,1,45,1,45,1,46,1,46,1,46,3,46,710,8,46,1,47,1,47,
	1,47,1,47,3,47,716,8,47,3,47,718,8,47,1,48,1,48,1,48,1,48,4,48,724,8,48,
	11,48,12,48,725,1,48,1,48,3,48,730,8,48,1,49,1,49,1,49,1,49,1,49,1,49,3,
	49,738,8,49,1,49,3,49,741,8,49,1,50,1,50,3,50,745,8,50,1,51,1,51,3,51,749,
	8,51,1,51,1,51,1,51,1,52,1,52,3,52,756,8,52,1,52,1,52,1,52,1,53,1,53,1,
	53,5,53,764,8,53,10,53,12,53,767,9,53,1,54,1,54,1,54,5,54,772,8,54,10,54,
	12,54,775,9,54,1,55,1,55,1,55,3,55,780,8,55,1,56,1,56,1,56,1,56,5,56,786,
	8,56,10,56,12,56,789,9,56,1,57,1,57,1,57,1,57,1,57,1,57,1,57,1,57,1,57,
	1,57,1,57,1,57,1,57,3,57,804,8,57,1,58,1,58,1,58,1,59,1,59,1,59,5,59,812,
	8,59,10,59,12,59,815,9,59,1,60,1,60,1,60,5,60,820,8,60,10,60,12,60,823,
	9,60,1,61,1,61,1,61,5,61,828,8,61,10,61,12,61,831,9,61,1,62,1,62,1,62,5,
	62,836,8,62,10,62,12,62,839,9,62,1,63,1,63,1,63,5,63,844,8,63,10,63,12,
	63,847,9,63,1,64,1,64,1,64,5,64,852,8,64,10,64,12,64,855,9,64,1,65,1,65,
	1,65,3,65,860,8,65,1,66,1,66,1,66,3,66,865,8,66,1,67,3,67,868,8,67,1,67,
	1,67,5,67,872,8,67,10,67,12,67,875,9,67,1,68,1,68,1,68,3,68,880,8,68,1,
	68,1,68,1,68,3,68,885,8,68,1,68,1,68,1,68,3,68,890,8,68,1,68,1,68,1,68,
	1,68,4,68,896,8,68,11,68,12,68,897,1,68,4,68,901,8,68,11,68,12,68,902,1,
	68,1,68,1,68,1,68,3,68,909,8,68,1,69,1,69,3,69,913,8,69,1,69,1,69,1,69,
	1,69,3,69,919,8,69,5,69,921,8,69,10,69,12,69,924,9,69,1,69,3,69,927,8,69,
	3,69,929,8,69,1,70,1,70,1,70,1,70,1,70,1,70,1,70,3,70,938,8,70,1,71,1,71,
	1,71,5,71,943,8,71,10,71,12,71,946,9,71,1,71,3,71,949,8,71,1,72,1,72,3,
	72,953,8,72,1,72,1,72,3,72,957,8,72,1,72,3,72,960,8,72,3,72,962,8,72,1,
	73,1,73,3,73,966,8,73,1,74,1,74,3,74,970,8,74,1,74,1,74,1,74,3,74,975,8,
	74,5,74,977,8,74,10,74,12,74,980,9,74,1,74,3,74,983,8,74,1,75,1,75,1,75,
	5,75,988,8,75,10,75,12,75,991,9,75,1,75,3,75,994,8,75,1,76,1,76,1,76,1,
	76,1,76,1,76,3,76,1002,8,76,1,76,1,76,1,76,1,76,1,76,1,76,1,76,1,76,3,76,
	1012,8,76,5,76,1014,8,76,10,76,12,76,1017,9,76,1,76,3,76,1020,8,76,3,76,
	1022,8,76,1,76,1,76,3,76,1026,8,76,1,76,1,76,1,76,1,76,3,76,1032,8,76,5,
	76,1034,8,76,10,76,12,76,1037,9,76,1,76,3,76,1040,8,76,3,76,1042,8,76,3,
	76,1044,8,76,1,77,1,77,1,77,1,77,3,77,1050,8,77,1,77,3,77,1053,8,77,1,77,
	1,77,1,77,1,78,1,78,3,78,1060,8,78,1,78,1,78,1,79,1,79,1,79,5,79,1067,8,
	79,10,79,12,79,1070,9,79,1,79,3,79,1073,8,79,1,80,1,80,3,80,1077,8,80,1,
	80,1,80,1,80,1,80,1,80,1,80,1,80,1,80,3,80,1087,8,80,1,81,1,81,3,81,1091,
	8,81,1,82,3,82,1094,8,82,1,82,1,82,1,82,1,82,1,82,3,82,1101,8,82,1,83,1,
	83,1,83,3,83,1106,8,83,1,84,1,84,1,85,1,85,3,85,1112,8,85,1,86,1,86,1,86,
	3,86,1117,8,86,1,87,1,87,5,87,1121,8,87,10,87,12,87,1124,9,87,1,87,1,87,
	1,87,5,87,1129,8,87,10,87,12,87,1132,9,87,1,87,1,87,1,87,5,87,1137,8,87,
	10,87,12,87,1140,9,87,1,87,1,87,1,87,5,87,1145,8,87,10,87,12,87,1148,9,
	87,1,87,3,87,1151,8,87,1,88,1,88,1,88,1,88,3,88,1157,8,88,1,88,1,88,3,88,
	1161,8,88,1,89,1,89,1,89,1,89,3,89,1167,8,89,1,89,1,89,3,89,1171,8,89,1,
	89,0,0,90,0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,
	44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,
	92,94,96,98,100,102,104,106,108,110,112,114,116,118,120,122,124,126,128,
	130,132,134,136,138,140,142,144,146,148,150,152,154,156,158,160,162,164,
	166,168,170,172,174,176,178,0,6,1,0,90,102,1,0,55,56,1,0,70,71,1,0,72,73,
	3,0,57,57,74,76,88,88,2,0,72,73,77,77,1307,0,184,1,0,0,0,2,194,1,0,0,0,
	4,196,1,0,0,0,6,205,1,0,0,0,8,217,1,0,0,0,10,221,1,0,0,0,12,227,1,0,0,0,
	14,230,1,0,0,0,16,240,1,0,0,0,18,327,1,0,0,0,20,329,1,0,0,0,22,415,1,0,
	0,0,24,417,1,0,0,0,26,421,1,0,0,0,28,423,1,0,0,0,30,444,1,0,0,0,32,446,
	1,0,0,0,34,465,1,0,0,0,36,473,1,0,0,0,38,488,1,0,0,0,40,490,1,0,0,0,42,
	493,1,0,0,0,44,500,1,0,0,0,46,502,1,0,0,0,48,504,1,0,0,0,50,506,1,0,0,0,
	52,510,1,0,0,0,54,512,1,0,0,0,56,522,1,0,0,0,58,524,1,0,0,0,60,527,1,0,
	0,0,62,551,1,0,0,0,64,556,1,0,0,0,66,561,1,0,0,0,68,572,1,0,0,0,70,580,
	1,0,0,0,72,588,1,0,0,0,74,597,1,0,0,0,76,606,1,0,0,0,78,621,1,0,0,0,80,
	623,1,0,0,0,82,629,1,0,0,0,84,648,1,0,0,0,86,657,1,0,0,0,88,668,1,0,0,0,
	90,694,1,0,0,0,92,706,1,0,0,0,94,711,1,0,0,0,96,729,1,0,0,0,98,740,1,0,
	0,0,100,744,1,0,0,0,102,746,1,0,0,0,104,753,1,0,0,0,106,760,1,0,0,0,108,
	768,1,0,0,0,110,779,1,0,0,0,112,781,1,0,0,0,114,803,1,0,0,0,116,805,1,0,
	0,0,118,808,1,0,0,0,120,816,1,0,0,0,122,824,1,0,0,0,124,832,1,0,0,0,126,
	840,1,0,0,0,128,848,1,0,0,0,130,859,1,0,0,0,132,861,1,0,0,0,134,867,1,0,
	0,0,136,908,1,0,0,0,138,912,1,0,0,0,140,937,1,0,0,0,142,939,1,0,0,0,144,
	961,1,0,0,0,146,963,1,0,0,0,148,969,1,0,0,0,150,984,1,0,0,0,152,1043,1,
	0,0,0,154,1045,1,0,0,0,156,1057,1,0,0,0,158,1063,1,0,0,0,160,1086,1,0,0,
	0,162,1090,1,0,0,0,164,1093,1,0,0,0,166,1102,1,0,0,0,168,1107,1,0,0,0,170,
	1109,1,0,0,0,172,1116,1,0,0,0,174,1150,1,0,0,0,176,1160,1,0,0,0,178,1170,
	1,0,0,0,180,183,5,45,0,0,181,183,3,26,13,0,182,180,1,0,0,0,182,181,1,0,
	0,0,183,186,1,0,0,0,184,182,1,0,0,0,184,185,1,0,0,0,185,187,1,0,0,0,186,
	184,1,0,0,0,187,188,5,0,0,1,188,1,1,0,0,0,189,195,5,45,0,0,190,195,3,28,
	14,0,191,192,3,78,39,0,192,193,5,45,0,0,193,195,1,0,0,0,194,189,1,0,0,0,
	194,190,1,0,0,0,194,191,1,0,0,0,195,3,1,0,0,0,196,200,3,150,75,0,197,199,
	5,45,0,0,198,197,1,0,0,0,199,202,1,0,0,0,200,198,1,0,0,0,200,201,1,0,0,
	0,201,203,1,0,0,0,202,200,1,0,0,0,203,204,5,0,0,1,204,5,1,0,0,0,205,206,
	5,88,0,0,206,212,3,70,35,0,207,209,5,58,0,0,208,210,3,158,79,0,209,208,
	1,0,0,0,209,210,1,0,0,0,210,211,1,0,0,0,211,213,5,59,0,0,212,207,1,0,0,
	0,212,213,1,0,0,0,213,214,1,0,0,0,214,215,5,45,0,0,215,7,1,0,0,0,216,218,
	3,6,3,0,217,216,1,0,0,0,218,219,1,0,0,0,219,217,1,0,0,0,219,220,1,0,0,0,
	220,9,1,0,0,0,221,225,3,8,4,0,222,226,3,154,77,0,223,226,3,14,7,0,224,226,
	3,12,6,0,225,222,1,0,0,0,225,223,1,0,0,0,225,224,1,0,0,0,226,11,1,0,0,0,
	227,228,5,43,0,0,228,229,3,14,7,0,229,13,1,0,0,0,230,231,5,10,0,0,231,232,
	5,46,0,0,232,235,3,16,8,0,233,234,5,89,0,0,234,236,3,98,49,0,235,233,1,
	0,0,0,235,236,1,0,0,0,236,237,1,0,0,0,237,238,5,61,0,0,238,239,3,96,48,
	0,239,15,1,0,0,0,240,242,5,58,0,0,241,243,3,18,9,0,242,241,1,0,0,0,242,
	243,1,0,0,0,243,244,1,0,0,0,244,245,5,59,0,0,245,17,1,0,0,0,246,249,3,20,
	10,0,247,248,5,64,0,0,248,250,3,98,49,0,249,247,1,0,0,0,249,250,1,0,0,0,
	250,259,1,0,0,0,251,252,5,60,0,0,252,255,3,20,10,0,253,254,5,64,0,0,254,
	256,3,98,49,0,255,253,1,0,0,0,255,256,1,0,0,0,256,258,1,0,0,0,257,251,1,
	0,0,0,258,261,1,0,0,0,259,257,1,0,0,0,259,260,1,0,0,0,260,295,1,0,0,0,261,
	259,1,0,0,0,262,293,5,60,0,0,263,265,5,57,0,0,264,266,3,20,10,0,265,264,
	1,0,0,0,265,266,1,0,0,0,266,275,1,0,0,0,267,268,5,60,0,0,268,271,3,20,10,
	0,269,270,5,64,0,0,270,272,3,98,49,0,271,269,1,0,0,0,271,272,1,0,0,0,272,
	274,1,0,0,0,273,267,1,0,0,0,274,277,1,0,0,0,275,273,1,0,0,0,275,276,1,0,
	0,0,276,286,1,0,0,0,277,275,1,0,0,0,278,284,5,60,0,0,279,280,5,63,0,0,280,
	282,3,20,10,0,281,283,5,60,0,0,282,281,1,0,0,0,282,283,1,0,0,0,283,285,
	1,0,0,0,284,279,1,0,0,0,284,285,1,0,0,0,285,287,1,0,0,0,286,278,1,0,0,0,
	286,287,1,0,0,0,287,294,1,0,0,0,288,289,5,63,0,0,289,291,3,20,10,0,290,
	292,5,60,0,0,291,290,1,0,0,0,291,292,1,0,0,0,292,294,1,0,0,0,293,263,1,
	0,0,0,293,288,1,0,0,0,293,294,1,0,0,0,294,296,1,0,0,0,295,262,1,0,0,0,295,
	296,1,0,0,0,296,328,1,0,0,0,297,299,5,57,0,0,298,300,3,20,10,0,299,298,
	1,0,0,0,299,300,1,0,0,0,300,309,1,0,0,0,301,302,5,60,0,0,302,305,3,20,10,
	0,303,304,5,64,0,0,304,306,3,98,49,0,305,303,1,0,0,0,305,306,1,0,0,0,306,
	308,1,0,0,0,307,301,1,0,0,0,308,311,1,0,0,0,309,307,1,0,0,0,309,310,1,0,
	0,0,310,320,1,0,0,0,311,309,1,0,0,0,312,318,5,60,0,0,313,314,5,63,0,0,314,
	316,3,20,10,0,315,317,5,60,0,0,316,315,1,0,0,0,316,317,1,0,0,0,317,319,
	1,0,0,0,318,313,1,0,0,0,318,319,1,0,0,0,319,321,1,0,0,0,320,312,1,0,0,0,
	320,321,1,0,0,0,321,328,1,0,0,0,322,323,5,63,0,0,323,325,3,20,10,0,324,
	326,5,60,0,0,325,324,1,0,0,0,325,326,1,0,0,0,326,328,1,0,0,0,327,246,1,
	0,0,0,327,297,1,0,0,0,327,322,1,0,0,0,328,19,1,0,0,0,329,332,5,46,0,0,330,
	331,5,61,0,0,331,333,3,98,49,0,332,330,1,0,0,0,332,333,1,0,0,0,333,21,1,
	0,0,0,334,337,3,24,12,0,335,336,5,64,0,0,336,338,3,98,49,0,337,335,1,0,
	0,0,337,338,1,0,0,0,338,347,1,0,0,0,339,340,5,60,0,0,340,343,3,24,12,0,
	341,342,5,64,0,0,342,344,3,98,49,0,343,341,1,0,0,0,343,344,1,0,0,0,344,
	346,1,0,0,0,345,339,1,0,0,0,346,349,1,0,0,0,347,345,1,0,0,0,347,348,1,0,
	0,0,348,383,1,0,0,0,349,347,1,0,0,0,350,381,5,60,0,0,351,353,5,57,0,0,352,
	354,3,24,12,0,353,352,1,0,0,0,353,354,1,0,0,0,354,363,1,0,0,0,355,356,5,
	60,0,0,356,359,3,24,12,0,357,358,5,64,0,0,358,360,3,98,49,0,359,357,1,0,
	0,0,359,360,1,0,0,0,360,362,1,0,0,0,361,355,1,0,0,0,362,365,1,0,0,0,363,
	361,1,0,0,0,363,364,1,0,0,0,364,374,1,0,0,0,365,363,1,0,0,0,366,372,5,60,
	0,0,367,368,5,63,0,0,368,370,3,24,12,0,369,371,5,60,0,0,370,369,1,0,0,0,
	370,371,1,0,0,0,371,373,1,0,0,0,372,367,1,0,0,0,372,373,1,0,0,0,373,375,
	1,0,0,0,374,366,1,0,0,0,374,375,1,0,0,0,375,382,1,0,0,0,376,377,5,63,0,
	0,377,379,3,24,12,0,378,380,5,60,0,0,379,378,1,0,0,0,379,380,1,0,0,0,380,
	382,1,0,0,0,381,351,1,0,0,0,381,376,1,0,0,0,381,382,1,0,0,0,382,384,1,0,
	0,0,383,350,1,0,0,0,383,384,1,0,0,0,384,416,1,0,0,0,385,387,5,57,0,0,386,
	388,3,24,12,0,387,386,1,0,0,0,387,388,1,0,0,0,388,397,1,0,0,0,389,390,5,
	60,0,0,390,393,3,24,12,0,391,392,5,64,0,0,392,394,3,98,49,0,393,391,1,0,
	0,0,393,394,1,0,0,0,394,396,1,0,0,0,395,389,1,0,0,0,396,399,1,0,0,0,397,
	395,1,0,0,0,397,398,1,0,0,0,398,408,1,0,0,0,399,397,1,0,0,0,400,406,5,60,
	0,0,401,402,5,63,0,0,402,404,3,24,12,0,403,405,5,60,0,0,404,403,1,0,0,0,
	404,405,1,0,0,0,405,407,1,0,0,0,406,401,1,0,0,0,406,407,1,0,0,0,407,409,
	1,0,0,0,408,400,1,0,0,0,408,409,1,0,0,0,409,416,1,0,0,0,410,411,5,63,0,
	0,411,413,3,24,12,0,412,414,5,60,0,0,413,412,1,0,0,0,413,414,1,0,0,0,414,
	416,1,0,0,0,415,334,1,0,0,0,415,385,1,0,0,0,415,410,1,0,0,0,416,23,1,0,
	0,0,417,418,5,46,0,0,418,25,1,0,0,0,419,422,3,28,14,0,420,422,3,78,39,0,
	421,419,1,0,0,0,421,420,1,0,0,0,422,27,1,0,0,0,423,428,3,30,15,0,424,425,
	5,62,0,0,425,427,3,30,15,0,426,424,1,0,0,0,427,430,1,0,0,0,428,426,1,0,
	0,0,428,429,1,0,0,0,429,432,1,0,0,0,430,428,1,0,0,0,431,433,5,62,0,0,432,
	431,1,0,0,0,432,433,1,0,0,0,433,434,1,0,0,0,434,435,5,45,0,0,435,29,1,0,
	0,0,436,445,3,32,16,0,437,445,3,40,20,0,438,445,3,42,21,0,439,445,3,44,
	22,0,440,445,3,56,28,0,441,445,3,72,36,0,442,445,3,74,37,0,443,445,3,76,
	38,0,444,436,1,0,0,0,444,437,1,0,0,0,444,438,1,0,0,0,444,439,1,0,0,0,444,
	440,1,0,0,0,444,441,1,0,0,0,444,442,1,0,0,0,444,443,1,0,0,0,445,31,1,0,
	0,0,446,463,3,36,18,0,447,464,3,34,17,0,448,451,3,38,19,0,449,452,3,170,
	85,0,450,452,3,150,75,0,451,449,1,0,0,0,451,450,1,0,0,0,452,464,1,0,0,0,
	453,456,5,64,0,0,454,457,3,170,85,0,455,457,3,36,18,0,456,454,1,0,0,0,456,
	455,1,0,0,0,457,459,1,0,0,0,458,453,1,0,0,0,459,462,1,0,0,0,460,458,1,0,
	0,0,460,461,1,0,0,0,461,464,1,0,0,0,462,460,1,0,0,0,463,447,1,0,0,0,463,
	448,1,0,0,0,463,460,1,0,0,0,464,33,1,0,0,0,465,466,5,61,0,0,466,469,3,98,
	49,0,467,468,5,64,0,0,468,470,3,98,49,0,469,467,1,0,0,0,469,470,1,0,0,0,
	470,35,1,0,0,0,471,474,3,98,49,0,472,474,3,116,58,0,473,471,1,0,0,0,473,
	472,1,0,0,0,474,482,1,0,0,0,475,478,5,60,0,0,476,479,3,98,49,0,477,479,
	3,116,58,0,478,476,1,0,0,0,478,477,1,0,0,0,479,481,1,0,0,0,480,475,1,0,
	0,0,481,484,1,0,0,0,482,480,1,0,0,0,482,483,1,0,0,0,483,486,1,0,0,0,484,
	482,1,0,0,0,485,487,5,60,0,0,486,485,1,0,0,0,486,487,1,0,0,0,487,37,1,0,
	0,0,488,489,7,0,0,0,489,39,1,0,0,0,490,491,5,39,0,0,491,492,3,148,74,0,
	492,41,1,0,0,0,493,494,5,40,0,0,494,43,1,0,0,0,495,501,3,46,23,0,496,501,
	3,48,24,0,497,501,3,50,25,0,498,501,3,54,27,0,499,501,3,52,26,0,500,495,
	1,0,0,0,500,496,1,0,0,0,500,497,1,0,0,0,500,498,1,0,0,0,500,499,1,0,0,0,
	501,45,1,0,0,0,502,503,5,42,0,0,503,47,1,0,0,0,504,505,5,41,0,0,505,49,
	1,0,0,0,506,508,5,11,0,0,507,509,3,150,75,0,508,507,1,0,0,0,508,509,1,0,
	0,0,509,51,1,0,0,0,510,511,3,170,85,0,511,53,1,0,0,0,512,518,5,12,0,0,513,
	516,3,98,49,0,514,515,5,13,0,0,515,517,3,98,49,0,516,514,1,0,0,0,516,517,
	1,0,0,0,517,519,1,0,0,0,518,513,1,0,0,0,518,519,1,0,0,0,519,55,1,0,0,0,
	520,523,3,58,29,0,521,523,3,60,30,0,522,520,1,0,0,0,522,521,1,0,0,0,523,
	57,1,0,0,0,524,525,5,14,0,0,525,526,3,68,34,0,526,59,1,0,0,0,527,540,5,
	13,0,0,528,530,7,1,0,0,529,528,1,0,0,0,530,533,1,0,0,0,531,529,1,0,0,0,
	531,532,1,0,0,0,532,534,1,0,0,0,533,531,1,0,0,0,534,541,3,70,35,0,535,537,
	7,1,0,0,536,535,1,0,0,0,537,538,1,0,0,0,538,536,1,0,0,0,538,539,1,0,0,0,
	539,541,1,0,0,0,540,531,1,0,0,0,540,536,1,0,0,0,541,542,1,0,0,0,542,549,
	5,14,0,0,543,550,5,57,0,0,544,545,5,58,0,0,545,546,3,66,33,0,546,547,5,
	59,0,0,547,550,1,0,0,0,548,550,3,66,33,0,549,543,1,0,0,0,549,544,1,0,0,
	0,549,548,1,0,0,0,550,61,1,0,0,0,551,554,5,46,0,0,552,553,5,15,0,0,553,
	555,5,46,0,0,554,552,1,0,0,0,554,555,1,0,0,0,555,63,1,0,0,0,556,559,3,70,
	35,0,557,558,5,15,0,0,558,560,5,46,0,0,559,557,1,0,0,0,559,560,1,0,0,0,
	560,65,1,0,0,0,561,566,3,62,31,0,562,563,5,60,0,0,563,565,3,62,31,0,564,
	562,1,0,0,0,565,568,1,0,0,0,566,564,1,0,0,0,566,567,1,0,0,0,567,570,1,0,
	0,0,568,566,1,0,0,0,569,571,5,60,0,0,570,569,1,0,0,0,570,571,1,0,0,0,571,
	67,1,0,0,0,572,577,3,64,32,0,573,574,5,60,0,0,574,576,3,64,32,0,575,573,
	1,0,0,0,576,579,1,0,0,0,577,575,1,0,0,0,577,578,1,0,0,0,578,69,1,0,0,0,
	579,577,1,0,0,0,580,585,5,46,0,0,581,582,5,55,0,0,582,584,5,46,0,0,583,
	581,1,0,0,0,584,587,1,0,0,0,585,583,1,0,0,0,585,586,1,0,0,0,586,71,1,0,
	0,0,587,585,1,0,0,0,588,589,5,16,0,0,589,594,5,46,0,0,590,591,5,60,0,0,
	591,593,5,46,0,0,592,590,1,0,0,0,593,596,1,0,0,0,594,592,1,0,0,0,594,595,
	1,0,0,0,595,73,1,0,0,0,596,594,1,0,0,0,597,598,5,17,0,0,598,603,5,46,0,
	0,599,600,5,60,0,0,600,602,5,46,0,0,601,599,1,0,0,0,602,605,1,0,0,0,603,
	601,1,0,0,0,603,604,1,0,0,0,604,75,1,0,0,0,605,603,1,0,0,0,606,607,5,18,
	0,0,607,610,3,98,49,0,608,609,5,60,0,0,609,611,3,98,49,0,610,608,1,0,0,
	0,610,611,1,0,0,0,611,77,1,0,0,0,612,622,3,82,41,0,613,622,3,84,42,0,614,
	622,3,86,43,0,615,622,3,88,44,0,616,622,3,90,45,0,617,622,3,14,7,0,618,
	622,3,154,77,0,619,622,3,10,5,0,620,622,3,80,40,0,621,612,1,0,0,0,621,613,
	1,0,0,0,621,614,1,0,0,0,621,615,1,0,0,0,621,616,1,0,0,0,621,617,1,0,0,0,
	621,618,1,0,0,0,621,619,1,0,0,0,621,620,1,0,0,0,622,79,1,0,0,0,623,627,
	5,43,0,0,624,628,3,14,7,0,625,628,3,90,45,0,626,628,3,86,43,0,627,624,1,
	0,0,0,627,625,1,0,0,0,627,626,1,0,0,0,628,81,1,0,0,0,629,630,5,19,0,0,630,
	631,3,98,49,0,631,632,5,61,0,0,632,640,3,96,48,0,633,634,5,20,0,0,634,635,
	3,98,49,0,635,636,5,61,0,0,636,637,3,96,48,0,637,639,1,0,0,0,638,633,1,
	0,0,0,639,642,1,0,0,0,640,638,1,0,0,0,640,641,1,0,0,0,641,646,1,0,0,0,642,
	640,1,0,0,0,643,644,5,21,0,0,644,645,5,61,0,0,645,647,3,96,48,0,646,643,
	1,0,0,0,646,647,1,0,0,0,647,83,1,0,0,0,648,649,5,22,0,0,649,650,3,98,49,
	0,650,651,5,61,0,0,651,655,3,96,48,0,652,653,5,21,0,0,653,654,5,61,0,0,
	654,656,3,96,48,0,655,652,1,0,0,0,655,656,1,0,0,0,656,85,1,0,0,0,657,658,
	5,23,0,0,658,659,3,148,74,0,659,660,5,24,0,0,660,661,3,150,75,0,661,662,
	5,61,0,0,662,666,3,96,48,0,663,664,5,21,0,0,664,665,5,61,0,0,665,667,3,
	96,48,0,666,663,1,0,0,0,666,667,1,0,0,0,667,87,1,0,0,0,668,669,5,25,0,0,
	669,670,5,61,0,0,670,692,3,96,48,0,671,672,3,94,47,0,672,673,5,61,0,0,673,
	674,3,96,48,0,674,676,1,0,0,0,675,671,1,0,0,0,676,677,1,0,0,0,677,675,1,
	0,0,0,677,678,1,0,0,0,678,682,1,0,0,0,679,680,5,21,0,0,680,681,5,61,0,0,
	681,683,3,96,48,0,682,679,1,0,0,0,682,683,1,0,0,0,683,687,1,0,0,0,684,685,
	5,26,0,0,685,686,5,61,0,0,686,688,3,96,48,0,687,684,1,0,0,0,687,688,1,0,
	0,0,688,693,1,0,0,0,689,690,5,26,0,0,690,691,5,61,0,0,691,693,3,96,48,0,
	692,675,1,0,0,0,692,689,1,0,0,0,693,89,1,0,0,0,694,695,5,27,0,0,695,700,
	3,92,46,0,696,697,5,60,0,0,697,699,3,92,46,0,698,696,1,0,0,0,699,702,1,
	0,0,0,700,698,1,0,0,0,700,701,1,0,0,0,701,703,1,0,0,0,702,700,1,0,0,0,703,
	704,5,61,0,0,704,705,3,96,48,0,705,91,1,0,0,0,706,709,3,98,49,0,707,708,
	5,15,0,0,708,710,3,118,59,0,709,707,1,0,0,0,709,710,1,0,0,0,710,93,1,0,
	0,0,711,717,5,28,0,0,712,715,3,98,49,0,713,714,5,15,0,0,714,716,5,46,0,
	0,715,713,1,0,0,0,715,716,1,0,0,0,716,718,1,0,0,0,717,712,1,0,0,0,717,718,
	1,0,0,0,718,95,1,0,0,0,719,730,3,28,14,0,720,721,5,45,0,0,721,723,5,1,0,
	0,722,724,3,26,13,0,723,722,1,0,0,0,724,725,1,0,0,0,725,723,1,0,0,0,725,
	726,1,0,0,0,726,727,1,0,0,0,727,728,5,2,0,0,728,730,1,0,0,0,729,719,1,0,
	0,0,729,720,1,0,0,0,730,97,1,0,0,0,731,737,3,106,53,0,732,733,5,19,0,0,
	733,734,3,106,53,0,734,735,5,21,0,0,735,736,3,98,49,0,736,738,1,0,0,0,737,
	732,1,0,0,0,737,738,1,0,0,0,738,741,1,0,0,0,739,741,3,102,51,0,740,731,
	1,0,0,0,740,739,1,0,0,0,741,99,1,0,0,0,742,745,3,106,53,0,743,745,3,104,
	52,0,744,742,1,0,0,0,744,743,1,0,0,0,745,101,1,0,0,0,746,748,5,29,0,0,747,
	749,3,22,11,0,748,747,1,0,0,0,748,749,1,0,0,0,749,750,1,0,0,0,750,751,5,
	61,0,0,751,752,3,98,49,0,752,103,1,0,0,0,753,755,5,29,0,0,754,756,3,22,
	11,0,755,754,1,0,0,0,755,756,1,0,0,0,756,757,1,0,0,0,757,758,5,61,0,0,758,
	759,3,100,50,0,759,105,1,0,0,0,760,765,3,108,54,0,761,762,5,30,0,0,762,
	764,3,108,54,0,763,761,1,0,0,0,764,767,1,0,0,0,765,763,1,0,0,0,765,766,
	1,0,0,0,766,107,1,0,0,0,767,765,1,0,0,0,768,773,3,110,55,0,769,770,5,31,
	0,0,770,772,3,110,55,0,771,769,1,0,0,0,772,775,1,0,0,0,773,771,1,0,0,0,
	773,774,1,0,0,0,774,109,1,0,0,0,775,773,1,0,0,0,776,777,5,32,0,0,777,780,
	3,110,55,0,778,780,3,112,56,0,779,776,1,0,0,0,779,778,1,0,0,0,780,111,1,
	0,0,0,781,787,3,118,59,0,782,783,3,114,57,0,783,784,3,118,59,0,784,786,
	1,0,0,0,785,782,1,0,0,0,786,789,1,0,0,0,787,785,1,0,0,0,787,788,1,0,0,0,
	788,113,1,0,0,0,789,787,1,0,0,0,790,804,5,81,0,0,791,804,5,82,0,0,792,804,
	5,83,0,0,793,804,5,84,0,0,794,804,5,85,0,0,795,804,5,86,0,0,796,804,5,87,
	0,0,797,804,5,24,0,0,798,799,5,32,0,0,799,804,5,24,0,0,800,804,5,33,0,0,
	801,802,5,33,0,0,802,804,5,32,0,0,803,790,1,0,0,0,803,791,1,0,0,0,803,792,
	1,0,0,0,803,793,1,0,0,0,803,794,1,0,0,0,803,795,1,0,0,0,803,796,1,0,0,0,
	803,797,1,0,0,0,803,798,1,0,0,0,803,800,1,0,0,0,803,801,1,0,0,0,804,115,
	1,0,0,0,805,806,5,57,0,0,806,807,3,118,59,0,807,117,1,0,0,0,808,813,3,120,
	60,0,809,810,5,67,0,0,810,812,3,120,60,0,811,809,1,0,0,0,812,815,1,0,0,
	0,813,811,1,0,0,0,813,814,1,0,0,0,814,119,1,0,0,0,815,813,1,0,0,0,816,821,
	3,122,61,0,817,818,5,68,0,0,818,820,3,122,61,0,819,817,1,0,0,0,820,823,
	1,0,0,0,821,819,1,0,0,0,821,822,1,0,0,0,822,121,1,0,0,0,823,821,1,0,0,0,
	824,829,3,124,62,0,825,826,5,69,0,0,826,828,3,124,62,0,827,825,1,0,0,0,
	828,831,1,0,0,0,829,827,1,0,0,0,829,830,1,0,0,0,830,123,1,0,0,0,831,829,
	1,0,0,0,832,837,3,126,63,0,833,834,7,2,0,0,834,836,3,126,63,0,835,833,1,
	0,0,0,836,839,1,0,0,0,837,835,1,0,0,0,837,838,1,0,0,0,838,125,1,0,0,0,839,
	837,1,0,0,0,840,845,3,128,64,0,841,842,7,3,0,0,842,844,3,128,64,0,843,841,
	1,0,0,0,844,847,1,0,0,0,845,843,1,0,0,0,845,846,1,0,0,0,846,127,1,0,0,0,
	847,845,1,0,0,0,848,853,3,130,65,0,849,850,7,4,0,0,850,852,3,130,65,0,851,
	849,1,0,0,0,852,855,1,0,0,0,853,851,1,0,0,0,853,854,1,0,0,0,854,129,1,0,
	0,0,855,853,1,0,0,0,856,857,7,5,0,0,857,860,3,130,65,0,858,860,3,132,66,
	0,859,856,1,0,0,0,859,858,1,0,0,0,860,131,1,0,0,0,861,864,3,134,67,0,862,
	863,5,63,0,0,863,865,3,130,65,0,864,862,1,0,0,0,864,865,1,0,0,0,865,133,
	1,0,0,0,866,868,5,44,0,0,867,866,1,0,0,0,867,868,1,0,0,0,868,869,1,0,0,
	0,869,873,3,136,68,0,870,872,3,140,70,0,871,870,1,0,0,0,872,875,1,0,0,0,
	873,871,1,0,0,0,873,874,1,0,0,0,874,135,1,0,0,0,875,873,1,0,0,0,876,879,
	5,58,0,0,877,880,3,170,85,0,878,880,3,138,69,0,879,877,1,0,0,0,879,878,
	1,0,0,0,879,880,1,0,0,0,880,881,1,0,0,0,881,909,5,59,0,0,882,884,5,65,0,
	0,883,885,3,138,69,0,884,883,1,0,0,0,884,885,1,0,0,0,885,886,1,0,0,0,886,
	909,5,66,0,0,887,889,5,78,0,0,888,890,3,152,76,0,889,888,1,0,0,0,889,890,
	1,0,0,0,890,891,1,0,0,0,891,909,5,80,0,0,892,909,5,46,0,0,893,909,5,8,0,
	0,894,896,3,174,87,0,895,894,1,0,0,0,896,897,1,0,0,0,897,895,1,0,0,0,897,
	898,1,0,0,0,898,909,1,0,0,0,899,901,5,7,0,0,900,899,1,0,0,0,901,902,1,0,
	0,0,902,900,1,0,0,0,902,903,1,0,0,0,903,909,1,0,0,0,904,909,5,56,0,0,905,
	909,5,34,0,0,906,909,5,35,0,0,907,909,5,36,0,0,908,876,1,0,0,0,908,882,
	1,0,0,0,908,887,1,0,0,0,908,892,1,0,0,0,908,893,1,0,0,0,908,895,1,0,0,0,
	908,900,1,0,0,0,908,904,1,0,0,0,908,905,1,0,0,0,908,906,1,0,0,0,908,907,
	1,0,0,0,909,137,1,0,0,0,910,913,3,98,49,0,911,913,3,116,58,0,912,910,1,
	0,0,0,912,911,1,0,0,0,913,928,1,0,0,0,914,929,3,164,82,0,915,918,5,60,0,
	0,916,919,3,98,49,0,917,919,3,116,58,0,918,916,1,0,0,0,918,917,1,0,0,0,
	919,921,1,0,0,0,920,915,1,0,0,0,921,924,1,0,0,0,922,920,1,0,0,0,922,923,
	1,0,0,0,923,926,1,0,0,0,924,922,1,0,0,0,925,927,5,60,0,0,926,925,1,0,0,
	0,926,927,1,0,0,0,927,929,1,0,0,0,928,914,1,0,0,0,928,922,1,0,0,0,929,139,
	1,0,0,0,930,938,3,156,78,0,931,932,5,65,0,0,932,933,3,142,71,0,933,934,
	5,66,0,0,934,938,1,0,0,0,935,936,5,55,0,0,936,938,5,46,0,0,937,930,1,0,
	0,0,937,931,1,0,0,0,937,935,1,0,0,0,938,141,1,0,0,0,939,944,3,144,72,0,
	940,941,5,60,0,0,941,943,3,144,72,0,942,940,1,0,0,0,943,946,1,0,0,0,944,
	942,1,0,0,0,944,945,1,0,0,0,945,948,1,0,0,0,946,944,1,0,0,0,947,949,5,60,
	0,0,948,947,1,0,0,0,948,949,1,0,0,0,949,143,1,0,0,0,950,962,3,98,49,0,951,
	953,3,98,49,0,952,951,1,0,0,0,952,953,1,0,0,0,953,954,1,0,0,0,954,956,5,
	61,0,0,955,957,3,98,49,0,956,955,1,0,0,0,956,957,1,0,0,0,957,959,1,0,0,
	0,958,960,3,146,73,0,959,958,1,0,0,0,959,960,1,0,0,0,960,962,1,0,0,0,961,
	950,1,0,0,0,961,952,1,0,0,0,962,145,1,0,0,0,963,965,5,61,0,0,964,966,3,
	98,49,0,965,964,1,0,0,0,965,966,1,0,0,0,966,147,1,0,0,0,967,970,3,118,59,
	0,968,970,3,116,58,0,969,967,1,0,0,0,969,968,1,0,0,0,970,978,1,0,0,0,971,
	974,5,60,0,0,972,975,3,118,59,0,973,975,3,116,58,0,974,972,1,0,0,0,974,
	973,1,0,0,0,975,977,1,0,0,0,976,971,1,0,0,0,977,980,1,0,0,0,978,976,1,0,
	0,0,978,979,1,0,0,0,979,982,1,0,0,0,980,978,1,0,0,0,981,983,5,60,0,0,982,
	981,1,0,0,0,982,983,1,0,0,0,983,149,1,0,0,0,984,989,3,98,49,0,985,986,5,
	60,0,0,986,988,3,98,49,0,987,985,1,0,0,0,988,991,1,0,0,0,989,987,1,0,0,
	0,989,990,1,0,0,0,990,993,1,0,0,0,991,989,1,0,0,0,992,994,5,60,0,0,993,
	992,1,0,0,0,993,994,1,0,0,0,994,151,1,0,0,0,995,996,3,98,49,0,996,997,5,
	61,0,0,997,998,3,98,49,0,998,1002,1,0,0,0,999,1000,5,63,0,0,1000,1002,3,
	118,59,0,1001,995,1,0,0,0,1001,999,1,0,0,0,1002,1021,1,0,0,0,1003,1022,
	3,164,82,0,1004,1011,5,60,0,0,1005,1006,3,98,49,0,1006,1007,5,61,0,0,1007,
	1008,3,98,49,0,1008,1012,1,0,0,0,1009,1010,5,63,0,0,1010,1012,3,118,59,
	0,1011,1005,1,0,0,0,1011,1009,1,0,0,0,1012,1014,1,0,0,0,1013,1004,1,0,0,
	0,1014,1017,1,0,0,0,1015,1013,1,0,0,0,1015,1016,1,0,0,0,1016,1019,1,0,0,
	0,1017,1015,1,0,0,0,1018,1020,5,60,0,0,1019,1018,1,0,0,0,1019,1020,1,0,
	0,0,1020,1022,1,0,0,0,1021,1003,1,0,0,0,1021,1015,1,0,0,0,1022,1044,1,0,
	0,0,1023,1026,3,98,49,0,1024,1026,3,116,58,0,1025,1023,1,0,0,0,1025,1024,
	1,0,0,0,1026,1041,1,0,0,0,1027,1042,3,164,82,0,1028,1031,5,60,0,0,1029,
	1032,3,98,49,0,1030,1032,3,116,58,0,1031,1029,1,0,0,0,1031,1030,1,0,0,0,
	1032,1034,1,0,0,0,1033,1028,1,0,0,0,1034,1037,1,0,0,0,1035,1033,1,0,0,0,
	1035,1036,1,0,0,0,1036,1039,1,0,0,0,1037,1035,1,0,0,0,1038,1040,5,60,0,
	0,1039,1038,1,0,0,0,1039,1040,1,0,0,0,1040,1042,1,0,0,0,1041,1027,1,0,0,
	0,1041,1035,1,0,0,0,1042,1044,1,0,0,0,1043,1001,1,0,0,0,1043,1025,1,0,0,
	0,1044,153,1,0,0,0,1045,1046,5,37,0,0,1046,1052,5,46,0,0,1047,1049,5,58,
	0,0,1048,1050,3,158,79,0,1049,1048,1,0,0,0,1049,1050,1,0,0,0,1050,1051,
	1,0,0,0,1051,1053,5,59,0,0,1052,1047,1,0,0,0,1052,1053,1,0,0,0,1053,1054,
	1,0,0,0,1054,1055,5,61,0,0,1055,1056,3,96,48,0,1056,155,1,0,0,0,1057,1059,
	5,58,0,0,1058,1060,3,158,79,0,1059,1058,1,0,0,0,1059,1060,1,0,0,0,1060,
	1061,1,0,0,0,1061,1062,5,59,0,0,1062,157,1,0,0,0,1063,1068,3,160,80,0,1064,
	1065,5,60,0,0,1065,1067,3,160,80,0,1066,1064,1,0,0,0,1067,1070,1,0,0,0,
	1068,1066,1,0,0,0,1068,1069,1,0,0,0,1069,1072,1,0,0,0,1070,1068,1,0,0,0,
	1071,1073,5,60,0,0,1072,1071,1,0,0,0,1072,1073,1,0,0,0,1073,159,1,0,0,0,
	1074,1076,3,98,49,0,1075,1077,3,164,82,0,1076,1075,1,0,0,0,1076,1077,1,
	0,0,0,1077,1087,1,0,0,0,1078,1079,3,98,49,0,1079,1080,5,64,0,0,1080,1081,
	3,98,49,0,1081,1087,1,0,0,0,1082,1083,5,63,0,0,1083,1087,3,98,49,0,1084,
	1085,5,57,0,0,1085,1087,3,98,49,0,1086,1074,1,0,0,0,1086,1078,1,0,0,0,1086,
	1082,1,0,0,0,1086,1084,1,0,0,0,1087,161,1,0,0,0,1088,1091,3,164,82,0,1089,
	1091,3,166,83,0,1090,1088,1,0,0,0,1090,1089,1,0,0,0,1091,163,1,0,0,0,1092,
	1094,5,43,0,0,1093,1092,1,0,0,0,1093,1094,1,0,0,0,1094,1095,1,0,0,0,1095,
	1096,5,23,0,0,1096,1097,3,148,74,0,1097,1098,5,24,0,0,1098,1100,3,106,53,
	0,1099,1101,3,162,81,0,1100,1099,1,0,0,0,1100,1101,1,0,0,0,1101,165,1,0,
	0,0,1102,1103,5,19,0,0,1103,1105,3,100,50,0,1104,1106,3,162,81,0,1105,1104,
	1,0,0,0,1105,1106,1,0,0,0,1106,167,1,0,0,0,1107,1108,5,46,0,0,1108,169,
	1,0,0,0,1109,1111,5,38,0,0,1110,1112,3,172,86,0,1111,1110,1,0,0,0,1111,
	1112,1,0,0,0,1112,171,1,0,0,0,1113,1114,5,13,0,0,1114,1117,3,98,49,0,1115,
	1117,3,150,75,0,1116,1113,1,0,0,0,1116,1115,1,0,0,0,1117,173,1,0,0,0,1118,
	1122,5,3,0,0,1119,1121,3,176,88,0,1120,1119,1,0,0,0,1121,1124,1,0,0,0,1122,
	1120,1,0,0,0,1122,1123,1,0,0,0,1123,1125,1,0,0,0,1124,1122,1,0,0,0,1125,
	1151,5,107,0,0,1126,1130,5,5,0,0,1127,1129,3,176,88,0,1128,1127,1,0,0,0,
	1129,1132,1,0,0,0,1130,1128,1,0,0,0,1130,1131,1,0,0,0,1131,1133,1,0,0,0,
	1132,1130,1,0,0,0,1133,1151,5,108,0,0,1134,1138,5,4,0,0,1135,1137,3,178,
	89,0,1136,1135,1,0,0,0,1137,1140,1,0,0,0,1138,1136,1,0,0,0,1138,1139,1,
	0,0,0,1139,1141,1,0,0,0,1140,1138,1,0,0,0,1141,1151,5,110,0,0,1142,1146,
	5,6,0,0,1143,1145,3,178,89,0,1144,1143,1,0,0,0,1145,1148,1,0,0,0,1146,1144,
	1,0,0,0,1146,1147,1,0,0,0,1147,1149,1,0,0,0,1148,1146,1,0,0,0,1149,1151,
	5,111,0,0,1150,1118,1,0,0,0,1150,1126,1,0,0,0,1150,1134,1,0,0,0,1150,1142,
	1,0,0,0,1151,175,1,0,0,0,1152,1161,5,109,0,0,1153,1156,5,78,0,0,1154,1157,
	3,98,49,0,1155,1157,3,116,58,0,1156,1154,1,0,0,0,1156,1155,1,0,0,0,1157,
	1158,1,0,0,0,1158,1159,5,79,0,0,1159,1161,1,0,0,0,1160,1152,1,0,0,0,1160,
	1153,1,0,0,0,1161,177,1,0,0,0,1162,1171,5,112,0,0,1163,1166,5,78,0,0,1164,
	1167,3,98,49,0,1165,1167,3,116,58,0,1166,1164,1,0,0,0,1166,1165,1,0,0,0,
	1167,1168,1,0,0,0,1168,1169,5,79,0,0,1169,1171,1,0,0,0,1170,1162,1,0,0,
	0,1170,1163,1,0,0,0,1171,179,1,0,0,0,176,182,184,194,200,209,212,219,225,
	235,242,249,255,259,265,271,275,282,284,286,291,293,295,299,305,309,316,
	318,320,325,327,332,337,343,347,353,359,363,370,372,374,379,381,383,387,
	393,397,404,406,408,413,415,421,428,432,444,451,456,460,463,469,473,478,
	482,486,500,508,516,518,522,531,538,540,549,554,559,566,570,577,585,594,
	603,610,621,627,640,646,655,666,677,682,687,692,700,709,715,717,725,729,
	737,740,744,748,755,765,773,779,787,803,813,821,829,837,845,853,859,864,
	867,873,879,884,889,897,902,908,912,918,922,926,928,937,944,948,952,956,
	959,961,965,969,974,978,982,989,993,1001,1011,1015,1019,1021,1025,1031,
	1035,1039,1041,1043,1049,1052,1059,1068,1072,1076,1086,1090,1093,1100,1105,
	1111,1116,1122,1130,1138,1146,1150,1156,1160,1166,1170];

	private static __ATN: ATN;
	public static get _ATN(): ATN {
		if (!Python3Parser.__ATN) {
			Python3Parser.__ATN = new ATNDeserializer().deserialize(Python3Parser._serializedATN);
		}

		return Python3Parser.__ATN;
	}


	static DecisionsToDFA = Python3Parser._ATN.decisionToState.map( (ds: DecisionState, index: number) => new DFA(ds, index) );

}

export class File_inputContext extends ParserRuleContext {
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public EOF(): TerminalNode {
		return this.getToken(Python3Parser.EOF, 0);
	}
	public NEWLINE_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.NEWLINE);
	}
	public NEWLINE(i: number): TerminalNode {
		return this.getToken(Python3Parser.NEWLINE, i);
	}
	public stmt_list(): StmtContext[] {
		return this.getTypedRuleContexts(StmtContext) as StmtContext[];
	}
	public stmt(i: number): StmtContext {
		return this.getTypedRuleContext(StmtContext, i) as StmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_file_input;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterFile_input) {
	 		listener.enterFile_input(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitFile_input) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NEWLINE(): TerminalNode {
		return this.getToken(Python3Parser.NEWLINE, 0);
	}
	public simple_stmt(): Simple_stmtContext {
		return this.getTypedRuleContext(Simple_stmtContext, 0) as Simple_stmtContext;
	}
	public compound_stmt(): Compound_stmtContext {
		return this.getTypedRuleContext(Compound_stmtContext, 0) as Compound_stmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_single_input;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSingle_input) {
	 		listener.enterSingle_input(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSingle_input) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public testlist(): TestlistContext {
		return this.getTypedRuleContext(TestlistContext, 0) as TestlistContext;
	}
	public EOF(): TerminalNode {
		return this.getToken(Python3Parser.EOF, 0);
	}
	public NEWLINE_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.NEWLINE);
	}
	public NEWLINE(i: number): TerminalNode {
		return this.getToken(Python3Parser.NEWLINE, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_eval_input;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterEval_input) {
	 		listener.enterEval_input(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitEval_input) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public AT(): TerminalNode {
		return this.getToken(Python3Parser.AT, 0);
	}
	public dotted_name(): Dotted_nameContext {
		return this.getTypedRuleContext(Dotted_nameContext, 0) as Dotted_nameContext;
	}
	public NEWLINE(): TerminalNode {
		return this.getToken(Python3Parser.NEWLINE, 0);
	}
	public OPEN_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_PAREN, 0);
	}
	public CLOSE_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_PAREN, 0);
	}
	public arglist(): ArglistContext {
		return this.getTypedRuleContext(ArglistContext, 0) as ArglistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_decorator;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDecorator) {
	 		listener.enterDecorator(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDecorator) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public decorator_list(): DecoratorContext[] {
		return this.getTypedRuleContexts(DecoratorContext) as DecoratorContext[];
	}
	public decorator(i: number): DecoratorContext {
		return this.getTypedRuleContext(DecoratorContext, i) as DecoratorContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_decorators;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDecorators) {
	 		listener.enterDecorators(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDecorators) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public decorators(): DecoratorsContext {
		return this.getTypedRuleContext(DecoratorsContext, 0) as DecoratorsContext;
	}
	public classdef(): ClassdefContext {
		return this.getTypedRuleContext(ClassdefContext, 0) as ClassdefContext;
	}
	public funcdef(): FuncdefContext {
		return this.getTypedRuleContext(FuncdefContext, 0) as FuncdefContext;
	}
	public async_funcdef(): Async_funcdefContext {
		return this.getTypedRuleContext(Async_funcdefContext, 0) as Async_funcdefContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_decorated;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDecorated) {
	 		listener.enterDecorated(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDecorated) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public ASYNC(): TerminalNode {
		return this.getToken(Python3Parser.ASYNC, 0);
	}
	public funcdef(): FuncdefContext {
		return this.getTypedRuleContext(FuncdefContext, 0) as FuncdefContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_async_funcdef;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAsync_funcdef) {
	 		listener.enterAsync_funcdef(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAsync_funcdef) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public DEF(): TerminalNode {
		return this.getToken(Python3Parser.DEF, 0);
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
	public parameters(): ParametersContext {
		return this.getTypedRuleContext(ParametersContext, 0) as ParametersContext;
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public suite(): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, 0) as SuiteContext;
	}
	public ARROW(): TerminalNode {
		return this.getToken(Python3Parser.ARROW, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_funcdef;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterFuncdef) {
	 		listener.enterFuncdef(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitFuncdef) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public OPEN_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_PAREN, 0);
	}
	public CLOSE_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_PAREN, 0);
	}
	public typedargslist(): TypedargslistContext {
		return this.getTypedRuleContext(TypedargslistContext, 0) as TypedargslistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_parameters;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterParameters) {
	 		listener.enterParameters(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitParameters) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public tfpdef_list(): TfpdefContext[] {
		return this.getTypedRuleContexts(TfpdefContext) as TfpdefContext[];
	}
	public tfpdef(i: number): TfpdefContext {
		return this.getTypedRuleContext(TfpdefContext, i) as TfpdefContext;
	}
	public STAR(): TerminalNode {
		return this.getToken(Python3Parser.STAR, 0);
	}
	public POWER(): TerminalNode {
		return this.getToken(Python3Parser.POWER, 0);
	}
	public ASSIGN_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.ASSIGN);
	}
	public ASSIGN(i: number): TerminalNode {
		return this.getToken(Python3Parser.ASSIGN, i);
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_typedargslist;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTypedargslist) {
	 		listener.enterTypedargslist(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTypedargslist) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_tfpdef;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTfpdef) {
	 		listener.enterTfpdef(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTfpdef) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public vfpdef_list(): VfpdefContext[] {
		return this.getTypedRuleContexts(VfpdefContext) as VfpdefContext[];
	}
	public vfpdef(i: number): VfpdefContext {
		return this.getTypedRuleContext(VfpdefContext, i) as VfpdefContext;
	}
	public STAR(): TerminalNode {
		return this.getToken(Python3Parser.STAR, 0);
	}
	public POWER(): TerminalNode {
		return this.getToken(Python3Parser.POWER, 0);
	}
	public ASSIGN_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.ASSIGN);
	}
	public ASSIGN(i: number): TerminalNode {
		return this.getToken(Python3Parser.ASSIGN, i);
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_varargslist;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterVarargslist) {
	 		listener.enterVarargslist(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitVarargslist) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_vfpdef;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterVfpdef) {
	 		listener.enterVfpdef(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitVfpdef) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public simple_stmt(): Simple_stmtContext {
		return this.getTypedRuleContext(Simple_stmtContext, 0) as Simple_stmtContext;
	}
	public compound_stmt(): Compound_stmtContext {
		return this.getTypedRuleContext(Compound_stmtContext, 0) as Compound_stmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterStmt) {
	 		listener.enterStmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitStmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public small_stmt_list(): Small_stmtContext[] {
		return this.getTypedRuleContexts(Small_stmtContext) as Small_stmtContext[];
	}
	public small_stmt(i: number): Small_stmtContext {
		return this.getTypedRuleContext(Small_stmtContext, i) as Small_stmtContext;
	}
	public NEWLINE(): TerminalNode {
		return this.getToken(Python3Parser.NEWLINE, 0);
	}
	public SEMI_COLON_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.SEMI_COLON);
	}
	public SEMI_COLON(i: number): TerminalNode {
		return this.getToken(Python3Parser.SEMI_COLON, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_simple_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSimple_stmt) {
	 		listener.enterSimple_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSimple_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public expr_stmt(): Expr_stmtContext {
		return this.getTypedRuleContext(Expr_stmtContext, 0) as Expr_stmtContext;
	}
	public del_stmt(): Del_stmtContext {
		return this.getTypedRuleContext(Del_stmtContext, 0) as Del_stmtContext;
	}
	public pass_stmt(): Pass_stmtContext {
		return this.getTypedRuleContext(Pass_stmtContext, 0) as Pass_stmtContext;
	}
	public flow_stmt(): Flow_stmtContext {
		return this.getTypedRuleContext(Flow_stmtContext, 0) as Flow_stmtContext;
	}
	public import_stmt(): Import_stmtContext {
		return this.getTypedRuleContext(Import_stmtContext, 0) as Import_stmtContext;
	}
	public global_stmt(): Global_stmtContext {
		return this.getTypedRuleContext(Global_stmtContext, 0) as Global_stmtContext;
	}
	public nonlocal_stmt(): Nonlocal_stmtContext {
		return this.getTypedRuleContext(Nonlocal_stmtContext, 0) as Nonlocal_stmtContext;
	}
	public assert_stmt(): Assert_stmtContext {
		return this.getTypedRuleContext(Assert_stmtContext, 0) as Assert_stmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_small_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSmall_stmt) {
	 		listener.enterSmall_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSmall_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public testlist_star_expr_list(): Testlist_star_exprContext[] {
		return this.getTypedRuleContexts(Testlist_star_exprContext) as Testlist_star_exprContext[];
	}
	public testlist_star_expr(i: number): Testlist_star_exprContext {
		return this.getTypedRuleContext(Testlist_star_exprContext, i) as Testlist_star_exprContext;
	}
	public annassign(): AnnassignContext {
		return this.getTypedRuleContext(AnnassignContext, 0) as AnnassignContext;
	}
	public augassign(): AugassignContext {
		return this.getTypedRuleContext(AugassignContext, 0) as AugassignContext;
	}
	public yield_expr_list(): Yield_exprContext[] {
		return this.getTypedRuleContexts(Yield_exprContext) as Yield_exprContext[];
	}
	public yield_expr(i: number): Yield_exprContext {
		return this.getTypedRuleContext(Yield_exprContext, i) as Yield_exprContext;
	}
	public testlist(): TestlistContext {
		return this.getTypedRuleContext(TestlistContext, 0) as TestlistContext;
	}
	public ASSIGN_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.ASSIGN);
	}
	public ASSIGN(i: number): TerminalNode {
		return this.getToken(Python3Parser.ASSIGN, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_expr_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterExpr_stmt) {
	 		listener.enterExpr_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitExpr_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.ASSIGN, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_annassign;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAnnassign) {
	 		listener.enterAnnassign(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAnnassign) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public star_expr_list(): Star_exprContext[] {
		return this.getTypedRuleContexts(Star_exprContext) as Star_exprContext[];
	}
	public star_expr(i: number): Star_exprContext {
		return this.getTypedRuleContext(Star_exprContext, i) as Star_exprContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_testlist_star_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTestlist_star_expr) {
	 		listener.enterTestlist_star_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTestlist_star_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public ADD_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.ADD_ASSIGN, 0);
	}
	public SUB_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.SUB_ASSIGN, 0);
	}
	public MULT_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.MULT_ASSIGN, 0);
	}
	public AT_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.AT_ASSIGN, 0);
	}
	public DIV_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.DIV_ASSIGN, 0);
	}
	public MOD_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.MOD_ASSIGN, 0);
	}
	public AND_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.AND_ASSIGN, 0);
	}
	public OR_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.OR_ASSIGN, 0);
	}
	public XOR_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.XOR_ASSIGN, 0);
	}
	public LEFT_SHIFT_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.LEFT_SHIFT_ASSIGN, 0);
	}
	public RIGHT_SHIFT_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.RIGHT_SHIFT_ASSIGN, 0);
	}
	public POWER_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.POWER_ASSIGN, 0);
	}
	public IDIV_ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.IDIV_ASSIGN, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_augassign;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAugassign) {
	 		listener.enterAugassign(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAugassign) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public DEL(): TerminalNode {
		return this.getToken(Python3Parser.DEL, 0);
	}
	public exprlist(): ExprlistContext {
		return this.getTypedRuleContext(ExprlistContext, 0) as ExprlistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_del_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDel_stmt) {
	 		listener.enterDel_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDel_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public PASS(): TerminalNode {
		return this.getToken(Python3Parser.PASS, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_pass_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterPass_stmt) {
	 		listener.enterPass_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitPass_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public break_stmt(): Break_stmtContext {
		return this.getTypedRuleContext(Break_stmtContext, 0) as Break_stmtContext;
	}
	public continue_stmt(): Continue_stmtContext {
		return this.getTypedRuleContext(Continue_stmtContext, 0) as Continue_stmtContext;
	}
	public return_stmt(): Return_stmtContext {
		return this.getTypedRuleContext(Return_stmtContext, 0) as Return_stmtContext;
	}
	public raise_stmt(): Raise_stmtContext {
		return this.getTypedRuleContext(Raise_stmtContext, 0) as Raise_stmtContext;
	}
	public yield_stmt(): Yield_stmtContext {
		return this.getTypedRuleContext(Yield_stmtContext, 0) as Yield_stmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_flow_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterFlow_stmt) {
	 		listener.enterFlow_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitFlow_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public BREAK(): TerminalNode {
		return this.getToken(Python3Parser.BREAK, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_break_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterBreak_stmt) {
	 		listener.enterBreak_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitBreak_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public CONTINUE(): TerminalNode {
		return this.getToken(Python3Parser.CONTINUE, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_continue_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterContinue_stmt) {
	 		listener.enterContinue_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitContinue_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public RETURN(): TerminalNode {
		return this.getToken(Python3Parser.RETURN, 0);
	}
	public testlist(): TestlistContext {
		return this.getTypedRuleContext(TestlistContext, 0) as TestlistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_return_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterReturn_stmt) {
	 		listener.enterReturn_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitReturn_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public yield_expr(): Yield_exprContext {
		return this.getTypedRuleContext(Yield_exprContext, 0) as Yield_exprContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_yield_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterYield_stmt) {
	 		listener.enterYield_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitYield_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public RAISE(): TerminalNode {
		return this.getToken(Python3Parser.RAISE, 0);
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public FROM(): TerminalNode {
		return this.getToken(Python3Parser.FROM, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_raise_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterRaise_stmt) {
	 		listener.enterRaise_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitRaise_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public import_name(): Import_nameContext {
		return this.getTypedRuleContext(Import_nameContext, 0) as Import_nameContext;
	}
	public import_from(): Import_fromContext {
		return this.getTypedRuleContext(Import_fromContext, 0) as Import_fromContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_import_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterImport_stmt) {
	 		listener.enterImport_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitImport_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public IMPORT(): TerminalNode {
		return this.getToken(Python3Parser.IMPORT, 0);
	}
	public dotted_as_names(): Dotted_as_namesContext {
		return this.getTypedRuleContext(Dotted_as_namesContext, 0) as Dotted_as_namesContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_import_name;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterImport_name) {
	 		listener.enterImport_name(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitImport_name) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public FROM(): TerminalNode {
		return this.getToken(Python3Parser.FROM, 0);
	}
	public IMPORT(): TerminalNode {
		return this.getToken(Python3Parser.IMPORT, 0);
	}
	public dotted_name(): Dotted_nameContext {
		return this.getTypedRuleContext(Dotted_nameContext, 0) as Dotted_nameContext;
	}
	public STAR(): TerminalNode {
		return this.getToken(Python3Parser.STAR, 0);
	}
	public OPEN_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_PAREN, 0);
	}
	public import_as_names(): Import_as_namesContext {
		return this.getTypedRuleContext(Import_as_namesContext, 0) as Import_as_namesContext;
	}
	public CLOSE_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_PAREN, 0);
	}
	public DOT_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.DOT);
	}
	public DOT(i: number): TerminalNode {
		return this.getToken(Python3Parser.DOT, i);
	}
	public ELLIPSIS_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.ELLIPSIS);
	}
	public ELLIPSIS(i: number): TerminalNode {
		return this.getToken(Python3Parser.ELLIPSIS, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_import_from;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterImport_from) {
	 		listener.enterImport_from(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitImport_from) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NAME_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.NAME);
	}
	public NAME(i: number): TerminalNode {
		return this.getToken(Python3Parser.NAME, i);
	}
	public AS(): TerminalNode {
		return this.getToken(Python3Parser.AS, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_import_as_name;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterImport_as_name) {
	 		listener.enterImport_as_name(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitImport_as_name) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public dotted_name(): Dotted_nameContext {
		return this.getTypedRuleContext(Dotted_nameContext, 0) as Dotted_nameContext;
	}
	public AS(): TerminalNode {
		return this.getToken(Python3Parser.AS, 0);
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_dotted_as_name;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDotted_as_name) {
	 		listener.enterDotted_as_name(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDotted_as_name) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public import_as_name_list(): Import_as_nameContext[] {
		return this.getTypedRuleContexts(Import_as_nameContext) as Import_as_nameContext[];
	}
	public import_as_name(i: number): Import_as_nameContext {
		return this.getTypedRuleContext(Import_as_nameContext, i) as Import_as_nameContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_import_as_names;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterImport_as_names) {
	 		listener.enterImport_as_names(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitImport_as_names) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public dotted_as_name_list(): Dotted_as_nameContext[] {
		return this.getTypedRuleContexts(Dotted_as_nameContext) as Dotted_as_nameContext[];
	}
	public dotted_as_name(i: number): Dotted_as_nameContext {
		return this.getTypedRuleContext(Dotted_as_nameContext, i) as Dotted_as_nameContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_dotted_as_names;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDotted_as_names) {
	 		listener.enterDotted_as_names(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDotted_as_names) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NAME_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.NAME);
	}
	public NAME(i: number): TerminalNode {
		return this.getToken(Python3Parser.NAME, i);
	}
	public DOT_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.DOT);
	}
	public DOT(i: number): TerminalNode {
		return this.getToken(Python3Parser.DOT, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_dotted_name;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDotted_name) {
	 		listener.enterDotted_name(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDotted_name) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public GLOBAL(): TerminalNode {
		return this.getToken(Python3Parser.GLOBAL, 0);
	}
	public NAME_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.NAME);
	}
	public NAME(i: number): TerminalNode {
		return this.getToken(Python3Parser.NAME, i);
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_global_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterGlobal_stmt) {
	 		listener.enterGlobal_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitGlobal_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NONLOCAL(): TerminalNode {
		return this.getToken(Python3Parser.NONLOCAL, 0);
	}
	public NAME_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.NAME);
	}
	public NAME(i: number): TerminalNode {
		return this.getToken(Python3Parser.NAME, i);
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_nonlocal_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterNonlocal_stmt) {
	 		listener.enterNonlocal_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitNonlocal_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public ASSERT(): TerminalNode {
		return this.getToken(Python3Parser.ASSERT, 0);
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COMMA(): TerminalNode {
		return this.getToken(Python3Parser.COMMA, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_assert_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAssert_stmt) {
	 		listener.enterAssert_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAssert_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public if_stmt(): If_stmtContext {
		return this.getTypedRuleContext(If_stmtContext, 0) as If_stmtContext;
	}
	public while_stmt(): While_stmtContext {
		return this.getTypedRuleContext(While_stmtContext, 0) as While_stmtContext;
	}
	public for_stmt(): For_stmtContext {
		return this.getTypedRuleContext(For_stmtContext, 0) as For_stmtContext;
	}
	public try_stmt(): Try_stmtContext {
		return this.getTypedRuleContext(Try_stmtContext, 0) as Try_stmtContext;
	}
	public with_stmt(): With_stmtContext {
		return this.getTypedRuleContext(With_stmtContext, 0) as With_stmtContext;
	}
	public funcdef(): FuncdefContext {
		return this.getTypedRuleContext(FuncdefContext, 0) as FuncdefContext;
	}
	public classdef(): ClassdefContext {
		return this.getTypedRuleContext(ClassdefContext, 0) as ClassdefContext;
	}
	public decorated(): DecoratedContext {
		return this.getTypedRuleContext(DecoratedContext, 0) as DecoratedContext;
	}
	public async_stmt(): Async_stmtContext {
		return this.getTypedRuleContext(Async_stmtContext, 0) as Async_stmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_compound_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterCompound_stmt) {
	 		listener.enterCompound_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitCompound_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public ASYNC(): TerminalNode {
		return this.getToken(Python3Parser.ASYNC, 0);
	}
	public funcdef(): FuncdefContext {
		return this.getTypedRuleContext(FuncdefContext, 0) as FuncdefContext;
	}
	public with_stmt(): With_stmtContext {
		return this.getTypedRuleContext(With_stmtContext, 0) as With_stmtContext;
	}
	public for_stmt(): For_stmtContext {
		return this.getTypedRuleContext(For_stmtContext, 0) as For_stmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_async_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAsync_stmt) {
	 		listener.enterAsync_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAsync_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public IF(): TerminalNode {
		return this.getToken(Python3Parser.IF, 0);
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COLON_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COLON);
	}
	public COLON(i: number): TerminalNode {
		return this.getToken(Python3Parser.COLON, i);
	}
	public suite_list(): SuiteContext[] {
		return this.getTypedRuleContexts(SuiteContext) as SuiteContext[];
	}
	public suite(i: number): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, i) as SuiteContext;
	}
	public ELIF_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.ELIF);
	}
	public ELIF(i: number): TerminalNode {
		return this.getToken(Python3Parser.ELIF, i);
	}
	public ELSE(): TerminalNode {
		return this.getToken(Python3Parser.ELSE, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_if_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterIf_stmt) {
	 		listener.enterIf_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitIf_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public WHILE(): TerminalNode {
		return this.getToken(Python3Parser.WHILE, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public COLON_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COLON);
	}
	public COLON(i: number): TerminalNode {
		return this.getToken(Python3Parser.COLON, i);
	}
	public suite_list(): SuiteContext[] {
		return this.getTypedRuleContexts(SuiteContext) as SuiteContext[];
	}
	public suite(i: number): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, i) as SuiteContext;
	}
	public ELSE(): TerminalNode {
		return this.getToken(Python3Parser.ELSE, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_while_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterWhile_stmt) {
	 		listener.enterWhile_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitWhile_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public FOR(): TerminalNode {
		return this.getToken(Python3Parser.FOR, 0);
	}
	public exprlist(): ExprlistContext {
		return this.getTypedRuleContext(ExprlistContext, 0) as ExprlistContext;
	}
	public IN(): TerminalNode {
		return this.getToken(Python3Parser.IN, 0);
	}
	public testlist(): TestlistContext {
		return this.getTypedRuleContext(TestlistContext, 0) as TestlistContext;
	}
	public COLON_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COLON);
	}
	public COLON(i: number): TerminalNode {
		return this.getToken(Python3Parser.COLON, i);
	}
	public suite_list(): SuiteContext[] {
		return this.getTypedRuleContexts(SuiteContext) as SuiteContext[];
	}
	public suite(i: number): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, i) as SuiteContext;
	}
	public ELSE(): TerminalNode {
		return this.getToken(Python3Parser.ELSE, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_for_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterFor_stmt) {
	 		listener.enterFor_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitFor_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public TRY(): TerminalNode {
		return this.getToken(Python3Parser.TRY, 0);
	}
	public COLON_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COLON);
	}
	public COLON(i: number): TerminalNode {
		return this.getToken(Python3Parser.COLON, i);
	}
	public suite_list(): SuiteContext[] {
		return this.getTypedRuleContexts(SuiteContext) as SuiteContext[];
	}
	public suite(i: number): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, i) as SuiteContext;
	}
	public FINALLY(): TerminalNode {
		return this.getToken(Python3Parser.FINALLY, 0);
	}
	public except_clause_list(): Except_clauseContext[] {
		return this.getTypedRuleContexts(Except_clauseContext) as Except_clauseContext[];
	}
	public except_clause(i: number): Except_clauseContext {
		return this.getTypedRuleContext(Except_clauseContext, i) as Except_clauseContext;
	}
	public ELSE(): TerminalNode {
		return this.getToken(Python3Parser.ELSE, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_try_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTry_stmt) {
	 		listener.enterTry_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTry_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public WITH(): TerminalNode {
		return this.getToken(Python3Parser.WITH, 0);
	}
	public with_item_list(): With_itemContext[] {
		return this.getTypedRuleContexts(With_itemContext) as With_itemContext[];
	}
	public with_item(i: number): With_itemContext {
		return this.getTypedRuleContext(With_itemContext, i) as With_itemContext;
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public suite(): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, 0) as SuiteContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_with_stmt;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterWith_stmt) {
	 		listener.enterWith_stmt(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitWith_stmt) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public AS(): TerminalNode {
		return this.getToken(Python3Parser.AS, 0);
	}
	public expr(): ExprContext {
		return this.getTypedRuleContext(ExprContext, 0) as ExprContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_with_item;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterWith_item) {
	 		listener.enterWith_item(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitWith_item) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public EXCEPT(): TerminalNode {
		return this.getToken(Python3Parser.EXCEPT, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public AS(): TerminalNode {
		return this.getToken(Python3Parser.AS, 0);
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_except_clause;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterExcept_clause) {
	 		listener.enterExcept_clause(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitExcept_clause) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public simple_stmt(): Simple_stmtContext {
		return this.getTypedRuleContext(Simple_stmtContext, 0) as Simple_stmtContext;
	}
	public NEWLINE(): TerminalNode {
		return this.getToken(Python3Parser.NEWLINE, 0);
	}
	public INDENT(): TerminalNode {
		return this.getToken(Python3Parser.INDENT, 0);
	}
	public DEDENT(): TerminalNode {
		return this.getToken(Python3Parser.DEDENT, 0);
	}
	public stmt_list(): StmtContext[] {
		return this.getTypedRuleContexts(StmtContext) as StmtContext[];
	}
	public stmt(i: number): StmtContext {
		return this.getTypedRuleContext(StmtContext, i) as StmtContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_suite;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSuite) {
	 		listener.enterSuite(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSuite) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public or_test_list(): Or_testContext[] {
		return this.getTypedRuleContexts(Or_testContext) as Or_testContext[];
	}
	public or_test(i: number): Or_testContext {
		return this.getTypedRuleContext(Or_testContext, i) as Or_testContext;
	}
	public IF(): TerminalNode {
		return this.getToken(Python3Parser.IF, 0);
	}
	public ELSE(): TerminalNode {
		return this.getToken(Python3Parser.ELSE, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public lambdef(): LambdefContext {
		return this.getTypedRuleContext(LambdefContext, 0) as LambdefContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_test;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTest) {
	 		listener.enterTest(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTest) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public or_test(): Or_testContext {
		return this.getTypedRuleContext(Or_testContext, 0) as Or_testContext;
	}
	public lambdef_nocond(): Lambdef_nocondContext {
		return this.getTypedRuleContext(Lambdef_nocondContext, 0) as Lambdef_nocondContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_test_nocond;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTest_nocond) {
	 		listener.enterTest_nocond(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTest_nocond) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public LAMBDA(): TerminalNode {
		return this.getToken(Python3Parser.LAMBDA, 0);
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public varargslist(): VarargslistContext {
		return this.getTypedRuleContext(VarargslistContext, 0) as VarargslistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_lambdef;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterLambdef) {
	 		listener.enterLambdef(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitLambdef) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public LAMBDA(): TerminalNode {
		return this.getToken(Python3Parser.LAMBDA, 0);
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public test_nocond(): Test_nocondContext {
		return this.getTypedRuleContext(Test_nocondContext, 0) as Test_nocondContext;
	}
	public varargslist(): VarargslistContext {
		return this.getTypedRuleContext(VarargslistContext, 0) as VarargslistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_lambdef_nocond;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterLambdef_nocond) {
	 		listener.enterLambdef_nocond(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitLambdef_nocond) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public and_test_list(): And_testContext[] {
		return this.getTypedRuleContexts(And_testContext) as And_testContext[];
	}
	public and_test(i: number): And_testContext {
		return this.getTypedRuleContext(And_testContext, i) as And_testContext;
	}
	public OR_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.OR);
	}
	public OR(i: number): TerminalNode {
		return this.getToken(Python3Parser.OR, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_or_test;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterOr_test) {
	 		listener.enterOr_test(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitOr_test) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public not_test_list(): Not_testContext[] {
		return this.getTypedRuleContexts(Not_testContext) as Not_testContext[];
	}
	public not_test(i: number): Not_testContext {
		return this.getTypedRuleContext(Not_testContext, i) as Not_testContext;
	}
	public AND_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.AND);
	}
	public AND(i: number): TerminalNode {
		return this.getToken(Python3Parser.AND, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_and_test;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAnd_test) {
	 		listener.enterAnd_test(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAnd_test) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NOT(): TerminalNode {
		return this.getToken(Python3Parser.NOT, 0);
	}
	public not_test(): Not_testContext {
		return this.getTypedRuleContext(Not_testContext, 0) as Not_testContext;
	}
	public comparison(): ComparisonContext {
		return this.getTypedRuleContext(ComparisonContext, 0) as ComparisonContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_not_test;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterNot_test) {
	 		listener.enterNot_test(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitNot_test) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public expr_list(): ExprContext[] {
		return this.getTypedRuleContexts(ExprContext) as ExprContext[];
	}
	public expr(i: number): ExprContext {
		return this.getTypedRuleContext(ExprContext, i) as ExprContext;
	}
	public comp_op_list(): Comp_opContext[] {
		return this.getTypedRuleContexts(Comp_opContext) as Comp_opContext[];
	}
	public comp_op(i: number): Comp_opContext {
		return this.getTypedRuleContext(Comp_opContext, i) as Comp_opContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_comparison;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterComparison) {
	 		listener.enterComparison(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitComparison) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public LESS_THAN(): TerminalNode {
		return this.getToken(Python3Parser.LESS_THAN, 0);
	}
	public GREATER_THAN(): TerminalNode {
		return this.getToken(Python3Parser.GREATER_THAN, 0);
	}
	public EQUALS(): TerminalNode {
		return this.getToken(Python3Parser.EQUALS, 0);
	}
	public GT_EQ(): TerminalNode {
		return this.getToken(Python3Parser.GT_EQ, 0);
	}
	public LT_EQ(): TerminalNode {
		return this.getToken(Python3Parser.LT_EQ, 0);
	}
	public NOT_EQ_1(): TerminalNode {
		return this.getToken(Python3Parser.NOT_EQ_1, 0);
	}
	public NOT_EQ_2(): TerminalNode {
		return this.getToken(Python3Parser.NOT_EQ_2, 0);
	}
	public IN(): TerminalNode {
		return this.getToken(Python3Parser.IN, 0);
	}
	public NOT(): TerminalNode {
		return this.getToken(Python3Parser.NOT, 0);
	}
	public IS(): TerminalNode {
		return this.getToken(Python3Parser.IS, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_comp_op;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterComp_op) {
	 		listener.enterComp_op(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitComp_op) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public STAR(): TerminalNode {
		return this.getToken(Python3Parser.STAR, 0);
	}
	public expr(): ExprContext {
		return this.getTypedRuleContext(ExprContext, 0) as ExprContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_star_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterStar_expr) {
	 		listener.enterStar_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitStar_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public xor_expr_list(): Xor_exprContext[] {
		return this.getTypedRuleContexts(Xor_exprContext) as Xor_exprContext[];
	}
	public xor_expr(i: number): Xor_exprContext {
		return this.getTypedRuleContext(Xor_exprContext, i) as Xor_exprContext;
	}
	public OR_OP_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.OR_OP);
	}
	public OR_OP(i: number): TerminalNode {
		return this.getToken(Python3Parser.OR_OP, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterExpr) {
	 		listener.enterExpr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitExpr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public and_expr_list(): And_exprContext[] {
		return this.getTypedRuleContexts(And_exprContext) as And_exprContext[];
	}
	public and_expr(i: number): And_exprContext {
		return this.getTypedRuleContext(And_exprContext, i) as And_exprContext;
	}
	public XOR_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.XOR);
	}
	public XOR(i: number): TerminalNode {
		return this.getToken(Python3Parser.XOR, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_xor_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterXor_expr) {
	 		listener.enterXor_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitXor_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public shift_expr_list(): Shift_exprContext[] {
		return this.getTypedRuleContexts(Shift_exprContext) as Shift_exprContext[];
	}
	public shift_expr(i: number): Shift_exprContext {
		return this.getTypedRuleContext(Shift_exprContext, i) as Shift_exprContext;
	}
	public AND_OP_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.AND_OP);
	}
	public AND_OP(i: number): TerminalNode {
		return this.getToken(Python3Parser.AND_OP, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_and_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAnd_expr) {
	 		listener.enterAnd_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAnd_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public arith_expr_list(): Arith_exprContext[] {
		return this.getTypedRuleContexts(Arith_exprContext) as Arith_exprContext[];
	}
	public arith_expr(i: number): Arith_exprContext {
		return this.getTypedRuleContext(Arith_exprContext, i) as Arith_exprContext;
	}
	public LEFT_SHIFT_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.LEFT_SHIFT);
	}
	public LEFT_SHIFT(i: number): TerminalNode {
		return this.getToken(Python3Parser.LEFT_SHIFT, i);
	}
	public RIGHT_SHIFT_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.RIGHT_SHIFT);
	}
	public RIGHT_SHIFT(i: number): TerminalNode {
		return this.getToken(Python3Parser.RIGHT_SHIFT, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_shift_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterShift_expr) {
	 		listener.enterShift_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitShift_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public term_list(): TermContext[] {
		return this.getTypedRuleContexts(TermContext) as TermContext[];
	}
	public term(i: number): TermContext {
		return this.getTypedRuleContext(TermContext, i) as TermContext;
	}
	public ADD_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.ADD);
	}
	public ADD(i: number): TerminalNode {
		return this.getToken(Python3Parser.ADD, i);
	}
	public MINUS_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.MINUS);
	}
	public MINUS(i: number): TerminalNode {
		return this.getToken(Python3Parser.MINUS, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_arith_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterArith_expr) {
	 		listener.enterArith_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitArith_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public factor_list(): FactorContext[] {
		return this.getTypedRuleContexts(FactorContext) as FactorContext[];
	}
	public factor(i: number): FactorContext {
		return this.getTypedRuleContext(FactorContext, i) as FactorContext;
	}
	public STAR_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.STAR);
	}
	public STAR(i: number): TerminalNode {
		return this.getToken(Python3Parser.STAR, i);
	}
	public AT_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.AT);
	}
	public AT(i: number): TerminalNode {
		return this.getToken(Python3Parser.AT, i);
	}
	public DIV_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.DIV);
	}
	public DIV(i: number): TerminalNode {
		return this.getToken(Python3Parser.DIV, i);
	}
	public MOD_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.MOD);
	}
	public MOD(i: number): TerminalNode {
		return this.getToken(Python3Parser.MOD, i);
	}
	public IDIV_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.IDIV);
	}
	public IDIV(i: number): TerminalNode {
		return this.getToken(Python3Parser.IDIV, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_term;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTerm) {
	 		listener.enterTerm(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTerm) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public factor(): FactorContext {
		return this.getTypedRuleContext(FactorContext, 0) as FactorContext;
	}
	public ADD(): TerminalNode {
		return this.getToken(Python3Parser.ADD, 0);
	}
	public MINUS(): TerminalNode {
		return this.getToken(Python3Parser.MINUS, 0);
	}
	public NOT_OP(): TerminalNode {
		return this.getToken(Python3Parser.NOT_OP, 0);
	}
	public power(): PowerContext {
		return this.getTypedRuleContext(PowerContext, 0) as PowerContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_factor;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterFactor) {
	 		listener.enterFactor(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitFactor) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public atom_expr(): Atom_exprContext {
		return this.getTypedRuleContext(Atom_exprContext, 0) as Atom_exprContext;
	}
	public POWER(): TerminalNode {
		return this.getToken(Python3Parser.POWER, 0);
	}
	public factor(): FactorContext {
		return this.getTypedRuleContext(FactorContext, 0) as FactorContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_power;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterPower) {
	 		listener.enterPower(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitPower) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public atom(): AtomContext {
		return this.getTypedRuleContext(AtomContext, 0) as AtomContext;
	}
	public AWAIT(): TerminalNode {
		return this.getToken(Python3Parser.AWAIT, 0);
	}
	public trailer_list(): TrailerContext[] {
		return this.getTypedRuleContexts(TrailerContext) as TrailerContext[];
	}
	public trailer(i: number): TrailerContext {
		return this.getTypedRuleContext(TrailerContext, i) as TrailerContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_atom_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAtom_expr) {
	 		listener.enterAtom_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAtom_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public OPEN_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_PAREN, 0);
	}
	public CLOSE_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_PAREN, 0);
	}
	public OPEN_BRACK(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_BRACK, 0);
	}
	public CLOSE_BRACK(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_BRACK, 0);
	}
	public OPEN_BRACE(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_BRACE, 0);
	}
	public CLOSE_BRACE(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_BRACE, 0);
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
	public NUMBER(): TerminalNode {
		return this.getToken(Python3Parser.NUMBER, 0);
	}
	public ELLIPSIS(): TerminalNode {
		return this.getToken(Python3Parser.ELLIPSIS, 0);
	}
	public NONE(): TerminalNode {
		return this.getToken(Python3Parser.NONE, 0);
	}
	public TRUE(): TerminalNode {
		return this.getToken(Python3Parser.TRUE, 0);
	}
	public FALSE(): TerminalNode {
		return this.getToken(Python3Parser.FALSE, 0);
	}
	public yield_expr(): Yield_exprContext {
		return this.getTypedRuleContext(Yield_exprContext, 0) as Yield_exprContext;
	}
	public testlist_comp(): Testlist_compContext {
		return this.getTypedRuleContext(Testlist_compContext, 0) as Testlist_compContext;
	}
	public dictorsetmaker(): DictorsetmakerContext {
		return this.getTypedRuleContext(DictorsetmakerContext, 0) as DictorsetmakerContext;
	}
	public string_template_list(): String_templateContext[] {
		return this.getTypedRuleContexts(String_templateContext) as String_templateContext[];
	}
	public string_template(i: number): String_templateContext {
		return this.getTypedRuleContext(String_templateContext, i) as String_templateContext;
	}
	public STRING_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.STRING);
	}
	public STRING(i: number): TerminalNode {
		return this.getToken(Python3Parser.STRING, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_atom;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterAtom) {
	 		listener.enterAtom(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitAtom) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public star_expr_list(): Star_exprContext[] {
		return this.getTypedRuleContexts(Star_exprContext) as Star_exprContext[];
	}
	public star_expr(i: number): Star_exprContext {
		return this.getTypedRuleContext(Star_exprContext, i) as Star_exprContext;
	}
	public comp_for(): Comp_forContext {
		return this.getTypedRuleContext(Comp_forContext, 0) as Comp_forContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_testlist_comp;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTestlist_comp) {
	 		listener.enterTestlist_comp(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTestlist_comp) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public callArguments(): CallArgumentsContext {
		return this.getTypedRuleContext(CallArgumentsContext, 0) as CallArgumentsContext;
	}
	public OPEN_BRACK(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_BRACK, 0);
	}
	public subscriptlist(): SubscriptlistContext {
		return this.getTypedRuleContext(SubscriptlistContext, 0) as SubscriptlistContext;
	}
	public CLOSE_BRACK(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_BRACK, 0);
	}
	public DOT(): TerminalNode {
		return this.getToken(Python3Parser.DOT, 0);
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_trailer;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTrailer) {
	 		listener.enterTrailer(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTrailer) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public subscript_list(): SubscriptContext[] {
		return this.getTypedRuleContexts(SubscriptContext) as SubscriptContext[];
	}
	public subscript(i: number): SubscriptContext {
		return this.getTypedRuleContext(SubscriptContext, i) as SubscriptContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_subscriptlist;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSubscriptlist) {
	 		listener.enterSubscriptlist(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSubscriptlist) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public sliceop(): SliceopContext {
		return this.getTypedRuleContext(SliceopContext, 0) as SliceopContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_subscript;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSubscript) {
	 		listener.enterSubscript(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSubscript) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_sliceop;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSliceop) {
	 		listener.enterSliceop(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSliceop) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public expr_list(): ExprContext[] {
		return this.getTypedRuleContexts(ExprContext) as ExprContext[];
	}
	public expr(i: number): ExprContext {
		return this.getTypedRuleContext(ExprContext, i) as ExprContext;
	}
	public star_expr_list(): Star_exprContext[] {
		return this.getTypedRuleContexts(Star_exprContext) as Star_exprContext[];
	}
	public star_expr(i: number): Star_exprContext {
		return this.getTypedRuleContext(Star_exprContext, i) as Star_exprContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_exprlist;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterExprlist) {
	 		listener.enterExprlist(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitExprlist) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_testlist;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterTestlist) {
	 		listener.enterTestlist(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitTestlist) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public COLON_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COLON);
	}
	public COLON(i: number): TerminalNode {
		return this.getToken(Python3Parser.COLON, i);
	}
	public POWER_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.POWER);
	}
	public POWER(i: number): TerminalNode {
		return this.getToken(Python3Parser.POWER, i);
	}
	public expr_list(): ExprContext[] {
		return this.getTypedRuleContexts(ExprContext) as ExprContext[];
	}
	public expr(i: number): ExprContext {
		return this.getTypedRuleContext(ExprContext, i) as ExprContext;
	}
	public comp_for(): Comp_forContext {
		return this.getTypedRuleContext(Comp_forContext, 0) as Comp_forContext;
	}
	public star_expr_list(): Star_exprContext[] {
		return this.getTypedRuleContexts(Star_exprContext) as Star_exprContext[];
	}
	public star_expr(i: number): Star_exprContext {
		return this.getTypedRuleContext(Star_exprContext, i) as Star_exprContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_dictorsetmaker;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDictorsetmaker) {
	 		listener.enterDictorsetmaker(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDictorsetmaker) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public CLASS(): TerminalNode {
		return this.getToken(Python3Parser.CLASS, 0);
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
	public COLON(): TerminalNode {
		return this.getToken(Python3Parser.COLON, 0);
	}
	public suite(): SuiteContext {
		return this.getTypedRuleContext(SuiteContext, 0) as SuiteContext;
	}
	public OPEN_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_PAREN, 0);
	}
	public CLOSE_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_PAREN, 0);
	}
	public arglist(): ArglistContext {
		return this.getTypedRuleContext(ArglistContext, 0) as ArglistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_classdef;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterClassdef) {
	 		listener.enterClassdef(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitClassdef) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public OPEN_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_PAREN, 0);
	}
	public CLOSE_PAREN(): TerminalNode {
		return this.getToken(Python3Parser.CLOSE_PAREN, 0);
	}
	public arglist(): ArglistContext {
		return this.getTypedRuleContext(ArglistContext, 0) as ArglistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_callArguments;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterCallArguments) {
	 		listener.enterCallArguments(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitCallArguments) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public argument_list(): ArgumentContext[] {
		return this.getTypedRuleContexts(ArgumentContext) as ArgumentContext[];
	}
	public argument(i: number): ArgumentContext {
		return this.getTypedRuleContext(ArgumentContext, i) as ArgumentContext;
	}
	public COMMA_list(): TerminalNode[] {
	    	return this.getTokens(Python3Parser.COMMA);
	}
	public COMMA(i: number): TerminalNode {
		return this.getToken(Python3Parser.COMMA, i);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_arglist;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterArglist) {
	 		listener.enterArglist(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitArglist) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public test_list(): TestContext[] {
		return this.getTypedRuleContexts(TestContext) as TestContext[];
	}
	public test(i: number): TestContext {
		return this.getTypedRuleContext(TestContext, i) as TestContext;
	}
	public ASSIGN(): TerminalNode {
		return this.getToken(Python3Parser.ASSIGN, 0);
	}
	public POWER(): TerminalNode {
		return this.getToken(Python3Parser.POWER, 0);
	}
	public STAR(): TerminalNode {
		return this.getToken(Python3Parser.STAR, 0);
	}
	public comp_for(): Comp_forContext {
		return this.getTypedRuleContext(Comp_forContext, 0) as Comp_forContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_argument;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterArgument) {
	 		listener.enterArgument(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitArgument) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public comp_for(): Comp_forContext {
		return this.getTypedRuleContext(Comp_forContext, 0) as Comp_forContext;
	}
	public comp_if(): Comp_ifContext {
		return this.getTypedRuleContext(Comp_ifContext, 0) as Comp_ifContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_comp_iter;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterComp_iter) {
	 		listener.enterComp_iter(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitComp_iter) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public FOR(): TerminalNode {
		return this.getToken(Python3Parser.FOR, 0);
	}
	public exprlist(): ExprlistContext {
		return this.getTypedRuleContext(ExprlistContext, 0) as ExprlistContext;
	}
	public IN(): TerminalNode {
		return this.getToken(Python3Parser.IN, 0);
	}
	public or_test(): Or_testContext {
		return this.getTypedRuleContext(Or_testContext, 0) as Or_testContext;
	}
	public ASYNC(): TerminalNode {
		return this.getToken(Python3Parser.ASYNC, 0);
	}
	public comp_iter(): Comp_iterContext {
		return this.getTypedRuleContext(Comp_iterContext, 0) as Comp_iterContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_comp_for;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterComp_for) {
	 		listener.enterComp_for(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitComp_for) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public IF(): TerminalNode {
		return this.getToken(Python3Parser.IF, 0);
	}
	public test_nocond(): Test_nocondContext {
		return this.getTypedRuleContext(Test_nocondContext, 0) as Test_nocondContext;
	}
	public comp_iter(): Comp_iterContext {
		return this.getTypedRuleContext(Comp_iterContext, 0) as Comp_iterContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_comp_if;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterComp_if) {
	 		listener.enterComp_if(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitComp_if) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public NAME(): TerminalNode {
		return this.getToken(Python3Parser.NAME, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_encoding_decl;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterEncoding_decl) {
	 		listener.enterEncoding_decl(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitEncoding_decl) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public YIELD(): TerminalNode {
		return this.getToken(Python3Parser.YIELD, 0);
	}
	public yield_arg(): Yield_argContext {
		return this.getTypedRuleContext(Yield_argContext, 0) as Yield_argContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_yield_expr;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterYield_expr) {
	 		listener.enterYield_expr(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitYield_expr) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public FROM(): TerminalNode {
		return this.getToken(Python3Parser.FROM, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public testlist(): TestlistContext {
		return this.getTypedRuleContext(TestlistContext, 0) as TestlistContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_yield_arg;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterYield_arg) {
	 		listener.enterYield_arg(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitYield_arg) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START(): TerminalNode {
		return this.getToken(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START, 0);
	}
	public SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END(): TerminalNode {
		return this.getToken(Python3Parser.SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END, 0);
	}
	public single_string_template_atom_list(): Single_string_template_atomContext[] {
		return this.getTypedRuleContexts(Single_string_template_atomContext) as Single_string_template_atomContext[];
	}
	public single_string_template_atom(i: number): Single_string_template_atomContext {
		return this.getTypedRuleContext(Single_string_template_atomContext, i) as Single_string_template_atomContext;
	}
	public SINGLE_QUOTE_LONG_TEMPLATE_STRING_START(): TerminalNode {
		return this.getToken(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_START, 0);
	}
	public SINGLE_QUOTE_LONG_TEMPLATE_STRING_END(): TerminalNode {
		return this.getToken(Python3Parser.SINGLE_QUOTE_LONG_TEMPLATE_STRING_END, 0);
	}
	public DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START(): TerminalNode {
		return this.getToken(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START, 0);
	}
	public DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END(): TerminalNode {
		return this.getToken(Python3Parser.DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END, 0);
	}
	public double_string_template_atom_list(): Double_string_template_atomContext[] {
		return this.getTypedRuleContexts(Double_string_template_atomContext) as Double_string_template_atomContext[];
	}
	public double_string_template_atom(i: number): Double_string_template_atomContext {
		return this.getTypedRuleContext(Double_string_template_atomContext, i) as Double_string_template_atomContext;
	}
	public DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START(): TerminalNode {
		return this.getToken(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START, 0);
	}
	public DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END(): TerminalNode {
		return this.getToken(Python3Parser.DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END, 0);
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_string_template;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterString_template) {
	 		listener.enterString_template(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitString_template) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public SINGLE_QUOTE_STRING_ATOM(): TerminalNode {
		return this.getToken(Python3Parser.SINGLE_QUOTE_STRING_ATOM, 0);
	}
	public OPEN_BRACE(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_BRACE, 0);
	}
	public TEMPLATE_CLOSE_BRACE(): TerminalNode {
		return this.getToken(Python3Parser.TEMPLATE_CLOSE_BRACE, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public star_expr(): Star_exprContext {
		return this.getTypedRuleContext(Star_exprContext, 0) as Star_exprContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_single_string_template_atom;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterSingle_string_template_atom) {
	 		listener.enterSingle_string_template_atom(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitSingle_string_template_atom) {
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
	constructor(parser?: Python3Parser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public DOUBLE_QUOTE_STRING_ATOM(): TerminalNode {
		return this.getToken(Python3Parser.DOUBLE_QUOTE_STRING_ATOM, 0);
	}
	public OPEN_BRACE(): TerminalNode {
		return this.getToken(Python3Parser.OPEN_BRACE, 0);
	}
	public TEMPLATE_CLOSE_BRACE(): TerminalNode {
		return this.getToken(Python3Parser.TEMPLATE_CLOSE_BRACE, 0);
	}
	public test(): TestContext {
		return this.getTypedRuleContext(TestContext, 0) as TestContext;
	}
	public star_expr(): Star_exprContext {
		return this.getTypedRuleContext(Star_exprContext, 0) as Star_exprContext;
	}
    public get ruleIndex(): number {
    	return Python3Parser.RULE_double_string_template_atom;
	}
	public enterRule(listener: Python3ParserListener): void {
	    if(listener.enterDouble_string_template_atom) {
	 		listener.enterDouble_string_template_atom(this);
		}
	}
	public exitRule(listener: Python3ParserListener): void {
	    if(listener.exitDouble_string_template_atom) {
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
