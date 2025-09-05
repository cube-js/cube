// Generated from src/parser/GenericSql.g4 by ANTLR 4.13.2
// noinspection ES6UnusedImports,JSUnusedGlobalSymbols,JSUnusedLocalSymbols

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
import GenericSqlListener from "./GenericSqlListener.js";
import GenericSqlVisitor from "./GenericSqlVisitor.js";

// for running tests with parameters, TODO: discuss strategy for typed parameters in CI
// eslint-disable-next-line no-unused-vars
type int = number;

export default class GenericSqlParser extends Parser {
	public static readonly T__0 = 1;
	public static readonly T__1 = 2;
	public static readonly T__2 = 3;
	public static readonly T__3 = 4;
	public static readonly SELECT = 5;
	public static readonly ASTERISK = 6;
	public static readonly FROM = 7;
	public static readonly WHERE = 8;
	public static readonly AND = 9;
	public static readonly OR = 10;
	public static readonly NOT = 11;
	public static readonly AS = 12;
	public static readonly LT = 13;
	public static readonly LTE = 14;
	public static readonly GT = 15;
	public static readonly GTE = 16;
	public static readonly EQUALS = 17;
	public static readonly NOT_EQUALS = 18;
	public static readonly IS = 19;
	public static readonly NULL = 20;
	public static readonly CAST = 21;
	public static readonly REGEXP = 22;
	public static readonly INDEXED_PARAM = 23;
	public static readonly PARAM_PLACEHOLDER = 24;
	public static readonly ID = 25;
	public static readonly DIGIT = 26;
	public static readonly QUOTED_ID = 27;
	public static readonly STRING = 28;
	public static readonly WHITESPACE = 29;
	public static readonly COMMENT = 30;
	public static readonly MULTILINE_COMMENT = 31;
	public static override readonly EOF = Token.EOF;
	public static readonly RULE_statement = 0;
	public static readonly RULE_query = 1;
	public static readonly RULE_fromTables = 2;
	public static readonly RULE_selectFields = 3;
	public static readonly RULE_field = 4;
	public static readonly RULE_selectField = 5;
	public static readonly RULE_aliasField = 6;
	public static readonly RULE_boolExp = 7;
	public static readonly RULE_exp = 8;
	public static readonly RULE_numeric = 9;
	public static readonly RULE_binaryOperator = 10;
	public static readonly RULE_unaryOperator = 11;
	public static readonly RULE_idPath = 12;
	public static readonly RULE_identifier = 13;
	public static readonly literalNames: (string | null)[] = [ null, "'('", 
                                                            "')'", "','", 
                                                            "'.'", "'SELECT'", 
                                                            "'*'", "'FROM'", 
                                                            "'WHERE'", "'AND'", 
                                                            "'OR'", "'NOT'", 
                                                            "'AS'", "'<'", 
                                                            "'<='", "'>'", 
                                                            "'>='", "'='", 
                                                            null, "'IS'", 
                                                            "'NULL'", "'CAST'", 
                                                            "'REGEXP'", 
                                                            null, "'?'" ];
	public static readonly symbolicNames: (string | null)[] = [ null, null, 
                                                             null, null, 
                                                             null, "SELECT", 
                                                             "ASTERISK", 
                                                             "FROM", "WHERE", 
                                                             "AND", "OR", 
                                                             "NOT", "AS", 
                                                             "LT", "LTE", 
                                                             "GT", "GTE", 
                                                             "EQUALS", "NOT_EQUALS", 
                                                             "IS", "NULL", 
                                                             "CAST", "REGEXP", 
                                                             "INDEXED_PARAM", 
                                                             "PARAM_PLACEHOLDER", 
                                                             "ID", "DIGIT", 
                                                             "QUOTED_ID", 
                                                             "STRING", "WHITESPACE", 
                                                             "COMMENT", 
                                                             "MULTILINE_COMMENT" ];
	// tslint:disable:no-trailing-whitespace
	public static readonly ruleNames: string[] = [
		"statement", "query", "fromTables", "selectFields", "field", "selectField", 
		"aliasField", "boolExp", "exp", "numeric", "binaryOperator", "unaryOperator", 
		"idPath", "identifier",
	];
	public get grammarFileName(): string { return "GenericSql.g4"; }
	public get literalNames(): (string | null)[] { return GenericSqlParser.literalNames; }
	public get symbolicNames(): (string | null)[] { return GenericSqlParser.symbolicNames; }
	public get ruleNames(): string[] { return GenericSqlParser.ruleNames; }
	public get serializedATN(): number[] { return GenericSqlParser._serializedATN; }

	protected createFailedPredicateException(predicate?: string, message?: string): FailedPredicateException {
		return new FailedPredicateException(this, predicate, message);
	}

	constructor(input: TokenStream) {
		super(input);
		this._interp = new ParserATNSimulator(this, GenericSqlParser._ATN, GenericSqlParser.DecisionsToDFA, new PredictionContextCache());
	}
	// @RuleVersion(0)
	public statement(): StatementContext {
		let localctx: StatementContext = new StatementContext(this, this._ctx, this.state);
		this.enterRule(localctx, 0, GenericSqlParser.RULE_statement);
		try {
			this.state = 36;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 5:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 28;
				this.query();
				this.state = 29;
				this.match(GenericSqlParser.EOF);
				}
				break;
			case 1:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 31;
				this.match(GenericSqlParser.T__0);
				this.state = 32;
				this.query();
				this.state = 33;
				this.match(GenericSqlParser.T__1);
				this.state = 34;
				this.match(GenericSqlParser.EOF);
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
	public query(): QueryContext {
		let localctx: QueryContext = new QueryContext(this, this._ctx, this.state);
		this.enterRule(localctx, 2, GenericSqlParser.RULE_query);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 38;
			this.match(GenericSqlParser.SELECT);
			this.state = 39;
			this.selectFields();
			this.state = 40;
			this.match(GenericSqlParser.FROM);
			this.state = 41;
			localctx._from_ = this.fromTables();
			this.state = 44;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===8) {
				{
				this.state = 42;
				this.match(GenericSqlParser.WHERE);
				this.state = 43;
				localctx._where = this.boolExp(0);
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
	public fromTables(): FromTablesContext {
		let localctx: FromTablesContext = new FromTablesContext(this, this._ctx, this.state);
		this.enterRule(localctx, 4, GenericSqlParser.RULE_fromTables);
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 46;
			this.aliasField();
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
	public selectFields(): SelectFieldsContext {
		let localctx: SelectFieldsContext = new SelectFieldsContext(this, this._ctx, this.state);
		this.enterRule(localctx, 6, GenericSqlParser.RULE_selectFields);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			{
			this.state = 48;
			this.field();
			this.state = 53;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===3) {
				{
				{
				this.state = 49;
				this.match(GenericSqlParser.T__2);
				this.state = 50;
				this.field();
				}
				}
				this.state = 55;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
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
	public field(): FieldContext {
		let localctx: FieldContext = new FieldContext(this, this._ctx, this.state);
		this.enterRule(localctx, 8, GenericSqlParser.RULE_field);
		try {
			this.state = 58;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 1:
			case 4:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 56;
				this.selectField();
				}
				break;
			case 6:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 57;
				this.match(GenericSqlParser.ASTERISK);
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
	public selectField(): SelectFieldContext {
		let localctx: SelectFieldContext = new SelectFieldContext(this, this._ctx, this.state);
		this.enterRule(localctx, 10, GenericSqlParser.RULE_selectField);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 60;
			this.exp(0);
			this.state = 65;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 167776256) !== 0)) {
				{
				this.state = 62;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===12) {
					{
					this.state = 61;
					this.match(GenericSqlParser.AS);
					}
				}

				this.state = 64;
				this.identifier();
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
	public aliasField(): AliasFieldContext {
		let localctx: AliasFieldContext = new AliasFieldContext(this, this._ctx, this.state);
		this.enterRule(localctx, 12, GenericSqlParser.RULE_aliasField);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 67;
			this.idPath();
			this.state = 72;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 167776256) !== 0)) {
				{
				this.state = 69;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===12) {
					{
					this.state = 68;
					this.match(GenericSqlParser.AS);
					}
				}

				this.state = 71;
				this.identifier();
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

	public boolExp(): BoolExpContext;
	public boolExp(_p: number): BoolExpContext;
	// @RuleVersion(0)
	public boolExp(_p?: number): BoolExpContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let localctx: BoolExpContext = new BoolExpContext(this, this._ctx, _parentState);
		let _prevctx: BoolExpContext = localctx;
		let _startState: number = 14;
		this.enterRecursionRule(localctx, 14, GenericSqlParser.RULE_boolExp, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 78;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 1:
			case 4:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
				{
				this.state = 75;
				this.exp(0);
				}
				break;
			case 11:
				{
				this.state = 76;
				this.match(GenericSqlParser.NOT);
				this.state = 77;
				this.boolExp(1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this._ctx.stop = this._input.LT(-1);
			this.state = 88;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 10, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = localctx;
					{
					this.state = 86;
					this._errHandler.sync(this);
					switch ( this._interp.adaptivePredict(this._input, 9, this._ctx) ) {
					case 1:
						{
						localctx = new BoolExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_boolExp);
						this.state = 80;
						if (!(this.precpred(this._ctx, 3))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 3)");
						}
						this.state = 81;
						this.match(GenericSqlParser.AND);
						this.state = 82;
						this.boolExp(4);
						}
						break;
					case 2:
						{
						localctx = new BoolExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_boolExp);
						this.state = 83;
						if (!(this.precpred(this._ctx, 2))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 2)");
						}
						this.state = 84;
						this.match(GenericSqlParser.OR);
						this.state = 85;
						this.boolExp(3);
						}
						break;
					}
					}
				}
				this.state = 90;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 10, this._ctx);
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
			this.unrollRecursionContexts(_parentctx);
		}
		return localctx;
	}

	public exp(): ExpContext;
	public exp(_p: number): ExpContext;
	// @RuleVersion(0)
	public exp(_p?: number): ExpContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let localctx: ExpContext = new ExpContext(this, this._ctx, _parentState);
		let _prevctx: ExpContext = localctx;
		let _startState: number = 16;
		this.enterRecursionRule(localctx, 16, GenericSqlParser.RULE_exp, _p);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 123;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 12, this._ctx) ) {
			case 1:
				{
				this.state = 92;
				this.idPath();
				}
				break;
			case 2:
				{
				this.state = 93;
				this.identifier();
				this.state = 94;
				this.match(GenericSqlParser.T__0);
				{
				this.state = 95;
				this.exp(0);
				this.state = 100;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===3) {
					{
					{
					this.state = 96;
					this.match(GenericSqlParser.T__2);
					this.state = 97;
					this.exp(0);
					}
					}
					this.state = 102;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				}
				this.state = 103;
				this.match(GenericSqlParser.T__1);
				}
				break;
			case 3:
				{
				this.state = 105;
				this.match(GenericSqlParser.CAST);
				this.state = 106;
				this.match(GenericSqlParser.T__0);
				this.state = 107;
				this.exp(0);
				this.state = 108;
				this.match(GenericSqlParser.AS);
				this.state = 109;
				this.identifier();
				this.state = 110;
				this.match(GenericSqlParser.T__1);
				}
				break;
			case 4:
				{
				this.state = 112;
				this.match(GenericSqlParser.REGEXP);
				this.state = 113;
				this.match(GenericSqlParser.STRING);
				}
				break;
			case 5:
				{
				this.state = 114;
				this.match(GenericSqlParser.STRING);
				}
				break;
			case 6:
				{
				this.state = 115;
				this.numeric();
				}
				break;
			case 7:
				{
				this.state = 116;
				this.identifier();
				}
				break;
			case 8:
				{
				this.state = 117;
				this.match(GenericSqlParser.INDEXED_PARAM);
				}
				break;
			case 9:
				{
				this.state = 118;
				this.match(GenericSqlParser.PARAM_PLACEHOLDER);
				}
				break;
			case 10:
				{
				this.state = 119;
				this.match(GenericSqlParser.T__0);
				this.state = 120;
				this.exp(0);
				this.state = 121;
				this.match(GenericSqlParser.T__1);
				}
				break;
			}
			this._ctx.stop = this._input.LT(-1);
			this.state = 133;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 14, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = localctx;
					{
					this.state = 131;
					this._errHandler.sync(this);
					switch ( this._interp.adaptivePredict(this._input, 13, this._ctx) ) {
					case 1:
						{
						localctx = new ExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_exp);
						this.state = 125;
						if (!(this.precpred(this._ctx, 12))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 12)");
						}
						this.state = 126;
						this.binaryOperator();
						this.state = 127;
						this.exp(13);
						}
						break;
					case 2:
						{
						localctx = new ExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_exp);
						this.state = 129;
						if (!(this.precpred(this._ctx, 11))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 11)");
						}
						this.state = 130;
						this.unaryOperator();
						}
						break;
					}
					}
				}
				this.state = 135;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 14, this._ctx);
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
			this.unrollRecursionContexts(_parentctx);
		}
		return localctx;
	}
	// @RuleVersion(0)
	public numeric(): NumericContext {
		let localctx: NumericContext = new NumericContext(this, this._ctx, this.state);
		this.enterRule(localctx, 18, GenericSqlParser.RULE_numeric);
		try {
			let _alt: number;
			this.state = 155;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 26:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 137;
				this._errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						this.state = 136;
						this.match(GenericSqlParser.DIGIT);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					this.state = 139;
					this._errHandler.sync(this);
					_alt = this._interp.adaptivePredict(this._input, 15, this._ctx);
				} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
				this.state = 147;
				this._errHandler.sync(this);
				switch ( this._interp.adaptivePredict(this._input, 17, this._ctx) ) {
				case 1:
					{
					this.state = 141;
					this.match(GenericSqlParser.T__3);
					this.state = 143;
					this._errHandler.sync(this);
					_alt = 1;
					do {
						switch (_alt) {
						case 1:
							{
							{
							this.state = 142;
							this.match(GenericSqlParser.DIGIT);
							}
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						this.state = 145;
						this._errHandler.sync(this);
						_alt = this._interp.adaptivePredict(this._input, 16, this._ctx);
					} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
					}
					break;
				}
				}
				break;
			case 4:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 149;
				this.match(GenericSqlParser.T__3);
				this.state = 151;
				this._errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						this.state = 150;
						this.match(GenericSqlParser.DIGIT);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					this.state = 153;
					this._errHandler.sync(this);
					_alt = this._interp.adaptivePredict(this._input, 18, this._ctx);
				} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
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
	public binaryOperator(): BinaryOperatorContext {
		let localctx: BinaryOperatorContext = new BinaryOperatorContext(this, this._ctx, this.state);
		this.enterRule(localctx, 20, GenericSqlParser.RULE_binaryOperator);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 157;
			_la = this._input.LA(1);
			if(!((((_la) & ~0x1F) === 0 && ((1 << _la) & 516096) !== 0))) {
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
	public unaryOperator(): UnaryOperatorContext {
		let localctx: UnaryOperatorContext = new UnaryOperatorContext(this, this._ctx, this.state);
		this.enterRule(localctx, 22, GenericSqlParser.RULE_unaryOperator);
		try {
			this.state = 164;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 20, this._ctx) ) {
			case 1:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 159;
				this.match(GenericSqlParser.IS);
				this.state = 160;
				this.match(GenericSqlParser.NULL);
				}
				break;
			case 2:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 161;
				this.match(GenericSqlParser.IS);
				this.state = 162;
				this.match(GenericSqlParser.NOT);
				this.state = 163;
				this.match(GenericSqlParser.NULL);
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
	public idPath(): IdPathContext {
		let localctx: IdPathContext = new IdPathContext(this, this._ctx, this.state);
		this.enterRule(localctx, 24, GenericSqlParser.RULE_idPath);
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 166;
			this.identifier();
			this.state = 171;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 21, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 167;
					this.match(GenericSqlParser.T__3);
					this.state = 168;
					this.identifier();
					}
					}
				}
				this.state = 173;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 21, this._ctx);
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
	public identifier(): IdentifierContext {
		let localctx: IdentifierContext = new IdentifierContext(this, this._ctx, this.state);
		this.enterRule(localctx, 26, GenericSqlParser.RULE_identifier);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 174;
			_la = this._input.LA(1);
			if(!(_la===25 || _la===27)) {
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

	public sempred(localctx: RuleContext, ruleIndex: number, predIndex: number): boolean {
		switch (ruleIndex) {
		case 7:
			return this.boolExp_sempred(localctx as BoolExpContext, predIndex);
		case 8:
			return this.exp_sempred(localctx as ExpContext, predIndex);
		}
		return true;
	}
	private boolExp_sempred(localctx: BoolExpContext, predIndex: number): boolean {
		switch (predIndex) {
		case 0:
			return this.precpred(this._ctx, 3);
		case 1:
			return this.precpred(this._ctx, 2);
		}
		return true;
	}
	private exp_sempred(localctx: ExpContext, predIndex: number): boolean {
		switch (predIndex) {
		case 2:
			return this.precpred(this._ctx, 12);
		case 3:
			return this.precpred(this._ctx, 11);
		}
		return true;
	}

	public static readonly _serializedATN: number[] = [4,1,31,177,2,0,7,0,2,
	1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,7,7,7,2,8,7,8,2,9,7,9,2,
	10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,3,
	0,37,8,0,1,1,1,1,1,1,1,1,1,1,1,1,3,1,45,8,1,1,2,1,2,1,3,1,3,1,3,5,3,52,
	8,3,10,3,12,3,55,9,3,1,4,1,4,3,4,59,8,4,1,5,1,5,3,5,63,8,5,1,5,3,5,66,8,
	5,1,6,1,6,3,6,70,8,6,1,6,3,6,73,8,6,1,7,1,7,1,7,1,7,3,7,79,8,7,1,7,1,7,
	1,7,1,7,1,7,1,7,5,7,87,8,7,10,7,12,7,90,9,7,1,8,1,8,1,8,1,8,1,8,1,8,1,8,
	5,8,99,8,8,10,8,12,8,102,9,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,
	8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,3,8,124,8,8,1,8,1,8,1,8,1,8,1,8,1,
	8,5,8,132,8,8,10,8,12,8,135,9,8,1,9,4,9,138,8,9,11,9,12,9,139,1,9,1,9,4,
	9,144,8,9,11,9,12,9,145,3,9,148,8,9,1,9,1,9,4,9,152,8,9,11,9,12,9,153,3,
	9,156,8,9,1,10,1,10,1,11,1,11,1,11,1,11,1,11,3,11,165,8,11,1,12,1,12,1,
	12,5,12,170,8,12,10,12,12,12,173,9,12,1,13,1,13,1,13,0,2,14,16,14,0,2,4,
	6,8,10,12,14,16,18,20,22,24,26,0,2,1,0,13,18,2,0,25,25,27,27,192,0,36,1,
	0,0,0,2,38,1,0,0,0,4,46,1,0,0,0,6,48,1,0,0,0,8,58,1,0,0,0,10,60,1,0,0,0,
	12,67,1,0,0,0,14,78,1,0,0,0,16,123,1,0,0,0,18,155,1,0,0,0,20,157,1,0,0,
	0,22,164,1,0,0,0,24,166,1,0,0,0,26,174,1,0,0,0,28,29,3,2,1,0,29,30,5,0,
	0,1,30,37,1,0,0,0,31,32,5,1,0,0,32,33,3,2,1,0,33,34,5,2,0,0,34,35,5,0,0,
	1,35,37,1,0,0,0,36,28,1,0,0,0,36,31,1,0,0,0,37,1,1,0,0,0,38,39,5,5,0,0,
	39,40,3,6,3,0,40,41,5,7,0,0,41,44,3,4,2,0,42,43,5,8,0,0,43,45,3,14,7,0,
	44,42,1,0,0,0,44,45,1,0,0,0,45,3,1,0,0,0,46,47,3,12,6,0,47,5,1,0,0,0,48,
	53,3,8,4,0,49,50,5,3,0,0,50,52,3,8,4,0,51,49,1,0,0,0,52,55,1,0,0,0,53,51,
	1,0,0,0,53,54,1,0,0,0,54,7,1,0,0,0,55,53,1,0,0,0,56,59,3,10,5,0,57,59,5,
	6,0,0,58,56,1,0,0,0,58,57,1,0,0,0,59,9,1,0,0,0,60,65,3,16,8,0,61,63,5,12,
	0,0,62,61,1,0,0,0,62,63,1,0,0,0,63,64,1,0,0,0,64,66,3,26,13,0,65,62,1,0,
	0,0,65,66,1,0,0,0,66,11,1,0,0,0,67,72,3,24,12,0,68,70,5,12,0,0,69,68,1,
	0,0,0,69,70,1,0,0,0,70,71,1,0,0,0,71,73,3,26,13,0,72,69,1,0,0,0,72,73,1,
	0,0,0,73,13,1,0,0,0,74,75,6,7,-1,0,75,79,3,16,8,0,76,77,5,11,0,0,77,79,
	3,14,7,1,78,74,1,0,0,0,78,76,1,0,0,0,79,88,1,0,0,0,80,81,10,3,0,0,81,82,
	5,9,0,0,82,87,3,14,7,4,83,84,10,2,0,0,84,85,5,10,0,0,85,87,3,14,7,3,86,
	80,1,0,0,0,86,83,1,0,0,0,87,90,1,0,0,0,88,86,1,0,0,0,88,89,1,0,0,0,89,15,
	1,0,0,0,90,88,1,0,0,0,91,92,6,8,-1,0,92,124,3,24,12,0,93,94,3,26,13,0,94,
	95,5,1,0,0,95,100,3,16,8,0,96,97,5,3,0,0,97,99,3,16,8,0,98,96,1,0,0,0,99,
	102,1,0,0,0,100,98,1,0,0,0,100,101,1,0,0,0,101,103,1,0,0,0,102,100,1,0,
	0,0,103,104,5,2,0,0,104,124,1,0,0,0,105,106,5,21,0,0,106,107,5,1,0,0,107,
	108,3,16,8,0,108,109,5,12,0,0,109,110,3,26,13,0,110,111,5,2,0,0,111,124,
	1,0,0,0,112,113,5,22,0,0,113,124,5,28,0,0,114,124,5,28,0,0,115,124,3,18,
	9,0,116,124,3,26,13,0,117,124,5,23,0,0,118,124,5,24,0,0,119,120,5,1,0,0,
	120,121,3,16,8,0,121,122,5,2,0,0,122,124,1,0,0,0,123,91,1,0,0,0,123,93,
	1,0,0,0,123,105,1,0,0,0,123,112,1,0,0,0,123,114,1,0,0,0,123,115,1,0,0,0,
	123,116,1,0,0,0,123,117,1,0,0,0,123,118,1,0,0,0,123,119,1,0,0,0,124,133,
	1,0,0,0,125,126,10,12,0,0,126,127,3,20,10,0,127,128,3,16,8,13,128,132,1,
	0,0,0,129,130,10,11,0,0,130,132,3,22,11,0,131,125,1,0,0,0,131,129,1,0,0,
	0,132,135,1,0,0,0,133,131,1,0,0,0,133,134,1,0,0,0,134,17,1,0,0,0,135,133,
	1,0,0,0,136,138,5,26,0,0,137,136,1,0,0,0,138,139,1,0,0,0,139,137,1,0,0,
	0,139,140,1,0,0,0,140,147,1,0,0,0,141,143,5,4,0,0,142,144,5,26,0,0,143,
	142,1,0,0,0,144,145,1,0,0,0,145,143,1,0,0,0,145,146,1,0,0,0,146,148,1,0,
	0,0,147,141,1,0,0,0,147,148,1,0,0,0,148,156,1,0,0,0,149,151,5,4,0,0,150,
	152,5,26,0,0,151,150,1,0,0,0,152,153,1,0,0,0,153,151,1,0,0,0,153,154,1,
	0,0,0,154,156,1,0,0,0,155,137,1,0,0,0,155,149,1,0,0,0,156,19,1,0,0,0,157,
	158,7,0,0,0,158,21,1,0,0,0,159,160,5,19,0,0,160,165,5,20,0,0,161,162,5,
	19,0,0,162,163,5,11,0,0,163,165,5,20,0,0,164,159,1,0,0,0,164,161,1,0,0,
	0,165,23,1,0,0,0,166,171,3,26,13,0,167,168,5,4,0,0,168,170,3,26,13,0,169,
	167,1,0,0,0,170,173,1,0,0,0,171,169,1,0,0,0,171,172,1,0,0,0,172,25,1,0,
	0,0,173,171,1,0,0,0,174,175,7,1,0,0,175,27,1,0,0,0,22,36,44,53,58,62,65,
	69,72,78,86,88,100,123,131,133,139,145,147,153,155,164,171];

	private static __ATN: ATN;
	public static get _ATN(): ATN {
		if (!GenericSqlParser.__ATN) {
			GenericSqlParser.__ATN = new ATNDeserializer().deserialize(GenericSqlParser._serializedATN);
		}

		return GenericSqlParser.__ATN;
	}


	static DecisionsToDFA = GenericSqlParser._ATN.decisionToState.map( (ds: DecisionState, index: number) => new DFA(ds, index) );

}

export class StatementContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public query(): QueryContext {
		return this.getTypedRuleContext(QueryContext, 0) as QueryContext;
	}
	public EOF(): TerminalNode {
		return this.getToken(GenericSqlParser.EOF, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_statement;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterStatement) {
	 		listener.enterStatement(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitStatement) {
	 		listener.exitStatement(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitStatement) {
			return visitor.visitStatement(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class QueryContext extends ParserRuleContext {
	public _from_!: FromTablesContext;
	public _where!: BoolExpContext;
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public SELECT(): TerminalNode {
		return this.getToken(GenericSqlParser.SELECT, 0);
	}
	public selectFields(): SelectFieldsContext {
		return this.getTypedRuleContext(SelectFieldsContext, 0) as SelectFieldsContext;
	}
	public FROM(): TerminalNode {
		return this.getToken(GenericSqlParser.FROM, 0);
	}
	public fromTables(): FromTablesContext {
		return this.getTypedRuleContext(FromTablesContext, 0) as FromTablesContext;
	}
	public WHERE(): TerminalNode {
		return this.getToken(GenericSqlParser.WHERE, 0);
	}
	public boolExp(): BoolExpContext {
		return this.getTypedRuleContext(BoolExpContext, 0) as BoolExpContext;
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_query;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterQuery) {
	 		listener.enterQuery(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitQuery) {
	 		listener.exitQuery(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitQuery) {
			return visitor.visitQuery(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FromTablesContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public aliasField(): AliasFieldContext {
		return this.getTypedRuleContext(AliasFieldContext, 0) as AliasFieldContext;
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_fromTables;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterFromTables) {
	 		listener.enterFromTables(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitFromTables) {
	 		listener.exitFromTables(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitFromTables) {
			return visitor.visitFromTables(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SelectFieldsContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public field_list(): FieldContext[] {
		return this.getTypedRuleContexts(FieldContext) as FieldContext[];
	}
	public field(i: number): FieldContext {
		return this.getTypedRuleContext(FieldContext, i) as FieldContext;
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_selectFields;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterSelectFields) {
	 		listener.enterSelectFields(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitSelectFields) {
	 		listener.exitSelectFields(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitSelectFields) {
			return visitor.visitSelectFields(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class FieldContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public selectField(): SelectFieldContext {
		return this.getTypedRuleContext(SelectFieldContext, 0) as SelectFieldContext;
	}
	public ASTERISK(): TerminalNode {
		return this.getToken(GenericSqlParser.ASTERISK, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_field;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterField) {
	 		listener.enterField(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitField) {
	 		listener.exitField(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitField) {
			return visitor.visitField(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class SelectFieldContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public exp(): ExpContext {
		return this.getTypedRuleContext(ExpContext, 0) as ExpContext;
	}
	public identifier(): IdentifierContext {
		return this.getTypedRuleContext(IdentifierContext, 0) as IdentifierContext;
	}
	public AS(): TerminalNode {
		return this.getToken(GenericSqlParser.AS, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_selectField;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterSelectField) {
	 		listener.enterSelectField(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitSelectField) {
	 		listener.exitSelectField(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitSelectField) {
			return visitor.visitSelectField(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class AliasFieldContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public idPath(): IdPathContext {
		return this.getTypedRuleContext(IdPathContext, 0) as IdPathContext;
	}
	public identifier(): IdentifierContext {
		return this.getTypedRuleContext(IdentifierContext, 0) as IdentifierContext;
	}
	public AS(): TerminalNode {
		return this.getToken(GenericSqlParser.AS, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_aliasField;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterAliasField) {
	 		listener.enterAliasField(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitAliasField) {
	 		listener.exitAliasField(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitAliasField) {
			return visitor.visitAliasField(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class BoolExpContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public exp(): ExpContext {
		return this.getTypedRuleContext(ExpContext, 0) as ExpContext;
	}
	public NOT(): TerminalNode {
		return this.getToken(GenericSqlParser.NOT, 0);
	}
	public boolExp_list(): BoolExpContext[] {
		return this.getTypedRuleContexts(BoolExpContext) as BoolExpContext[];
	}
	public boolExp(i: number): BoolExpContext {
		return this.getTypedRuleContext(BoolExpContext, i) as BoolExpContext;
	}
	public AND(): TerminalNode {
		return this.getToken(GenericSqlParser.AND, 0);
	}
	public OR(): TerminalNode {
		return this.getToken(GenericSqlParser.OR, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_boolExp;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterBoolExp) {
	 		listener.enterBoolExp(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitBoolExp) {
	 		listener.exitBoolExp(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitBoolExp) {
			return visitor.visitBoolExp(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class ExpContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public idPath(): IdPathContext {
		return this.getTypedRuleContext(IdPathContext, 0) as IdPathContext;
	}
	public identifier(): IdentifierContext {
		return this.getTypedRuleContext(IdentifierContext, 0) as IdentifierContext;
	}
	public exp_list(): ExpContext[] {
		return this.getTypedRuleContexts(ExpContext) as ExpContext[];
	}
	public exp(i: number): ExpContext {
		return this.getTypedRuleContext(ExpContext, i) as ExpContext;
	}
	public CAST(): TerminalNode {
		return this.getToken(GenericSqlParser.CAST, 0);
	}
	public AS(): TerminalNode {
		return this.getToken(GenericSqlParser.AS, 0);
	}
	public REGEXP(): TerminalNode {
		return this.getToken(GenericSqlParser.REGEXP, 0);
	}
	public STRING(): TerminalNode {
		return this.getToken(GenericSqlParser.STRING, 0);
	}
	public numeric(): NumericContext {
		return this.getTypedRuleContext(NumericContext, 0) as NumericContext;
	}
	public INDEXED_PARAM(): TerminalNode {
		return this.getToken(GenericSqlParser.INDEXED_PARAM, 0);
	}
	public PARAM_PLACEHOLDER(): TerminalNode {
		return this.getToken(GenericSqlParser.PARAM_PLACEHOLDER, 0);
	}
	public binaryOperator(): BinaryOperatorContext {
		return this.getTypedRuleContext(BinaryOperatorContext, 0) as BinaryOperatorContext;
	}
	public unaryOperator(): UnaryOperatorContext {
		return this.getTypedRuleContext(UnaryOperatorContext, 0) as UnaryOperatorContext;
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_exp;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterExp) {
	 		listener.enterExp(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitExp) {
	 		listener.exitExp(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitExp) {
			return visitor.visitExp(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class NumericContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public DIGIT_list(): TerminalNode[] {
	    	return this.getTokens(GenericSqlParser.DIGIT);
	}
	public DIGIT(i: number): TerminalNode {
		return this.getToken(GenericSqlParser.DIGIT, i);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_numeric;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterNumeric) {
	 		listener.enterNumeric(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitNumeric) {
	 		listener.exitNumeric(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitNumeric) {
			return visitor.visitNumeric(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class BinaryOperatorContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public LT(): TerminalNode {
		return this.getToken(GenericSqlParser.LT, 0);
	}
	public LTE(): TerminalNode {
		return this.getToken(GenericSqlParser.LTE, 0);
	}
	public GT(): TerminalNode {
		return this.getToken(GenericSqlParser.GT, 0);
	}
	public GTE(): TerminalNode {
		return this.getToken(GenericSqlParser.GTE, 0);
	}
	public EQUALS(): TerminalNode {
		return this.getToken(GenericSqlParser.EQUALS, 0);
	}
	public NOT_EQUALS(): TerminalNode {
		return this.getToken(GenericSqlParser.NOT_EQUALS, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_binaryOperator;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterBinaryOperator) {
	 		listener.enterBinaryOperator(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitBinaryOperator) {
	 		listener.exitBinaryOperator(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitBinaryOperator) {
			return visitor.visitBinaryOperator(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class UnaryOperatorContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public IS(): TerminalNode {
		return this.getToken(GenericSqlParser.IS, 0);
	}
	public NULL(): TerminalNode {
		return this.getToken(GenericSqlParser.NULL, 0);
	}
	public NOT(): TerminalNode {
		return this.getToken(GenericSqlParser.NOT, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_unaryOperator;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterUnaryOperator) {
	 		listener.enterUnaryOperator(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitUnaryOperator) {
	 		listener.exitUnaryOperator(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitUnaryOperator) {
			return visitor.visitUnaryOperator(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IdPathContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public identifier_list(): IdentifierContext[] {
		return this.getTypedRuleContexts(IdentifierContext) as IdentifierContext[];
	}
	public identifier(i: number): IdentifierContext {
		return this.getTypedRuleContext(IdentifierContext, i) as IdentifierContext;
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_idPath;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterIdPath) {
	 		listener.enterIdPath(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitIdPath) {
	 		listener.exitIdPath(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitIdPath) {
			return visitor.visitIdPath(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}


export class IdentifierContext extends ParserRuleContext {
	constructor(parser?: GenericSqlParser, parent?: ParserRuleContext, invokingState?: number) {
		super(parent, invokingState);
    	this.parser = parser;
	}
	public ID(): TerminalNode {
		return this.getToken(GenericSqlParser.ID, 0);
	}
	public QUOTED_ID(): TerminalNode {
		return this.getToken(GenericSqlParser.QUOTED_ID, 0);
	}
    public get ruleIndex(): number {
    	return GenericSqlParser.RULE_identifier;
	}
	public enterRule(listener: GenericSqlListener): void {
	    if(listener.enterIdentifier) {
	 		listener.enterIdentifier(this);
		}
	}
	public exitRule(listener: GenericSqlListener): void {
	    if(listener.exitIdentifier) {
	 		listener.exitIdentifier(this);
		}
	}
	// @Override
	public accept<Result>(visitor: GenericSqlVisitor<Result>): Result {
		if (visitor.visitIdentifier) {
			return visitor.visitIdentifier(this);
		} else {
			return visitor.visitChildren(this);
		}
	}
}
