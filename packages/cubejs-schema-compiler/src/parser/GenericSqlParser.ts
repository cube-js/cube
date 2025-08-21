// Generated from src/parser/GenericSql.g4 by ANTLR 4.13.2
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
	public static readonly INDEXED_PARAM = 22;
	public static readonly ID = 23;
	public static readonly DIGIT = 24;
	public static readonly QUOTED_ID = 25;
	public static readonly STRING = 26;
	public static readonly WHITESPACE = 27;
	public static override readonly EOF = Token.EOF;
	public static readonly RULE_statement = 0;
	public static readonly RULE_query = 1;
	public static readonly RULE_fromTables = 2;
	public static readonly RULE_selectFields = 3;
	public static readonly RULE_field = 4;
	public static readonly RULE_aliasField = 5;
	public static readonly RULE_boolExp = 6;
	public static readonly RULE_exp = 7;
	public static readonly RULE_numeric = 8;
	public static readonly RULE_binaryOperator = 9;
	public static readonly RULE_unaryOperator = 10;
	public static readonly RULE_idPath = 11;
	public static readonly RULE_identifier = 12;
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
                                                            "'NULL'", "'CAST'" ];
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
                                                             "CAST", "INDEXED_PARAM", 
                                                             "ID", "DIGIT", 
                                                             "QUOTED_ID", 
                                                             "STRING", "WHITESPACE" ];
	// tslint:disable:no-trailing-whitespace
	public static readonly ruleNames: string[] = [
		"statement", "query", "fromTables", "selectFields", "field", "aliasField", 
		"boolExp", "exp", "numeric", "binaryOperator", "unaryOperator", "idPath", 
		"identifier",
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
			this.state = 34;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 5:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 26;
				this.query();
				this.state = 27;
				this.match(GenericSqlParser.EOF);
				}
				break;
			case 1:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 29;
				this.match(GenericSqlParser.T__0);
				this.state = 30;
				this.query();
				this.state = 31;
				this.match(GenericSqlParser.T__1);
				this.state = 32;
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
			this.state = 36;
			this.match(GenericSqlParser.SELECT);
			this.state = 37;
			this.selectFields();
			this.state = 38;
			this.match(GenericSqlParser.FROM);
			this.state = 39;
			localctx._from_ = this.fromTables();
			this.state = 42;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la===8) {
				{
				this.state = 40;
				this.match(GenericSqlParser.WHERE);
				this.state = 41;
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
			this.state = 44;
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
			this.state = 46;
			this.field();
			this.state = 51;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la===3) {
				{
				{
				this.state = 47;
				this.match(GenericSqlParser.T__2);
				this.state = 48;
				this.field();
				}
				}
				this.state = 53;
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
			this.state = 56;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 23:
			case 25:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 54;
				this.aliasField();
				}
				break;
			case 6:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 55;
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
	public aliasField(): AliasFieldContext {
		let localctx: AliasFieldContext = new AliasFieldContext(this, this._ctx, this.state);
		this.enterRule(localctx, 10, GenericSqlParser.RULE_aliasField);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 58;
			this.idPath();
			this.state = 63;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & 41947136) !== 0)) {
				{
				this.state = 60;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la===12) {
					{
					this.state = 59;
					this.match(GenericSqlParser.AS);
					}
				}

				this.state = 62;
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
		let _startState: number = 12;
		this.enterRecursionRule(localctx, 12, GenericSqlParser.RULE_boolExp, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 69;
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
				{
				this.state = 66;
				this.exp(0);
				}
				break;
			case 11:
				{
				this.state = 67;
				this.match(GenericSqlParser.NOT);
				this.state = 68;
				this.boolExp(1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			this._ctx.stop = this._input.LT(-1);
			this.state = 79;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 8, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = localctx;
					{
					this.state = 77;
					this._errHandler.sync(this);
					switch ( this._interp.adaptivePredict(this._input, 7, this._ctx) ) {
					case 1:
						{
						localctx = new BoolExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_boolExp);
						this.state = 71;
						if (!(this.precpred(this._ctx, 3))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 3)");
						}
						this.state = 72;
						this.match(GenericSqlParser.AND);
						this.state = 73;
						this.boolExp(4);
						}
						break;
					case 2:
						{
						localctx = new BoolExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_boolExp);
						this.state = 74;
						if (!(this.precpred(this._ctx, 2))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 2)");
						}
						this.state = 75;
						this.match(GenericSqlParser.OR);
						this.state = 76;
						this.boolExp(3);
						}
						break;
					}
					}
				}
				this.state = 81;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 8, this._ctx);
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
		let _startState: number = 14;
		this.enterRecursionRule(localctx, 14, GenericSqlParser.RULE_exp, _p);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 111;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 10, this._ctx) ) {
			case 1:
				{
				this.state = 83;
				this.idPath();
				}
				break;
			case 2:
				{
				this.state = 84;
				this.identifier();
				this.state = 85;
				this.match(GenericSqlParser.T__0);
				{
				this.state = 86;
				this.exp(0);
				this.state = 91;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				while (_la===3) {
					{
					{
					this.state = 87;
					this.match(GenericSqlParser.T__2);
					this.state = 88;
					this.exp(0);
					}
					}
					this.state = 93;
					this._errHandler.sync(this);
					_la = this._input.LA(1);
				}
				}
				this.state = 94;
				this.match(GenericSqlParser.T__1);
				}
				break;
			case 3:
				{
				this.state = 96;
				this.match(GenericSqlParser.CAST);
				this.state = 97;
				this.match(GenericSqlParser.T__0);
				this.state = 98;
				this.exp(0);
				this.state = 99;
				this.match(GenericSqlParser.AS);
				this.state = 100;
				this.identifier();
				this.state = 101;
				this.match(GenericSqlParser.T__1);
				}
				break;
			case 4:
				{
				this.state = 103;
				this.match(GenericSqlParser.STRING);
				}
				break;
			case 5:
				{
				this.state = 104;
				this.numeric();
				}
				break;
			case 6:
				{
				this.state = 105;
				this.identifier();
				}
				break;
			case 7:
				{
				this.state = 106;
				this.match(GenericSqlParser.INDEXED_PARAM);
				}
				break;
			case 8:
				{
				this.state = 107;
				this.match(GenericSqlParser.T__0);
				this.state = 108;
				this.exp(0);
				this.state = 109;
				this.match(GenericSqlParser.T__1);
				}
				break;
			}
			this._ctx.stop = this._input.LT(-1);
			this.state = 121;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 12, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = localctx;
					{
					this.state = 119;
					this._errHandler.sync(this);
					switch ( this._interp.adaptivePredict(this._input, 11, this._ctx) ) {
					case 1:
						{
						localctx = new ExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_exp);
						this.state = 113;
						if (!(this.precpred(this._ctx, 10))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 10)");
						}
						this.state = 114;
						this.binaryOperator();
						this.state = 115;
						this.exp(11);
						}
						break;
					case 2:
						{
						localctx = new ExpContext(this, _parentctx, _parentState);
						this.pushNewRecursionContext(localctx, _startState, GenericSqlParser.RULE_exp);
						this.state = 117;
						if (!(this.precpred(this._ctx, 9))) {
							throw this.createFailedPredicateException("this.precpred(this._ctx, 9)");
						}
						this.state = 118;
						this.unaryOperator();
						}
						break;
					}
					}
				}
				this.state = 123;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 12, this._ctx);
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
		this.enterRule(localctx, 16, GenericSqlParser.RULE_numeric);
		try {
			let _alt: number;
			this.state = 143;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case 24:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 125;
				this._errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						this.state = 124;
						this.match(GenericSqlParser.DIGIT);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					this.state = 127;
					this._errHandler.sync(this);
					_alt = this._interp.adaptivePredict(this._input, 13, this._ctx);
				} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
				this.state = 135;
				this._errHandler.sync(this);
				switch ( this._interp.adaptivePredict(this._input, 15, this._ctx) ) {
				case 1:
					{
					this.state = 129;
					this.match(GenericSqlParser.T__3);
					this.state = 131;
					this._errHandler.sync(this);
					_alt = 1;
					do {
						switch (_alt) {
						case 1:
							{
							{
							this.state = 130;
							this.match(GenericSqlParser.DIGIT);
							}
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						this.state = 133;
						this._errHandler.sync(this);
						_alt = this._interp.adaptivePredict(this._input, 14, this._ctx);
					} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
					}
					break;
				}
				}
				break;
			case 4:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 137;
				this.match(GenericSqlParser.T__3);
				this.state = 139;
				this._errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						this.state = 138;
						this.match(GenericSqlParser.DIGIT);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					this.state = 141;
					this._errHandler.sync(this);
					_alt = this._interp.adaptivePredict(this._input, 16, this._ctx);
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
		this.enterRule(localctx, 18, GenericSqlParser.RULE_binaryOperator);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 145;
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
		this.enterRule(localctx, 20, GenericSqlParser.RULE_unaryOperator);
		try {
			this.state = 152;
			this._errHandler.sync(this);
			switch ( this._interp.adaptivePredict(this._input, 18, this._ctx) ) {
			case 1:
				this.enterOuterAlt(localctx, 1);
				{
				this.state = 147;
				this.match(GenericSqlParser.IS);
				this.state = 148;
				this.match(GenericSqlParser.NULL);
				}
				break;
			case 2:
				this.enterOuterAlt(localctx, 2);
				{
				this.state = 149;
				this.match(GenericSqlParser.IS);
				this.state = 150;
				this.match(GenericSqlParser.NOT);
				this.state = 151;
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
		this.enterRule(localctx, 22, GenericSqlParser.RULE_idPath);
		try {
			let _alt: number;
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 154;
			this.identifier();
			this.state = 159;
			this._errHandler.sync(this);
			_alt = this._interp.adaptivePredict(this._input, 19, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					{
					{
					this.state = 155;
					this.match(GenericSqlParser.T__3);
					this.state = 156;
					this.identifier();
					}
					}
				}
				this.state = 161;
				this._errHandler.sync(this);
				_alt = this._interp.adaptivePredict(this._input, 19, this._ctx);
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
		this.enterRule(localctx, 24, GenericSqlParser.RULE_identifier);
		let _la: number;
		try {
			this.enterOuterAlt(localctx, 1);
			{
			this.state = 162;
			_la = this._input.LA(1);
			if(!(_la===23 || _la===25)) {
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
		case 6:
			return this.boolExp_sempred(localctx as BoolExpContext, predIndex);
		case 7:
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
			return this.precpred(this._ctx, 10);
		case 3:
			return this.precpred(this._ctx, 9);
		}
		return true;
	}

	public static readonly _serializedATN: number[] = [4,1,27,165,2,0,7,0,2,
	1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,7,7,7,2,8,7,8,2,9,7,9,2,
	10,7,10,2,11,7,11,2,12,7,12,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,3,0,35,8,0,
	1,1,1,1,1,1,1,1,1,1,1,1,3,1,43,8,1,1,2,1,2,1,3,1,3,1,3,5,3,50,8,3,10,3,
	12,3,53,9,3,1,4,1,4,3,4,57,8,4,1,5,1,5,3,5,61,8,5,1,5,3,5,64,8,5,1,6,1,
	6,1,6,1,6,3,6,70,8,6,1,6,1,6,1,6,1,6,1,6,1,6,5,6,78,8,6,10,6,12,6,81,9,
	6,1,7,1,7,1,7,1,7,1,7,1,7,1,7,5,7,90,8,7,10,7,12,7,93,9,7,1,7,1,7,1,7,1,
	7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,7,3,7,112,8,7,1,7,1,
	7,1,7,1,7,1,7,1,7,5,7,120,8,7,10,7,12,7,123,9,7,1,8,4,8,126,8,8,11,8,12,
	8,127,1,8,1,8,4,8,132,8,8,11,8,12,8,133,3,8,136,8,8,1,8,1,8,4,8,140,8,8,
	11,8,12,8,141,3,8,144,8,8,1,9,1,9,1,10,1,10,1,10,1,10,1,10,3,10,153,8,10,
	1,11,1,11,1,11,5,11,158,8,11,10,11,12,11,161,9,11,1,12,1,12,1,12,0,2,12,
	14,13,0,2,4,6,8,10,12,14,16,18,20,22,24,0,2,1,0,13,18,2,0,23,23,25,25,177,
	0,34,1,0,0,0,2,36,1,0,0,0,4,44,1,0,0,0,6,46,1,0,0,0,8,56,1,0,0,0,10,58,
	1,0,0,0,12,69,1,0,0,0,14,111,1,0,0,0,16,143,1,0,0,0,18,145,1,0,0,0,20,152,
	1,0,0,0,22,154,1,0,0,0,24,162,1,0,0,0,26,27,3,2,1,0,27,28,5,0,0,1,28,35,
	1,0,0,0,29,30,5,1,0,0,30,31,3,2,1,0,31,32,5,2,0,0,32,33,5,0,0,1,33,35,1,
	0,0,0,34,26,1,0,0,0,34,29,1,0,0,0,35,1,1,0,0,0,36,37,5,5,0,0,37,38,3,6,
	3,0,38,39,5,7,0,0,39,42,3,4,2,0,40,41,5,8,0,0,41,43,3,12,6,0,42,40,1,0,
	0,0,42,43,1,0,0,0,43,3,1,0,0,0,44,45,3,10,5,0,45,5,1,0,0,0,46,51,3,8,4,
	0,47,48,5,3,0,0,48,50,3,8,4,0,49,47,1,0,0,0,50,53,1,0,0,0,51,49,1,0,0,0,
	51,52,1,0,0,0,52,7,1,0,0,0,53,51,1,0,0,0,54,57,3,10,5,0,55,57,5,6,0,0,56,
	54,1,0,0,0,56,55,1,0,0,0,57,9,1,0,0,0,58,63,3,22,11,0,59,61,5,12,0,0,60,
	59,1,0,0,0,60,61,1,0,0,0,61,62,1,0,0,0,62,64,3,24,12,0,63,60,1,0,0,0,63,
	64,1,0,0,0,64,11,1,0,0,0,65,66,6,6,-1,0,66,70,3,14,7,0,67,68,5,11,0,0,68,
	70,3,12,6,1,69,65,1,0,0,0,69,67,1,0,0,0,70,79,1,0,0,0,71,72,10,3,0,0,72,
	73,5,9,0,0,73,78,3,12,6,4,74,75,10,2,0,0,75,76,5,10,0,0,76,78,3,12,6,3,
	77,71,1,0,0,0,77,74,1,0,0,0,78,81,1,0,0,0,79,77,1,0,0,0,79,80,1,0,0,0,80,
	13,1,0,0,0,81,79,1,0,0,0,82,83,6,7,-1,0,83,112,3,22,11,0,84,85,3,24,12,
	0,85,86,5,1,0,0,86,91,3,14,7,0,87,88,5,3,0,0,88,90,3,14,7,0,89,87,1,0,0,
	0,90,93,1,0,0,0,91,89,1,0,0,0,91,92,1,0,0,0,92,94,1,0,0,0,93,91,1,0,0,0,
	94,95,5,2,0,0,95,112,1,0,0,0,96,97,5,21,0,0,97,98,5,1,0,0,98,99,3,14,7,
	0,99,100,5,12,0,0,100,101,3,24,12,0,101,102,5,2,0,0,102,112,1,0,0,0,103,
	112,5,26,0,0,104,112,3,16,8,0,105,112,3,24,12,0,106,112,5,22,0,0,107,108,
	5,1,0,0,108,109,3,14,7,0,109,110,5,2,0,0,110,112,1,0,0,0,111,82,1,0,0,0,
	111,84,1,0,0,0,111,96,1,0,0,0,111,103,1,0,0,0,111,104,1,0,0,0,111,105,1,
	0,0,0,111,106,1,0,0,0,111,107,1,0,0,0,112,121,1,0,0,0,113,114,10,10,0,0,
	114,115,3,18,9,0,115,116,3,14,7,11,116,120,1,0,0,0,117,118,10,9,0,0,118,
	120,3,20,10,0,119,113,1,0,0,0,119,117,1,0,0,0,120,123,1,0,0,0,121,119,1,
	0,0,0,121,122,1,0,0,0,122,15,1,0,0,0,123,121,1,0,0,0,124,126,5,24,0,0,125,
	124,1,0,0,0,126,127,1,0,0,0,127,125,1,0,0,0,127,128,1,0,0,0,128,135,1,0,
	0,0,129,131,5,4,0,0,130,132,5,24,0,0,131,130,1,0,0,0,132,133,1,0,0,0,133,
	131,1,0,0,0,133,134,1,0,0,0,134,136,1,0,0,0,135,129,1,0,0,0,135,136,1,0,
	0,0,136,144,1,0,0,0,137,139,5,4,0,0,138,140,5,24,0,0,139,138,1,0,0,0,140,
	141,1,0,0,0,141,139,1,0,0,0,141,142,1,0,0,0,142,144,1,0,0,0,143,125,1,0,
	0,0,143,137,1,0,0,0,144,17,1,0,0,0,145,146,7,0,0,0,146,19,1,0,0,0,147,148,
	5,19,0,0,148,153,5,20,0,0,149,150,5,19,0,0,150,151,5,11,0,0,151,153,5,20,
	0,0,152,147,1,0,0,0,152,149,1,0,0,0,153,21,1,0,0,0,154,159,3,24,12,0,155,
	156,5,4,0,0,156,158,3,24,12,0,157,155,1,0,0,0,158,161,1,0,0,0,159,157,1,
	0,0,0,159,160,1,0,0,0,160,23,1,0,0,0,161,159,1,0,0,0,162,163,7,1,0,0,163,
	25,1,0,0,0,20,34,42,51,56,60,63,69,77,79,91,111,119,121,127,133,135,141,
	143,152,159];

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
	public aliasField(): AliasFieldContext {
		return this.getTypedRuleContext(AliasFieldContext, 0) as AliasFieldContext;
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
	public STRING(): TerminalNode {
		return this.getToken(GenericSqlParser.STRING, 0);
	}
	public numeric(): NumericContext {
		return this.getTypedRuleContext(NumericContext, 0) as NumericContext;
	}
	public INDEXED_PARAM(): TerminalNode {
		return this.getToken(GenericSqlParser.INDEXED_PARAM, 0);
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
