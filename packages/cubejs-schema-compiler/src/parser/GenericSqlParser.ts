// Generated from src/parser/GenericSql.g4 by ANTLR 4.9.0-SNAPSHOT


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

import { GenericSqlListener } from "./GenericSqlListener";
import { GenericSqlVisitor } from "./GenericSqlVisitor";


export class GenericSqlParser extends Parser {
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
	// tslint:disable:no-trailing-whitespace
	public static readonly ruleNames: string[] = [
		"statement", "query", "fromTables", "selectFields", "field", "aliasField", 
		"boolExp", "exp", "numeric", "binaryOperator", "unaryOperator", "idPath", 
		"identifier",
	];

	private static readonly _LITERAL_NAMES: Array<string | undefined> = [
		undefined, "'('", "')'", "','", "'.'", "'SELECT'", "'*'", "'FROM'", "'WHERE'", 
		"'AND'", "'OR'", "'NOT'", "'AS'", "'<'", "'<='", "'>'", "'>='", "'='", 
		undefined, "'IS'", "'NULL'", "'CAST'",
	];
	private static readonly _SYMBOLIC_NAMES: Array<string | undefined> = [
		undefined, undefined, undefined, undefined, undefined, "SELECT", "ASTERISK", 
		"FROM", "WHERE", "AND", "OR", "NOT", "AS", "LT", "LTE", "GT", "GTE", "EQUALS", 
		"NOT_EQUALS", "IS", "NULL", "CAST", "INDEXED_PARAM", "ID", "DIGIT", "QUOTED_ID", 
		"STRING", "WHITESPACE",
	];
	public static readonly VOCABULARY: Vocabulary = new VocabularyImpl(GenericSqlParser._LITERAL_NAMES, GenericSqlParser._SYMBOLIC_NAMES, []);

	// @Override
	// @NotNull
	public get vocabulary(): Vocabulary {
		return GenericSqlParser.VOCABULARY;
	}
	// tslint:enable:no-trailing-whitespace

	// @Override
	public get grammarFileName(): string { return "GenericSql.g4"; }

	// @Override
	public get ruleNames(): string[] { return GenericSqlParser.ruleNames; }

	// @Override
	public get serializedATN(): string { return GenericSqlParser._serializedATN; }

	protected createFailedPredicateException(predicate?: string, message?: string): FailedPredicateException {
		return new FailedPredicateException(this, predicate, message);
	}

	constructor(input: TokenStream) {
		super(input);
		this._interp = new ParserATNSimulator(GenericSqlParser._ATN, this);
	}
	// @RuleVersion(0)
	public statement(): StatementContext {
		let _localctx: StatementContext = new StatementContext(this._ctx, this.state);
		this.enterRule(_localctx, 0, GenericSqlParser.RULE_statement);
		try {
			this.state = 34;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case GenericSqlParser.SELECT:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 26;
				this.query();
				this.state = 27;
				this.match(GenericSqlParser.EOF);
				}
				break;
			case GenericSqlParser.T__0:
				this.enterOuterAlt(_localctx, 2);
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
	public query(): QueryContext {
		let _localctx: QueryContext = new QueryContext(this._ctx, this.state);
		this.enterRule(_localctx, 2, GenericSqlParser.RULE_query);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 36;
			this.match(GenericSqlParser.SELECT);
			this.state = 37;
			this.selectFields();
			this.state = 38;
			this.match(GenericSqlParser.FROM);
			this.state = 39;
			_localctx._from = this.fromTables();
			this.state = 42;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if (_la === GenericSqlParser.WHERE) {
				{
				this.state = 40;
				this.match(GenericSqlParser.WHERE);
				this.state = 41;
				_localctx._where = this.boolExp(0);
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
	public fromTables(): FromTablesContext {
		let _localctx: FromTablesContext = new FromTablesContext(this._ctx, this.state);
		this.enterRule(_localctx, 4, GenericSqlParser.RULE_fromTables);
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 44;
			this.aliasField();
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
	public selectFields(): SelectFieldsContext {
		let _localctx: SelectFieldsContext = new SelectFieldsContext(this._ctx, this.state);
		this.enterRule(_localctx, 6, GenericSqlParser.RULE_selectFields);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			{
			this.state = 46;
			this.field();
			this.state = 51;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			while (_la === GenericSqlParser.T__2) {
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
	public field(): FieldContext {
		let _localctx: FieldContext = new FieldContext(this._ctx, this.state);
		this.enterRule(_localctx, 8, GenericSqlParser.RULE_field);
		try {
			this.state = 56;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case GenericSqlParser.ID:
			case GenericSqlParser.QUOTED_ID:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 54;
				this.aliasField();
				}
				break;
			case GenericSqlParser.ASTERISK:
				this.enterOuterAlt(_localctx, 2);
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
	public aliasField(): AliasFieldContext {
		let _localctx: AliasFieldContext = new AliasFieldContext(this._ctx, this.state);
		this.enterRule(_localctx, 10, GenericSqlParser.RULE_aliasField);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 58;
			this.idPath();
			this.state = 63;
			this._errHandler.sync(this);
			_la = this._input.LA(1);
			if ((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << GenericSqlParser.AS) | (1 << GenericSqlParser.ID) | (1 << GenericSqlParser.QUOTED_ID))) !== 0)) {
				{
				this.state = 60;
				this._errHandler.sync(this);
				_la = this._input.LA(1);
				if (_la === GenericSqlParser.AS) {
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

	public boolExp(): BoolExpContext;
	public boolExp(_p: number): BoolExpContext;
	// @RuleVersion(0)
	public boolExp(_p?: number): BoolExpContext {
		if (_p === undefined) {
			_p = 0;
		}

		let _parentctx: ParserRuleContext = this._ctx;
		let _parentState: number = this.state;
		let _localctx: BoolExpContext = new BoolExpContext(this._ctx, _parentState);
		let _prevctx: BoolExpContext = _localctx;
		let _startState: number = 12;
		this.enterRecursionRule(_localctx, 12, GenericSqlParser.RULE_boolExp, _p);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 69;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case GenericSqlParser.T__0:
			case GenericSqlParser.T__3:
			case GenericSqlParser.CAST:
			case GenericSqlParser.INDEXED_PARAM:
			case GenericSqlParser.ID:
			case GenericSqlParser.DIGIT:
			case GenericSqlParser.QUOTED_ID:
			case GenericSqlParser.STRING:
				{
				this.state = 66;
				this.exp(0);
				}
				break;
			case GenericSqlParser.NOT:
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
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 79;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 8, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					this.state = 77;
					this._errHandler.sync(this);
					switch ( this.interpreter.adaptivePredict(this._input, 7, this._ctx) ) {
					case 1:
						{
						_localctx = new BoolExpContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, GenericSqlParser.RULE_boolExp);
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
						_localctx = new BoolExpContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, GenericSqlParser.RULE_boolExp);
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
				_alt = this.interpreter.adaptivePredict(this._input, 8, this._ctx);
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
			this.unrollRecursionContexts(_parentctx);
		}
		return _localctx;
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
		let _localctx: ExpContext = new ExpContext(this._ctx, _parentState);
		let _prevctx: ExpContext = _localctx;
		let _startState: number = 14;
		this.enterRecursionRule(_localctx, 14, GenericSqlParser.RULE_exp, _p);
		let _la: number;
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 111;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 10, this._ctx) ) {
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
				while (_la === GenericSqlParser.T__2) {
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
			this._ctx._stop = this._input.tryLT(-1);
			this.state = 121;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 12, this._ctx);
			while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
				if (_alt === 1) {
					if (this._parseListeners != null) {
						this.triggerExitRuleEvent();
					}
					_prevctx = _localctx;
					{
					this.state = 119;
					this._errHandler.sync(this);
					switch ( this.interpreter.adaptivePredict(this._input, 11, this._ctx) ) {
					case 1:
						{
						_localctx = new ExpContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, GenericSqlParser.RULE_exp);
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
						_localctx = new ExpContext(_parentctx, _parentState);
						this.pushNewRecursionContext(_localctx, _startState, GenericSqlParser.RULE_exp);
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
				_alt = this.interpreter.adaptivePredict(this._input, 12, this._ctx);
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
			this.unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}
	// @RuleVersion(0)
	public numeric(): NumericContext {
		let _localctx: NumericContext = new NumericContext(this._ctx, this.state);
		this.enterRule(_localctx, 16, GenericSqlParser.RULE_numeric);
		try {
			let _alt: number;
			this.state = 143;
			this._errHandler.sync(this);
			switch (this._input.LA(1)) {
			case GenericSqlParser.DIGIT:
				this.enterOuterAlt(_localctx, 1);
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
					_alt = this.interpreter.adaptivePredict(this._input, 13, this._ctx);
				} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
				this.state = 135;
				this._errHandler.sync(this);
				switch ( this.interpreter.adaptivePredict(this._input, 15, this._ctx) ) {
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
						_alt = this.interpreter.adaptivePredict(this._input, 14, this._ctx);
					} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
					}
					break;
				}
				}
				break;
			case GenericSqlParser.T__3:
				this.enterOuterAlt(_localctx, 2);
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
					_alt = this.interpreter.adaptivePredict(this._input, 16, this._ctx);
				} while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
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
	public binaryOperator(): BinaryOperatorContext {
		let _localctx: BinaryOperatorContext = new BinaryOperatorContext(this._ctx, this.state);
		this.enterRule(_localctx, 18, GenericSqlParser.RULE_binaryOperator);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 145;
			_la = this._input.LA(1);
			if (!((((_la) & ~0x1F) === 0 && ((1 << _la) & ((1 << GenericSqlParser.LT) | (1 << GenericSqlParser.LTE) | (1 << GenericSqlParser.GT) | (1 << GenericSqlParser.GTE) | (1 << GenericSqlParser.EQUALS) | (1 << GenericSqlParser.NOT_EQUALS))) !== 0))) {
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
	public unaryOperator(): UnaryOperatorContext {
		let _localctx: UnaryOperatorContext = new UnaryOperatorContext(this._ctx, this.state);
		this.enterRule(_localctx, 20, GenericSqlParser.RULE_unaryOperator);
		try {
			this.state = 152;
			this._errHandler.sync(this);
			switch ( this.interpreter.adaptivePredict(this._input, 18, this._ctx) ) {
			case 1:
				this.enterOuterAlt(_localctx, 1);
				{
				this.state = 147;
				this.match(GenericSqlParser.IS);
				this.state = 148;
				this.match(GenericSqlParser.NULL);
				}
				break;

			case 2:
				this.enterOuterAlt(_localctx, 2);
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
	public idPath(): IdPathContext {
		let _localctx: IdPathContext = new IdPathContext(this._ctx, this.state);
		this.enterRule(_localctx, 22, GenericSqlParser.RULE_idPath);
		try {
			let _alt: number;
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 154;
			this.identifier();
			this.state = 159;
			this._errHandler.sync(this);
			_alt = this.interpreter.adaptivePredict(this._input, 19, this._ctx);
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
				_alt = this.interpreter.adaptivePredict(this._input, 19, this._ctx);
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
	public identifier(): IdentifierContext {
		let _localctx: IdentifierContext = new IdentifierContext(this._ctx, this.state);
		this.enterRule(_localctx, 24, GenericSqlParser.RULE_identifier);
		let _la: number;
		try {
			this.enterOuterAlt(_localctx, 1);
			{
			this.state = 162;
			_la = this._input.LA(1);
			if (!(_la === GenericSqlParser.ID || _la === GenericSqlParser.QUOTED_ID)) {
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

	public sempred(_localctx: RuleContext, ruleIndex: number, predIndex: number): boolean {
		switch (ruleIndex) {
		case 6:
			return this.boolExp_sempred(_localctx as BoolExpContext, predIndex);

		case 7:
			return this.exp_sempred(_localctx as ExpContext, predIndex);
		}
		return true;
	}
	private boolExp_sempred(_localctx: BoolExpContext, predIndex: number): boolean {
		switch (predIndex) {
		case 0:
			return this.precpred(this._ctx, 3);

		case 1:
			return this.precpred(this._ctx, 2);
		}
		return true;
	}
	private exp_sempred(_localctx: ExpContext, predIndex: number): boolean {
		switch (predIndex) {
		case 2:
			return this.precpred(this._ctx, 10);

		case 3:
			return this.precpred(this._ctx, 9);
		}
		return true;
	}

	public static readonly _serializedATN: string =
		"\x03\uC91D\uCABA\u058D\uAFBA\u4F53\u0607\uEA8B\uC241\x03\x1D\xA7\x04\x02" +
		"\t\x02\x04\x03\t\x03\x04\x04\t\x04\x04\x05\t\x05\x04\x06\t\x06\x04\x07" +
		"\t\x07\x04\b\t\b\x04\t\t\t\x04\n\t\n\x04\v\t\v\x04\f\t\f\x04\r\t\r\x04" +
		"\x0E\t\x0E\x03\x02\x03\x02\x03\x02\x03\x02\x03\x02\x03\x02\x03\x02\x03" +
		"\x02\x05\x02%\n\x02\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x05" +
		"\x03-\n\x03\x03\x04\x03\x04\x03\x05\x03\x05\x03\x05\x07\x054\n\x05\f\x05" +
		"\x0E\x057\v\x05\x03\x06\x03\x06\x05\x06;\n\x06\x03\x07\x03\x07\x05\x07" +
		"?\n\x07\x03\x07\x05\x07B\n\x07\x03\b\x03\b\x03\b\x03\b\x05\bH\n\b\x03" +
		"\b\x03\b\x03\b\x03\b\x03\b\x03\b\x07\bP\n\b\f\b\x0E\bS\v\b\x03\t\x03\t" +
		"\x03\t\x03\t\x03\t\x03\t\x03\t\x07\t\\\n\t\f\t\x0E\t_\v\t\x03\t\x03\t" +
		"\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03" +
		"\t\x03\t\x03\t\x03\t\x05\tr\n\t\x03\t\x03\t\x03\t\x03\t\x03\t\x03\t\x07" +
		"\tz\n\t\f\t\x0E\t}\v\t\x03\n\x06\n\x80\n\n\r\n\x0E\n\x81\x03\n\x03\n\x06" +
		"\n\x86\n\n\r\n\x0E\n\x87\x05\n\x8A\n\n\x03\n\x03\n\x06\n\x8E\n\n\r\n\x0E" +
		"\n\x8F\x05\n\x92\n\n\x03\v\x03\v\x03\f\x03\f\x03\f\x03\f\x03\f\x05\f\x9B" +
		"\n\f\x03\r\x03\r\x03\r\x07\r\xA0\n\r\f\r\x0E\r\xA3\v\r\x03\x0E\x03\x0E" +
		"\x03\x0E\x02\x02\x04\x0E\x10\x0F\x02\x02\x04\x02\x06\x02\b\x02\n\x02\f" +
		"\x02\x0E\x02\x10\x02\x12\x02\x14\x02\x16\x02\x18\x02\x1A\x02\x02\x04\x03" +
		"\x02\x0F\x14\x04\x02\x19\x19\x1B\x1B\x02\xB3\x02$\x03\x02\x02\x02\x04" +
		"&\x03\x02\x02\x02\x06.\x03\x02\x02\x02\b0\x03\x02\x02\x02\n:\x03\x02\x02" +
		"\x02\f<\x03\x02\x02\x02\x0EG\x03\x02\x02\x02\x10q\x03\x02\x02\x02\x12" +
		"\x91\x03\x02\x02\x02\x14\x93\x03\x02\x02\x02\x16\x9A\x03\x02\x02\x02\x18" +
		"\x9C\x03\x02\x02\x02\x1A\xA4\x03\x02\x02\x02\x1C\x1D\x05\x04\x03\x02\x1D" +
		"\x1E\x07\x02\x02\x03\x1E%\x03\x02\x02\x02\x1F \x07\x03\x02\x02 !\x05\x04" +
		"\x03\x02!\"\x07\x04\x02\x02\"#\x07\x02\x02\x03#%\x03\x02\x02\x02$\x1C" +
		"\x03\x02\x02\x02$\x1F\x03\x02\x02\x02%\x03\x03\x02\x02\x02&\'\x07\x07" +
		"\x02\x02\'(\x05\b\x05\x02()\x07\t\x02\x02),\x05\x06\x04\x02*+\x07\n\x02" +
		"\x02+-\x05\x0E\b\x02,*\x03\x02\x02\x02,-\x03\x02\x02\x02-\x05\x03\x02" +
		"\x02\x02./\x05\f\x07\x02/\x07\x03\x02\x02\x0205\x05\n\x06\x0212\x07\x05" +
		"\x02\x0224\x05\n\x06\x0231\x03\x02\x02\x0247\x03\x02\x02\x0253\x03\x02" +
		"\x02\x0256\x03\x02\x02\x026\t\x03\x02\x02\x0275\x03\x02\x02\x028;\x05" +
		"\f\x07\x029;\x07\b\x02\x02:8\x03\x02\x02\x02:9\x03\x02\x02\x02;\v\x03" +
		"\x02\x02\x02<A\x05\x18\r\x02=?\x07\x0E\x02\x02>=\x03\x02\x02\x02>?\x03" +
		"\x02\x02\x02?@\x03\x02\x02\x02@B\x05\x1A\x0E\x02A>\x03\x02\x02\x02AB\x03" +
		"\x02\x02\x02B\r\x03\x02\x02\x02CD\b\b\x01\x02DH\x05\x10\t\x02EF\x07\r" +
		"\x02\x02FH\x05\x0E\b\x03GC\x03\x02\x02\x02GE\x03\x02\x02\x02HQ\x03\x02" +
		"\x02\x02IJ\f\x05\x02\x02JK\x07\v\x02\x02KP\x05\x0E\b\x06LM\f\x04\x02\x02" +
		"MN\x07\f\x02\x02NP\x05\x0E\b\x05OI\x03\x02\x02\x02OL\x03\x02\x02\x02P" +
		"S\x03\x02\x02\x02QO\x03\x02\x02\x02QR\x03\x02\x02\x02R\x0F\x03\x02\x02" +
		"\x02SQ\x03\x02\x02\x02TU\b\t\x01\x02Ur\x05\x18\r\x02VW\x05\x1A\x0E\x02" +
		"WX\x07\x03\x02\x02X]\x05\x10\t\x02YZ\x07\x05\x02\x02Z\\\x05\x10\t\x02" +
		"[Y\x03\x02\x02\x02\\_\x03\x02\x02\x02][\x03\x02\x02\x02]^\x03\x02\x02" +
		"\x02^`\x03\x02\x02\x02_]\x03\x02\x02\x02`a\x07\x04\x02\x02ar\x03\x02\x02" +
		"\x02bc\x07\x17\x02\x02cd\x07\x03\x02\x02de\x05\x10\t\x02ef\x07\x0E\x02" +
		"\x02fg\x05\x1A\x0E\x02gh\x07\x04\x02\x02hr\x03\x02\x02\x02ir\x07\x1C\x02" +
		"\x02jr\x05\x12\n\x02kr\x05\x1A\x0E\x02lr\x07\x18\x02\x02mn\x07\x03\x02" +
		"\x02no\x05\x10\t\x02op\x07\x04\x02\x02pr\x03\x02\x02\x02qT\x03\x02\x02" +
		"\x02qV\x03\x02\x02\x02qb\x03\x02\x02\x02qi\x03\x02\x02\x02qj\x03\x02\x02" +
		"\x02qk\x03\x02\x02\x02ql\x03\x02\x02\x02qm\x03\x02\x02\x02r{\x03\x02\x02" +
		"\x02st\f\f\x02\x02tu\x05\x14\v\x02uv\x05\x10\t\rvz\x03\x02\x02\x02wx\f" +
		"\v\x02\x02xz\x05\x16\f\x02ys\x03\x02\x02\x02yw\x03\x02\x02\x02z}\x03\x02" +
		"\x02\x02{y\x03\x02\x02\x02{|\x03\x02\x02\x02|\x11\x03\x02\x02\x02}{\x03" +
		"\x02\x02\x02~\x80\x07\x1A\x02\x02\x7F~\x03\x02\x02\x02\x80\x81\x03\x02" +
		"\x02\x02\x81\x7F\x03\x02\x02\x02\x81\x82\x03\x02\x02\x02\x82\x89\x03\x02" +
		"\x02\x02\x83\x85\x07\x06\x02\x02\x84\x86\x07\x1A\x02\x02\x85\x84\x03\x02" +
		"\x02\x02\x86\x87\x03\x02\x02\x02\x87\x85\x03\x02\x02\x02\x87\x88\x03\x02" +
		"\x02\x02\x88\x8A\x03\x02\x02\x02\x89\x83\x03\x02\x02\x02\x89\x8A\x03\x02" +
		"\x02\x02\x8A\x92\x03\x02\x02\x02\x8B\x8D\x07\x06\x02\x02\x8C\x8E\x07\x1A" +
		"\x02\x02\x8D\x8C\x03\x02\x02\x02\x8E\x8F\x03\x02\x02\x02\x8F\x8D\x03\x02" +
		"\x02\x02\x8F\x90\x03\x02\x02\x02\x90\x92\x03\x02\x02\x02\x91\x7F\x03\x02" +
		"\x02\x02\x91\x8B\x03\x02\x02\x02\x92\x13\x03\x02\x02\x02\x93\x94\t\x02" +
		"\x02\x02\x94\x15\x03\x02\x02\x02\x95\x96\x07\x15\x02\x02\x96\x9B\x07\x16" +
		"\x02\x02\x97\x98\x07\x15\x02\x02\x98\x99\x07\r\x02\x02\x99\x9B\x07\x16" +
		"\x02\x02\x9A\x95\x03\x02\x02\x02\x9A\x97\x03\x02\x02\x02\x9B\x17\x03\x02" +
		"\x02\x02\x9C\xA1\x05\x1A\x0E\x02\x9D\x9E\x07\x06\x02\x02\x9E\xA0\x05\x1A" +
		"\x0E\x02\x9F\x9D\x03\x02\x02\x02\xA0\xA3\x03\x02\x02\x02\xA1\x9F\x03\x02" +
		"\x02\x02\xA1\xA2\x03\x02\x02\x02\xA2\x19\x03\x02\x02\x02\xA3\xA1\x03\x02" +
		"\x02\x02\xA4\xA5\t\x03\x02\x02\xA5\x1B\x03\x02\x02\x02\x16$,5:>AGOQ]q" +
		"y{\x81\x87\x89\x8F\x91\x9A\xA1";
	public static __ATN: ATN;
	public static get _ATN(): ATN {
		if (!GenericSqlParser.__ATN) {
			GenericSqlParser.__ATN = new ATNDeserializer().deserialize(Utils.toCharArray(GenericSqlParser._serializedATN));
		}

		return GenericSqlParser.__ATN;
	}

}

export class StatementContext extends ParserRuleContext {
	public query(): QueryContext {
		return this.getRuleContext(0, QueryContext);
	}
	public EOF(): TerminalNode { return this.getToken(GenericSqlParser.EOF, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_statement; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterStatement) {
			listener.enterStatement(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitStatement) {
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
	public _from!: FromTablesContext;
	public _where!: BoolExpContext;
	public SELECT(): TerminalNode { return this.getToken(GenericSqlParser.SELECT, 0); }
	public selectFields(): SelectFieldsContext {
		return this.getRuleContext(0, SelectFieldsContext);
	}
	public FROM(): TerminalNode { return this.getToken(GenericSqlParser.FROM, 0); }
	public fromTables(): FromTablesContext {
		return this.getRuleContext(0, FromTablesContext);
	}
	public WHERE(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.WHERE, 0); }
	public boolExp(): BoolExpContext | undefined {
		return this.tryGetRuleContext(0, BoolExpContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_query; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterQuery) {
			listener.enterQuery(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitQuery) {
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
	public aliasField(): AliasFieldContext {
		return this.getRuleContext(0, AliasFieldContext);
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_fromTables; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterFromTables) {
			listener.enterFromTables(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitFromTables) {
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
	public field(): FieldContext[];
	public field(i: number): FieldContext;
	public field(i?: number): FieldContext | FieldContext[] {
		if (i === undefined) {
			return this.getRuleContexts(FieldContext);
		} else {
			return this.getRuleContext(i, FieldContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_selectFields; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterSelectFields) {
			listener.enterSelectFields(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitSelectFields) {
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
	public aliasField(): AliasFieldContext | undefined {
		return this.tryGetRuleContext(0, AliasFieldContext);
	}
	public ASTERISK(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.ASTERISK, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_field; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterField) {
			listener.enterField(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitField) {
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
	public idPath(): IdPathContext {
		return this.getRuleContext(0, IdPathContext);
	}
	public identifier(): IdentifierContext | undefined {
		return this.tryGetRuleContext(0, IdentifierContext);
	}
	public AS(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.AS, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_aliasField; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterAliasField) {
			listener.enterAliasField(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitAliasField) {
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
	public exp(): ExpContext | undefined {
		return this.tryGetRuleContext(0, ExpContext);
	}
	public boolExp(): BoolExpContext[];
	public boolExp(i: number): BoolExpContext;
	public boolExp(i?: number): BoolExpContext | BoolExpContext[] {
		if (i === undefined) {
			return this.getRuleContexts(BoolExpContext);
		} else {
			return this.getRuleContext(i, BoolExpContext);
		}
	}
	public AND(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.AND, 0); }
	public OR(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.OR, 0); }
	public NOT(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.NOT, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_boolExp; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterBoolExp) {
			listener.enterBoolExp(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitBoolExp) {
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
	public exp(): ExpContext[];
	public exp(i: number): ExpContext;
	public exp(i?: number): ExpContext | ExpContext[] {
		if (i === undefined) {
			return this.getRuleContexts(ExpContext);
		} else {
			return this.getRuleContext(i, ExpContext);
		}
	}
	public binaryOperator(): BinaryOperatorContext | undefined {
		return this.tryGetRuleContext(0, BinaryOperatorContext);
	}
	public unaryOperator(): UnaryOperatorContext | undefined {
		return this.tryGetRuleContext(0, UnaryOperatorContext);
	}
	public idPath(): IdPathContext | undefined {
		return this.tryGetRuleContext(0, IdPathContext);
	}
	public identifier(): IdentifierContext | undefined {
		return this.tryGetRuleContext(0, IdentifierContext);
	}
	public CAST(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.CAST, 0); }
	public AS(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.AS, 0); }
	public STRING(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.STRING, 0); }
	public numeric(): NumericContext | undefined {
		return this.tryGetRuleContext(0, NumericContext);
	}
	public INDEXED_PARAM(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.INDEXED_PARAM, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_exp; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterExp) {
			listener.enterExp(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitExp) {
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
	public DIGIT(): TerminalNode[];
	public DIGIT(i: number): TerminalNode;
	public DIGIT(i?: number): TerminalNode | TerminalNode[] {
		if (i === undefined) {
			return this.getTokens(GenericSqlParser.DIGIT);
		} else {
			return this.getToken(GenericSqlParser.DIGIT, i);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_numeric; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterNumeric) {
			listener.enterNumeric(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitNumeric) {
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
	public LT(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.LT, 0); }
	public LTE(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.LTE, 0); }
	public GT(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.GT, 0); }
	public GTE(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.GTE, 0); }
	public EQUALS(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.EQUALS, 0); }
	public NOT_EQUALS(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.NOT_EQUALS, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_binaryOperator; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterBinaryOperator) {
			listener.enterBinaryOperator(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitBinaryOperator) {
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
	public IS(): TerminalNode { return this.getToken(GenericSqlParser.IS, 0); }
	public NULL(): TerminalNode { return this.getToken(GenericSqlParser.NULL, 0); }
	public NOT(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.NOT, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_unaryOperator; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterUnaryOperator) {
			listener.enterUnaryOperator(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitUnaryOperator) {
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
	public identifier(): IdentifierContext[];
	public identifier(i: number): IdentifierContext;
	public identifier(i?: number): IdentifierContext | IdentifierContext[] {
		if (i === undefined) {
			return this.getRuleContexts(IdentifierContext);
		} else {
			return this.getRuleContext(i, IdentifierContext);
		}
	}
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_idPath; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterIdPath) {
			listener.enterIdPath(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitIdPath) {
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
	public ID(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.ID, 0); }
	public QUOTED_ID(): TerminalNode | undefined { return this.tryGetToken(GenericSqlParser.QUOTED_ID, 0); }
	constructor(parent: ParserRuleContext | undefined, invokingState: number) {
		super(parent, invokingState);
	}
	// @Override
	public get ruleIndex(): number { return GenericSqlParser.RULE_identifier; }
	// @Override
	public enterRule(listener: GenericSqlListener): void {
		if (listener.enterIdentifier) {
			listener.enterIdentifier(this);
		}
	}
	// @Override
	public exitRule(listener: GenericSqlListener): void {
		if (listener.exitIdentifier) {
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


