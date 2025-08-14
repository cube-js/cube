// Generated from src/parser/GenericSql.g4 by ANTLR 4.13.2
// @ts-nocheck

import {ParseTreeListener} from "antlr4";


import { StatementContext } from "./GenericSqlParser.js";
import { QueryContext } from "./GenericSqlParser.js";
import { FromTablesContext } from "./GenericSqlParser.js";
import { SelectFieldsContext } from "./GenericSqlParser.js";
import { FieldContext } from "./GenericSqlParser.js";
import { AliasFieldContext } from "./GenericSqlParser.js";
import { BoolExpContext } from "./GenericSqlParser.js";
import { ExpContext } from "./GenericSqlParser.js";
import { NumericContext } from "./GenericSqlParser.js";
import { BinaryOperatorContext } from "./GenericSqlParser.js";
import { UnaryOperatorContext } from "./GenericSqlParser.js";
import { IdPathContext } from "./GenericSqlParser.js";
import { IdentifierContext } from "./GenericSqlParser.js";


/**
 * This interface defines a complete listener for a parse tree produced by
 * `GenericSqlParser`.
 */
export default class GenericSqlListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by `GenericSqlParser.statement`.
	 * @param ctx the parse tree
	 */
	enterStatement?: (ctx: StatementContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.statement`.
	 * @param ctx the parse tree
	 */
	exitStatement?: (ctx: StatementContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.query`.
	 * @param ctx the parse tree
	 */
	enterQuery?: (ctx: QueryContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.query`.
	 * @param ctx the parse tree
	 */
	exitQuery?: (ctx: QueryContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.fromTables`.
	 * @param ctx the parse tree
	 */
	enterFromTables?: (ctx: FromTablesContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.fromTables`.
	 * @param ctx the parse tree
	 */
	exitFromTables?: (ctx: FromTablesContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.selectFields`.
	 * @param ctx the parse tree
	 */
	enterSelectFields?: (ctx: SelectFieldsContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.selectFields`.
	 * @param ctx the parse tree
	 */
	exitSelectFields?: (ctx: SelectFieldsContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.field`.
	 * @param ctx the parse tree
	 */
	enterField?: (ctx: FieldContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.field`.
	 * @param ctx the parse tree
	 */
	exitField?: (ctx: FieldContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.aliasField`.
	 * @param ctx the parse tree
	 */
	enterAliasField?: (ctx: AliasFieldContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.aliasField`.
	 * @param ctx the parse tree
	 */
	exitAliasField?: (ctx: AliasFieldContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.boolExp`.
	 * @param ctx the parse tree
	 */
	enterBoolExp?: (ctx: BoolExpContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.boolExp`.
	 * @param ctx the parse tree
	 */
	exitBoolExp?: (ctx: BoolExpContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.exp`.
	 * @param ctx the parse tree
	 */
	enterExp?: (ctx: ExpContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.exp`.
	 * @param ctx the parse tree
	 */
	exitExp?: (ctx: ExpContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.numeric`.
	 * @param ctx the parse tree
	 */
	enterNumeric?: (ctx: NumericContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.numeric`.
	 * @param ctx the parse tree
	 */
	exitNumeric?: (ctx: NumericContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.binaryOperator`.
	 * @param ctx the parse tree
	 */
	enterBinaryOperator?: (ctx: BinaryOperatorContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.binaryOperator`.
	 * @param ctx the parse tree
	 */
	exitBinaryOperator?: (ctx: BinaryOperatorContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.unaryOperator`.
	 * @param ctx the parse tree
	 */
	enterUnaryOperator?: (ctx: UnaryOperatorContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.unaryOperator`.
	 * @param ctx the parse tree
	 */
	exitUnaryOperator?: (ctx: UnaryOperatorContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.idPath`.
	 * @param ctx the parse tree
	 */
	enterIdPath?: (ctx: IdPathContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.idPath`.
	 * @param ctx the parse tree
	 */
	exitIdPath?: (ctx: IdPathContext) => void;
	/**
	 * Enter a parse tree produced by `GenericSqlParser.identifier`.
	 * @param ctx the parse tree
	 */
	enterIdentifier?: (ctx: IdentifierContext) => void;
	/**
	 * Exit a parse tree produced by `GenericSqlParser.identifier`.
	 * @param ctx the parse tree
	 */
	exitIdentifier?: (ctx: IdentifierContext) => void;
}

