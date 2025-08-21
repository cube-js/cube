// Generated from src/parser/GenericSql.g4 by ANTLR 4.13.2
// @ts-nocheck

import {ParseTreeVisitor} from 'antlr4';


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
 * This interface defines a complete generic visitor for a parse tree produced
 * by `GenericSqlParser`.
 *
 * @param <Result> The return type of the visit operation. Use `void` for
 * operations with no return type.
 */
export default class GenericSqlVisitor<Result> extends ParseTreeVisitor<Result> {
	/**
	 * Visit a parse tree produced by `GenericSqlParser.statement`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitStatement?: (ctx: StatementContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.query`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitQuery?: (ctx: QueryContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.fromTables`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitFromTables?: (ctx: FromTablesContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.selectFields`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitSelectFields?: (ctx: SelectFieldsContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.field`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitField?: (ctx: FieldContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.aliasField`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitAliasField?: (ctx: AliasFieldContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.boolExp`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBoolExp?: (ctx: BoolExpContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.exp`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitExp?: (ctx: ExpContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.numeric`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitNumeric?: (ctx: NumericContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.binaryOperator`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitBinaryOperator?: (ctx: BinaryOperatorContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.unaryOperator`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitUnaryOperator?: (ctx: UnaryOperatorContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.idPath`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIdPath?: (ctx: IdPathContext) => Result;
	/**
	 * Visit a parse tree produced by `GenericSqlParser.identifier`.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	visitIdentifier?: (ctx: IdentifierContext) => Result;
}

