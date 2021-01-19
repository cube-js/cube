// Generated from src/parser/GenericSql.g4 by ANTLR 4.9.0-SNAPSHOT


import { ParseTreeVisitor } from "antlr4ts/tree/ParseTreeVisitor";

import { StatementContext } from "./GenericSqlParser";
import { QueryContext } from "./GenericSqlParser";
import { FromTablesContext } from "./GenericSqlParser";
import { SelectFieldsContext } from "./GenericSqlParser";
import { FieldContext } from "./GenericSqlParser";
import { AliasFieldContext } from "./GenericSqlParser";
import { BoolExpContext } from "./GenericSqlParser";
import { ExpContext } from "./GenericSqlParser";
import { NumericContext } from "./GenericSqlParser";
import { BinaryOperatorContext } from "./GenericSqlParser";
import { UnaryOperatorContext } from "./GenericSqlParser";
import { IdPathContext } from "./GenericSqlParser";
import { IdentifierContext } from "./GenericSqlParser";


/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by `GenericSqlParser`.
 *
 * @param <Result> The return type of the visit operation. Use `void` for
 * operations with no return type.
 */
export interface GenericSqlVisitor<Result> extends ParseTreeVisitor<Result> {
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

