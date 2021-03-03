// Generated from src/parser/GenericSql.g4 by ANTLR 4.9.0-SNAPSHOT


import { ParseTreeListener } from "antlr4ts/tree/ParseTreeListener";

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
 * This interface defines a complete listener for a parse tree produced by
 * `GenericSqlParser`.
 */
export interface GenericSqlListener extends ParseTreeListener {
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

