// Generated from src/parser/Python3Parser.g4 by ANTLR 4.13.2
// @ts-nocheck

import {ParseTreeListener} from "antlr4";


import { File_inputContext } from "./Python3Parser.js";
import { Single_inputContext } from "./Python3Parser.js";
import { Eval_inputContext } from "./Python3Parser.js";
import { DecoratorContext } from "./Python3Parser.js";
import { DecoratorsContext } from "./Python3Parser.js";
import { DecoratedContext } from "./Python3Parser.js";
import { Async_funcdefContext } from "./Python3Parser.js";
import { FuncdefContext } from "./Python3Parser.js";
import { ParametersContext } from "./Python3Parser.js";
import { TypedargslistContext } from "./Python3Parser.js";
import { TfpdefContext } from "./Python3Parser.js";
import { VarargslistContext } from "./Python3Parser.js";
import { VfpdefContext } from "./Python3Parser.js";
import { StmtContext } from "./Python3Parser.js";
import { Simple_stmtContext } from "./Python3Parser.js";
import { Small_stmtContext } from "./Python3Parser.js";
import { Expr_stmtContext } from "./Python3Parser.js";
import { AnnassignContext } from "./Python3Parser.js";
import { Testlist_star_exprContext } from "./Python3Parser.js";
import { AugassignContext } from "./Python3Parser.js";
import { Del_stmtContext } from "./Python3Parser.js";
import { Pass_stmtContext } from "./Python3Parser.js";
import { Flow_stmtContext } from "./Python3Parser.js";
import { Break_stmtContext } from "./Python3Parser.js";
import { Continue_stmtContext } from "./Python3Parser.js";
import { Return_stmtContext } from "./Python3Parser.js";
import { Yield_stmtContext } from "./Python3Parser.js";
import { Raise_stmtContext } from "./Python3Parser.js";
import { Import_stmtContext } from "./Python3Parser.js";
import { Import_nameContext } from "./Python3Parser.js";
import { Import_fromContext } from "./Python3Parser.js";
import { Import_as_nameContext } from "./Python3Parser.js";
import { Dotted_as_nameContext } from "./Python3Parser.js";
import { Import_as_namesContext } from "./Python3Parser.js";
import { Dotted_as_namesContext } from "./Python3Parser.js";
import { Dotted_nameContext } from "./Python3Parser.js";
import { Global_stmtContext } from "./Python3Parser.js";
import { Nonlocal_stmtContext } from "./Python3Parser.js";
import { Assert_stmtContext } from "./Python3Parser.js";
import { Compound_stmtContext } from "./Python3Parser.js";
import { Async_stmtContext } from "./Python3Parser.js";
import { If_stmtContext } from "./Python3Parser.js";
import { While_stmtContext } from "./Python3Parser.js";
import { For_stmtContext } from "./Python3Parser.js";
import { Try_stmtContext } from "./Python3Parser.js";
import { With_stmtContext } from "./Python3Parser.js";
import { With_itemContext } from "./Python3Parser.js";
import { Except_clauseContext } from "./Python3Parser.js";
import { SuiteContext } from "./Python3Parser.js";
import { TestContext } from "./Python3Parser.js";
import { Test_nocondContext } from "./Python3Parser.js";
import { LambdefContext } from "./Python3Parser.js";
import { Lambdef_nocondContext } from "./Python3Parser.js";
import { Or_testContext } from "./Python3Parser.js";
import { And_testContext } from "./Python3Parser.js";
import { Not_testContext } from "./Python3Parser.js";
import { ComparisonContext } from "./Python3Parser.js";
import { Comp_opContext } from "./Python3Parser.js";
import { Star_exprContext } from "./Python3Parser.js";
import { ExprContext } from "./Python3Parser.js";
import { Xor_exprContext } from "./Python3Parser.js";
import { And_exprContext } from "./Python3Parser.js";
import { Shift_exprContext } from "./Python3Parser.js";
import { Arith_exprContext } from "./Python3Parser.js";
import { TermContext } from "./Python3Parser.js";
import { FactorContext } from "./Python3Parser.js";
import { PowerContext } from "./Python3Parser.js";
import { Atom_exprContext } from "./Python3Parser.js";
import { AtomContext } from "./Python3Parser.js";
import { Testlist_compContext } from "./Python3Parser.js";
import { TrailerContext } from "./Python3Parser.js";
import { SubscriptlistContext } from "./Python3Parser.js";
import { SubscriptContext } from "./Python3Parser.js";
import { SliceopContext } from "./Python3Parser.js";
import { ExprlistContext } from "./Python3Parser.js";
import { TestlistContext } from "./Python3Parser.js";
import { DictorsetmakerContext } from "./Python3Parser.js";
import { ClassdefContext } from "./Python3Parser.js";
import { CallArgumentsContext } from "./Python3Parser.js";
import { ArglistContext } from "./Python3Parser.js";
import { ArgumentContext } from "./Python3Parser.js";
import { Comp_iterContext } from "./Python3Parser.js";
import { Comp_forContext } from "./Python3Parser.js";
import { Comp_ifContext } from "./Python3Parser.js";
import { Encoding_declContext } from "./Python3Parser.js";
import { Yield_exprContext } from "./Python3Parser.js";
import { Yield_argContext } from "./Python3Parser.js";
import { String_templateContext } from "./Python3Parser.js";
import { Single_string_template_atomContext } from "./Python3Parser.js";
import { Double_string_template_atomContext } from "./Python3Parser.js";


/**
 * This interface defines a complete listener for a parse tree produced by
 * `Python3Parser`.
 */
export default class Python3ParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by `Python3Parser.file_input`.
	 * @param ctx the parse tree
	 */
	enterFile_input?: (ctx: File_inputContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.file_input`.
	 * @param ctx the parse tree
	 */
	exitFile_input?: (ctx: File_inputContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.single_input`.
	 * @param ctx the parse tree
	 */
	enterSingle_input?: (ctx: Single_inputContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.single_input`.
	 * @param ctx the parse tree
	 */
	exitSingle_input?: (ctx: Single_inputContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.eval_input`.
	 * @param ctx the parse tree
	 */
	enterEval_input?: (ctx: Eval_inputContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.eval_input`.
	 * @param ctx the parse tree
	 */
	exitEval_input?: (ctx: Eval_inputContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.decorator`.
	 * @param ctx the parse tree
	 */
	enterDecorator?: (ctx: DecoratorContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.decorator`.
	 * @param ctx the parse tree
	 */
	exitDecorator?: (ctx: DecoratorContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.decorators`.
	 * @param ctx the parse tree
	 */
	enterDecorators?: (ctx: DecoratorsContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.decorators`.
	 * @param ctx the parse tree
	 */
	exitDecorators?: (ctx: DecoratorsContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.decorated`.
	 * @param ctx the parse tree
	 */
	enterDecorated?: (ctx: DecoratedContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.decorated`.
	 * @param ctx the parse tree
	 */
	exitDecorated?: (ctx: DecoratedContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.async_funcdef`.
	 * @param ctx the parse tree
	 */
	enterAsync_funcdef?: (ctx: Async_funcdefContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.async_funcdef`.
	 * @param ctx the parse tree
	 */
	exitAsync_funcdef?: (ctx: Async_funcdefContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.funcdef`.
	 * @param ctx the parse tree
	 */
	enterFuncdef?: (ctx: FuncdefContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.funcdef`.
	 * @param ctx the parse tree
	 */
	exitFuncdef?: (ctx: FuncdefContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.parameters`.
	 * @param ctx the parse tree
	 */
	enterParameters?: (ctx: ParametersContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.parameters`.
	 * @param ctx the parse tree
	 */
	exitParameters?: (ctx: ParametersContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.typedargslist`.
	 * @param ctx the parse tree
	 */
	enterTypedargslist?: (ctx: TypedargslistContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.typedargslist`.
	 * @param ctx the parse tree
	 */
	exitTypedargslist?: (ctx: TypedargslistContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.tfpdef`.
	 * @param ctx the parse tree
	 */
	enterTfpdef?: (ctx: TfpdefContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.tfpdef`.
	 * @param ctx the parse tree
	 */
	exitTfpdef?: (ctx: TfpdefContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.varargslist`.
	 * @param ctx the parse tree
	 */
	enterVarargslist?: (ctx: VarargslistContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.varargslist`.
	 * @param ctx the parse tree
	 */
	exitVarargslist?: (ctx: VarargslistContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.vfpdef`.
	 * @param ctx the parse tree
	 */
	enterVfpdef?: (ctx: VfpdefContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.vfpdef`.
	 * @param ctx the parse tree
	 */
	exitVfpdef?: (ctx: VfpdefContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.stmt`.
	 * @param ctx the parse tree
	 */
	enterStmt?: (ctx: StmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.stmt`.
	 * @param ctx the parse tree
	 */
	exitStmt?: (ctx: StmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.simple_stmt`.
	 * @param ctx the parse tree
	 */
	enterSimple_stmt?: (ctx: Simple_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.simple_stmt`.
	 * @param ctx the parse tree
	 */
	exitSimple_stmt?: (ctx: Simple_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.small_stmt`.
	 * @param ctx the parse tree
	 */
	enterSmall_stmt?: (ctx: Small_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.small_stmt`.
	 * @param ctx the parse tree
	 */
	exitSmall_stmt?: (ctx: Small_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.expr_stmt`.
	 * @param ctx the parse tree
	 */
	enterExpr_stmt?: (ctx: Expr_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.expr_stmt`.
	 * @param ctx the parse tree
	 */
	exitExpr_stmt?: (ctx: Expr_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.annassign`.
	 * @param ctx the parse tree
	 */
	enterAnnassign?: (ctx: AnnassignContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.annassign`.
	 * @param ctx the parse tree
	 */
	exitAnnassign?: (ctx: AnnassignContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.testlist_star_expr`.
	 * @param ctx the parse tree
	 */
	enterTestlist_star_expr?: (ctx: Testlist_star_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.testlist_star_expr`.
	 * @param ctx the parse tree
	 */
	exitTestlist_star_expr?: (ctx: Testlist_star_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.augassign`.
	 * @param ctx the parse tree
	 */
	enterAugassign?: (ctx: AugassignContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.augassign`.
	 * @param ctx the parse tree
	 */
	exitAugassign?: (ctx: AugassignContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.del_stmt`.
	 * @param ctx the parse tree
	 */
	enterDel_stmt?: (ctx: Del_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.del_stmt`.
	 * @param ctx the parse tree
	 */
	exitDel_stmt?: (ctx: Del_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.pass_stmt`.
	 * @param ctx the parse tree
	 */
	enterPass_stmt?: (ctx: Pass_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.pass_stmt`.
	 * @param ctx the parse tree
	 */
	exitPass_stmt?: (ctx: Pass_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.flow_stmt`.
	 * @param ctx the parse tree
	 */
	enterFlow_stmt?: (ctx: Flow_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.flow_stmt`.
	 * @param ctx the parse tree
	 */
	exitFlow_stmt?: (ctx: Flow_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.break_stmt`.
	 * @param ctx the parse tree
	 */
	enterBreak_stmt?: (ctx: Break_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.break_stmt`.
	 * @param ctx the parse tree
	 */
	exitBreak_stmt?: (ctx: Break_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.continue_stmt`.
	 * @param ctx the parse tree
	 */
	enterContinue_stmt?: (ctx: Continue_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.continue_stmt`.
	 * @param ctx the parse tree
	 */
	exitContinue_stmt?: (ctx: Continue_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.return_stmt`.
	 * @param ctx the parse tree
	 */
	enterReturn_stmt?: (ctx: Return_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.return_stmt`.
	 * @param ctx the parse tree
	 */
	exitReturn_stmt?: (ctx: Return_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.yield_stmt`.
	 * @param ctx the parse tree
	 */
	enterYield_stmt?: (ctx: Yield_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.yield_stmt`.
	 * @param ctx the parse tree
	 */
	exitYield_stmt?: (ctx: Yield_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.raise_stmt`.
	 * @param ctx the parse tree
	 */
	enterRaise_stmt?: (ctx: Raise_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.raise_stmt`.
	 * @param ctx the parse tree
	 */
	exitRaise_stmt?: (ctx: Raise_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.import_stmt`.
	 * @param ctx the parse tree
	 */
	enterImport_stmt?: (ctx: Import_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.import_stmt`.
	 * @param ctx the parse tree
	 */
	exitImport_stmt?: (ctx: Import_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.import_name`.
	 * @param ctx the parse tree
	 */
	enterImport_name?: (ctx: Import_nameContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.import_name`.
	 * @param ctx the parse tree
	 */
	exitImport_name?: (ctx: Import_nameContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.import_from`.
	 * @param ctx the parse tree
	 */
	enterImport_from?: (ctx: Import_fromContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.import_from`.
	 * @param ctx the parse tree
	 */
	exitImport_from?: (ctx: Import_fromContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.import_as_name`.
	 * @param ctx the parse tree
	 */
	enterImport_as_name?: (ctx: Import_as_nameContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.import_as_name`.
	 * @param ctx the parse tree
	 */
	exitImport_as_name?: (ctx: Import_as_nameContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.dotted_as_name`.
	 * @param ctx the parse tree
	 */
	enterDotted_as_name?: (ctx: Dotted_as_nameContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.dotted_as_name`.
	 * @param ctx the parse tree
	 */
	exitDotted_as_name?: (ctx: Dotted_as_nameContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.import_as_names`.
	 * @param ctx the parse tree
	 */
	enterImport_as_names?: (ctx: Import_as_namesContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.import_as_names`.
	 * @param ctx the parse tree
	 */
	exitImport_as_names?: (ctx: Import_as_namesContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.dotted_as_names`.
	 * @param ctx the parse tree
	 */
	enterDotted_as_names?: (ctx: Dotted_as_namesContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.dotted_as_names`.
	 * @param ctx the parse tree
	 */
	exitDotted_as_names?: (ctx: Dotted_as_namesContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.dotted_name`.
	 * @param ctx the parse tree
	 */
	enterDotted_name?: (ctx: Dotted_nameContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.dotted_name`.
	 * @param ctx the parse tree
	 */
	exitDotted_name?: (ctx: Dotted_nameContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.global_stmt`.
	 * @param ctx the parse tree
	 */
	enterGlobal_stmt?: (ctx: Global_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.global_stmt`.
	 * @param ctx the parse tree
	 */
	exitGlobal_stmt?: (ctx: Global_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.nonlocal_stmt`.
	 * @param ctx the parse tree
	 */
	enterNonlocal_stmt?: (ctx: Nonlocal_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.nonlocal_stmt`.
	 * @param ctx the parse tree
	 */
	exitNonlocal_stmt?: (ctx: Nonlocal_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.assert_stmt`.
	 * @param ctx the parse tree
	 */
	enterAssert_stmt?: (ctx: Assert_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.assert_stmt`.
	 * @param ctx the parse tree
	 */
	exitAssert_stmt?: (ctx: Assert_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.compound_stmt`.
	 * @param ctx the parse tree
	 */
	enterCompound_stmt?: (ctx: Compound_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.compound_stmt`.
	 * @param ctx the parse tree
	 */
	exitCompound_stmt?: (ctx: Compound_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.async_stmt`.
	 * @param ctx the parse tree
	 */
	enterAsync_stmt?: (ctx: Async_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.async_stmt`.
	 * @param ctx the parse tree
	 */
	exitAsync_stmt?: (ctx: Async_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.if_stmt`.
	 * @param ctx the parse tree
	 */
	enterIf_stmt?: (ctx: If_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.if_stmt`.
	 * @param ctx the parse tree
	 */
	exitIf_stmt?: (ctx: If_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.while_stmt`.
	 * @param ctx the parse tree
	 */
	enterWhile_stmt?: (ctx: While_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.while_stmt`.
	 * @param ctx the parse tree
	 */
	exitWhile_stmt?: (ctx: While_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.for_stmt`.
	 * @param ctx the parse tree
	 */
	enterFor_stmt?: (ctx: For_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.for_stmt`.
	 * @param ctx the parse tree
	 */
	exitFor_stmt?: (ctx: For_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.try_stmt`.
	 * @param ctx the parse tree
	 */
	enterTry_stmt?: (ctx: Try_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.try_stmt`.
	 * @param ctx the parse tree
	 */
	exitTry_stmt?: (ctx: Try_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.with_stmt`.
	 * @param ctx the parse tree
	 */
	enterWith_stmt?: (ctx: With_stmtContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.with_stmt`.
	 * @param ctx the parse tree
	 */
	exitWith_stmt?: (ctx: With_stmtContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.with_item`.
	 * @param ctx the parse tree
	 */
	enterWith_item?: (ctx: With_itemContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.with_item`.
	 * @param ctx the parse tree
	 */
	exitWith_item?: (ctx: With_itemContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.except_clause`.
	 * @param ctx the parse tree
	 */
	enterExcept_clause?: (ctx: Except_clauseContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.except_clause`.
	 * @param ctx the parse tree
	 */
	exitExcept_clause?: (ctx: Except_clauseContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.suite`.
	 * @param ctx the parse tree
	 */
	enterSuite?: (ctx: SuiteContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.suite`.
	 * @param ctx the parse tree
	 */
	exitSuite?: (ctx: SuiteContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.test`.
	 * @param ctx the parse tree
	 */
	enterTest?: (ctx: TestContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.test`.
	 * @param ctx the parse tree
	 */
	exitTest?: (ctx: TestContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.test_nocond`.
	 * @param ctx the parse tree
	 */
	enterTest_nocond?: (ctx: Test_nocondContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.test_nocond`.
	 * @param ctx the parse tree
	 */
	exitTest_nocond?: (ctx: Test_nocondContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.lambdef`.
	 * @param ctx the parse tree
	 */
	enterLambdef?: (ctx: LambdefContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.lambdef`.
	 * @param ctx the parse tree
	 */
	exitLambdef?: (ctx: LambdefContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.lambdef_nocond`.
	 * @param ctx the parse tree
	 */
	enterLambdef_nocond?: (ctx: Lambdef_nocondContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.lambdef_nocond`.
	 * @param ctx the parse tree
	 */
	exitLambdef_nocond?: (ctx: Lambdef_nocondContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.or_test`.
	 * @param ctx the parse tree
	 */
	enterOr_test?: (ctx: Or_testContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.or_test`.
	 * @param ctx the parse tree
	 */
	exitOr_test?: (ctx: Or_testContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.and_test`.
	 * @param ctx the parse tree
	 */
	enterAnd_test?: (ctx: And_testContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.and_test`.
	 * @param ctx the parse tree
	 */
	exitAnd_test?: (ctx: And_testContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.not_test`.
	 * @param ctx the parse tree
	 */
	enterNot_test?: (ctx: Not_testContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.not_test`.
	 * @param ctx the parse tree
	 */
	exitNot_test?: (ctx: Not_testContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.comparison`.
	 * @param ctx the parse tree
	 */
	enterComparison?: (ctx: ComparisonContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.comparison`.
	 * @param ctx the parse tree
	 */
	exitComparison?: (ctx: ComparisonContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.comp_op`.
	 * @param ctx the parse tree
	 */
	enterComp_op?: (ctx: Comp_opContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.comp_op`.
	 * @param ctx the parse tree
	 */
	exitComp_op?: (ctx: Comp_opContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.star_expr`.
	 * @param ctx the parse tree
	 */
	enterStar_expr?: (ctx: Star_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.star_expr`.
	 * @param ctx the parse tree
	 */
	exitStar_expr?: (ctx: Star_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.expr`.
	 * @param ctx the parse tree
	 */
	enterExpr?: (ctx: ExprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.expr`.
	 * @param ctx the parse tree
	 */
	exitExpr?: (ctx: ExprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.xor_expr`.
	 * @param ctx the parse tree
	 */
	enterXor_expr?: (ctx: Xor_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.xor_expr`.
	 * @param ctx the parse tree
	 */
	exitXor_expr?: (ctx: Xor_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.and_expr`.
	 * @param ctx the parse tree
	 */
	enterAnd_expr?: (ctx: And_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.and_expr`.
	 * @param ctx the parse tree
	 */
	exitAnd_expr?: (ctx: And_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.shift_expr`.
	 * @param ctx the parse tree
	 */
	enterShift_expr?: (ctx: Shift_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.shift_expr`.
	 * @param ctx the parse tree
	 */
	exitShift_expr?: (ctx: Shift_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.arith_expr`.
	 * @param ctx the parse tree
	 */
	enterArith_expr?: (ctx: Arith_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.arith_expr`.
	 * @param ctx the parse tree
	 */
	exitArith_expr?: (ctx: Arith_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.term`.
	 * @param ctx the parse tree
	 */
	enterTerm?: (ctx: TermContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.term`.
	 * @param ctx the parse tree
	 */
	exitTerm?: (ctx: TermContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.factor`.
	 * @param ctx the parse tree
	 */
	enterFactor?: (ctx: FactorContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.factor`.
	 * @param ctx the parse tree
	 */
	exitFactor?: (ctx: FactorContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.power`.
	 * @param ctx the parse tree
	 */
	enterPower?: (ctx: PowerContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.power`.
	 * @param ctx the parse tree
	 */
	exitPower?: (ctx: PowerContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.atom_expr`.
	 * @param ctx the parse tree
	 */
	enterAtom_expr?: (ctx: Atom_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.atom_expr`.
	 * @param ctx the parse tree
	 */
	exitAtom_expr?: (ctx: Atom_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.atom`.
	 * @param ctx the parse tree
	 */
	enterAtom?: (ctx: AtomContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.atom`.
	 * @param ctx the parse tree
	 */
	exitAtom?: (ctx: AtomContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.testlist_comp`.
	 * @param ctx the parse tree
	 */
	enterTestlist_comp?: (ctx: Testlist_compContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.testlist_comp`.
	 * @param ctx the parse tree
	 */
	exitTestlist_comp?: (ctx: Testlist_compContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.trailer`.
	 * @param ctx the parse tree
	 */
	enterTrailer?: (ctx: TrailerContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.trailer`.
	 * @param ctx the parse tree
	 */
	exitTrailer?: (ctx: TrailerContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.subscriptlist`.
	 * @param ctx the parse tree
	 */
	enterSubscriptlist?: (ctx: SubscriptlistContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.subscriptlist`.
	 * @param ctx the parse tree
	 */
	exitSubscriptlist?: (ctx: SubscriptlistContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.subscript`.
	 * @param ctx the parse tree
	 */
	enterSubscript?: (ctx: SubscriptContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.subscript`.
	 * @param ctx the parse tree
	 */
	exitSubscript?: (ctx: SubscriptContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.sliceop`.
	 * @param ctx the parse tree
	 */
	enterSliceop?: (ctx: SliceopContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.sliceop`.
	 * @param ctx the parse tree
	 */
	exitSliceop?: (ctx: SliceopContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.exprlist`.
	 * @param ctx the parse tree
	 */
	enterExprlist?: (ctx: ExprlistContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.exprlist`.
	 * @param ctx the parse tree
	 */
	exitExprlist?: (ctx: ExprlistContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.testlist`.
	 * @param ctx the parse tree
	 */
	enterTestlist?: (ctx: TestlistContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.testlist`.
	 * @param ctx the parse tree
	 */
	exitTestlist?: (ctx: TestlistContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.dictorsetmaker`.
	 * @param ctx the parse tree
	 */
	enterDictorsetmaker?: (ctx: DictorsetmakerContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.dictorsetmaker`.
	 * @param ctx the parse tree
	 */
	exitDictorsetmaker?: (ctx: DictorsetmakerContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.classdef`.
	 * @param ctx the parse tree
	 */
	enterClassdef?: (ctx: ClassdefContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.classdef`.
	 * @param ctx the parse tree
	 */
	exitClassdef?: (ctx: ClassdefContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.callArguments`.
	 * @param ctx the parse tree
	 */
	enterCallArguments?: (ctx: CallArgumentsContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.callArguments`.
	 * @param ctx the parse tree
	 */
	exitCallArguments?: (ctx: CallArgumentsContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.arglist`.
	 * @param ctx the parse tree
	 */
	enterArglist?: (ctx: ArglistContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.arglist`.
	 * @param ctx the parse tree
	 */
	exitArglist?: (ctx: ArglistContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.argument`.
	 * @param ctx the parse tree
	 */
	enterArgument?: (ctx: ArgumentContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.argument`.
	 * @param ctx the parse tree
	 */
	exitArgument?: (ctx: ArgumentContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.comp_iter`.
	 * @param ctx the parse tree
	 */
	enterComp_iter?: (ctx: Comp_iterContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.comp_iter`.
	 * @param ctx the parse tree
	 */
	exitComp_iter?: (ctx: Comp_iterContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.comp_for`.
	 * @param ctx the parse tree
	 */
	enterComp_for?: (ctx: Comp_forContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.comp_for`.
	 * @param ctx the parse tree
	 */
	exitComp_for?: (ctx: Comp_forContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.comp_if`.
	 * @param ctx the parse tree
	 */
	enterComp_if?: (ctx: Comp_ifContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.comp_if`.
	 * @param ctx the parse tree
	 */
	exitComp_if?: (ctx: Comp_ifContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.encoding_decl`.
	 * @param ctx the parse tree
	 */
	enterEncoding_decl?: (ctx: Encoding_declContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.encoding_decl`.
	 * @param ctx the parse tree
	 */
	exitEncoding_decl?: (ctx: Encoding_declContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.yield_expr`.
	 * @param ctx the parse tree
	 */
	enterYield_expr?: (ctx: Yield_exprContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.yield_expr`.
	 * @param ctx the parse tree
	 */
	exitYield_expr?: (ctx: Yield_exprContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.yield_arg`.
	 * @param ctx the parse tree
	 */
	enterYield_arg?: (ctx: Yield_argContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.yield_arg`.
	 * @param ctx the parse tree
	 */
	exitYield_arg?: (ctx: Yield_argContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.string_template`.
	 * @param ctx the parse tree
	 */
	enterString_template?: (ctx: String_templateContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.string_template`.
	 * @param ctx the parse tree
	 */
	exitString_template?: (ctx: String_templateContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.single_string_template_atom`.
	 * @param ctx the parse tree
	 */
	enterSingle_string_template_atom?: (ctx: Single_string_template_atomContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.single_string_template_atom`.
	 * @param ctx the parse tree
	 */
	exitSingle_string_template_atom?: (ctx: Single_string_template_atomContext) => void;
	/**
	 * Enter a parse tree produced by `Python3Parser.double_string_template_atom`.
	 * @param ctx the parse tree
	 */
	enterDouble_string_template_atom?: (ctx: Double_string_template_atomContext) => void;
	/**
	 * Exit a parse tree produced by `Python3Parser.double_string_template_atom`.
	 * @param ctx the parse tree
	 */
	exitDouble_string_template_atom?: (ctx: Double_string_template_atomContext) => void;
}

