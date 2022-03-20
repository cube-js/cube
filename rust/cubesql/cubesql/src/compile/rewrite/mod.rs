pub mod language;

use crate::compile::engine::df::scan::CubeScanNode;
use crate::compile::engine::provider::CubeContext;
use crate::sql::auth_service::AuthContext;
use crate::CubeError;
use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryTimeDimension};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::catalog::TableReference;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::window_frames::WindowFrame;
use datafusion::logical_plan::{
    build_join_schema, exprlist_to_fields, normalize_cols, DFField, DFSchema, DFSchemaRef, Expr,
    JoinConstraint, JoinType, LogicalPlan, Operator, Partitioning,
};
use datafusion::logical_plan::{Column, ExprRewriter};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::window_functions::WindowFunction;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::FileType;
use datafusion::sql::planner::ContextProvider;
use egg::{
    rewrite, Analysis, Applier, CostFunction, DidMerge, Language, Pattern, PatternAst, Subst,
    Symbol, Var,
};
use egg::{EGraph, Extractor, Id, RecExpr, Rewrite, Runner};
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Index;
use std::str::FromStr;
use std::sync::Arc;

trace_macros!(false);

crate::plan_to_language! {
    pub enum LogicalPlanLanguage {
        Projection {
            expr: Vec<Expr>,
            input: Arc<LogicalPlan>,
            schema: DFSchemaRef,
            alias: Option<String>,
        },
        Filter {
            predicate: Expr,
            input: Arc<LogicalPlan>,
        },
        Window {
            input: Arc<LogicalPlan>,
            window_expr: Vec<Expr>,
            schema: DFSchemaRef,
        },
        Aggregate {
            input: Arc<LogicalPlan>,
            group_expr: Vec<Expr>,
            aggr_expr: Vec<Expr>,
            schema: DFSchemaRef,
        },
        Sort {
            exp: Vec<Expr>,
            input: Arc<LogicalPlan>,
        },
        Join {
            left: Arc<LogicalPlan>,
            right: Arc<LogicalPlan>,
            left_on: Vec<Column>,
            right_on: Vec<Column>,
            join_type: JoinType,
            join_constraint: JoinConstraint,
            schema: DFSchemaRef,
        },
        CrossJoin {
            left: Arc<LogicalPlan>,
            right: Arc<LogicalPlan>,
            schema: DFSchemaRef,
        },
        Repartition {
            input: Arc<LogicalPlan>,
        },
        Union {
            inputs: Vec<LogicalPlan>,
            schema: DFSchemaRef,
            alias: Option<String>,
        },
        TableScan {
            source_table_name: String,
            table_name: String,
            projection: Option<Vec<usize>>,
            projected_schema: DFSchemaRef,
            filters: Vec<Expr>,
            limit: Option<usize>,
        },
        EmptyRelation {
            produce_one_row: bool,
            schema: DFSchemaRef,
        },
        Limit {
            n: usize,
            input: Arc<LogicalPlan>,
        },
        CreateExternalTable {
            schema: DFSchemaRef,
            name: String,
            location: String,
            has_header: bool,
        },
        Extension {
            node: Arc<LogicalPlan>,
        },

        AliasExpr {
            expr: Box<Expr>,
            alias: String,
        },
        ColumnExpr {
            column: Column,
        },
        ScalarVariableExpr {
            variable: Vec<String>,
        },
        LiteralExpr { value: ScalarValue, },
        BinaryExpr {
            left: Box<Expr>,
            op: Operator,
            right: Box<Expr>,
        },
        NotExpr { expr: Box<Expr>, },
        IsNotNullExpr { expr: Box<Expr>, },
        IsNullExpr { expr: Box<Expr>, },
        NegativeExpr { expr: Box<Expr>, },
        BetweenExpr {
            expr: Box<Expr>,
            negated: bool,
            low: Box<Expr>,
            high: Box<Expr>,
        },
        CaseExpr {
            expr: Option<Box<Expr>>,
            when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
            else_expr: Option<Box<Expr>>,
        },
        CastExpr {
            expr: Box<Expr>,
            data_type: DataType,
        },
        TryCastExpr {
            expr: Box<Expr>,
            data_type: DataType,
        },
        SortExpr {
            expr: Box<Expr>,
            asc: bool,
            nulls_first: bool,
        },
        ScalarFunctionExpr {
            fun: BuiltinScalarFunction,
            args: Vec<Expr>,
        },
        ScalarUDFExpr {
            fun: Arc<ScalarUDF>,
            args: Vec<Expr>,
        },
        AggregateFunctionExpr {
            fun: AggregateFunction,
            args: Vec<Expr>,
            distinct: bool,
        },
        WindowFunctionExpr {
            fun: WindowFunction,
            args: Vec<Expr>,
            partition_by: Vec<Expr>,
            order_by: Vec<Expr>,
            window_frame: Option<WindowFrame>,
        },
        AggregateUDFExpr {
            fun: Arc<AggregateUDF>,
            args: Vec<Expr>,
        },
        InListExpr {
            expr: Box<Expr>,
            list: Vec<Expr>,
            negated: bool,
        },
        WildcardExpr {},

        CubeScan {
            cube: Arc<LogicalPlan>,
            members: Vec<LogicalPlan>,
            filters: Vec<LogicalPlan>,
            order: Vec<LogicalPlan>,
        },
        Measure {
            name: String,
            expr: Arc<Expr>,
        },
        Dimension {
            name: String,
            expr: Arc<Expr>,
        },
        Order {
            member: String,
            asc: bool,
        },
        TimeDimension {
            name: String,
            granularity: Option<String>,
            dateRange: Option<Vec<String>>,
            expr: Arc<Expr>,
        },
        MemberAlias {
            name: String,
        },
        MemberReplacer {
            members: Vec<LogicalPlan>,
            cube: Arc<LogicalPlan>,
        },
        OrderReplacer {
            sort_expr: Vec<LogicalPlan>,
            column_name_to_member: Vec<(String, String)>,
            cube: Option<String>,
        },
        ColumnAliasReplacer {
            members: Vec<LogicalPlan>,
            aliases: Vec<(String, String)>,
            cube: Option<String>,
        },
    }
}

trace_macros!(false);

macro_rules! add_data_node {
    ($converter:expr, $value_expr:expr, $field_variant:ident) => {
        $converter
            .graph
            .add(LogicalPlanLanguage::$field_variant($field_variant(
                $value_expr.clone(),
            )))
    };
}

macro_rules! add_expr_list_node {
    ($converter:expr, $value_expr:expr, $field_variant:ident) => {{
        let list = $value_expr
            .iter()
            .map(|expr| $converter.add_expr(expr))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = $converter
            .graph
            .add(LogicalPlanLanguage::$field_variant(Vec::new()));
        for i in list.into_iter().rev() {
            current = $converter
                .graph
                .add(LogicalPlanLanguage::$field_variant(vec![i, current]));
        }
        current
    }};
}

macro_rules! add_plan_list_node {
    ($converter:expr, $value_expr:expr, $field_variant:ident) => {{
        let list = $value_expr
            .iter()
            .map(|expr| $converter.add_logical_plan(expr))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = $converter
            .graph
            .add(LogicalPlanLanguage::$field_variant(Vec::new()));
        for i in list.into_iter().rev() {
            current = $converter
                .graph
                .add(LogicalPlanLanguage::$field_variant(vec![i, current]));
        }
        current
    }};
}

macro_rules! var_iter {
    ($eclass:expr, $field_variant:ident) => {{
        $eclass.nodes.iter().filter_map(|node| match node {
            LogicalPlanLanguage::$field_variant($field_variant(v)) => Some(v),
            _ => None,
        })
    }};
}

pub struct LogicalPlanToLanguageConverter {
    graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    cube_context: CubeContext,
}

impl LogicalPlanToLanguageConverter {
    pub fn new(cube_context: CubeContext) -> Self {
        Self {
            graph: EGraph::<LogicalPlanLanguage, LogicalPlanAnalysis>::new(LogicalPlanAnalysis {
                cube_context: cube_context.clone(),
            }),
            cube_context,
        }
    }

    pub fn add_expr(&mut self, expr: &Expr) -> Result<Id, CubeError> {
        Ok(match expr {
            Expr::Alias(expr, alias) => {
                let expr = self.add_expr(expr)?;
                let alias = add_data_node!(self, alias, AliasExprAlias);
                self.graph
                    .add(LogicalPlanLanguage::AliasExpr([expr, alias]))
            }
            Expr::Column(column) => {
                let column = add_data_node!(self, column, ColumnExprColumn);
                self.graph.add(LogicalPlanLanguage::ColumnExpr([column]))
            }
            Expr::ScalarVariable(variable) => {
                let variable = add_data_node!(self, variable, ScalarVariableExprVariable);
                self.graph
                    .add(LogicalPlanLanguage::ScalarVariableExpr([variable]))
            }
            Expr::Literal(value) => {
                let value = add_data_node!(self, value, LiteralExprValue);
                self.graph.add(LogicalPlanLanguage::LiteralExpr([value]))
            }
            Expr::BinaryExpr { left, op, right } => {
                let left = self.add_expr(left)?;
                let op = add_data_node!(self, op, BinaryExprOp);
                let right = self.add_expr(right)?;
                self.graph
                    .add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
            }
            Expr::Not(expr) => {
                let expr = self.add_expr(expr)?;
                self.graph.add(LogicalPlanLanguage::NotExpr([expr]))
            }
            Expr::IsNotNull(expr) => {
                let expr = self.add_expr(expr)?;
                self.graph.add(LogicalPlanLanguage::IsNotNullExpr([expr]))
            }
            Expr::IsNull(expr) => {
                let expr = self.add_expr(expr)?;
                self.graph.add(LogicalPlanLanguage::IsNullExpr([expr]))
            }
            Expr::Negative(expr) => {
                let expr = self.add_expr(expr)?;
                self.graph.add(LogicalPlanLanguage::NegativeExpr([expr]))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.add_expr(expr)?;
                let negated = add_data_node!(self, negated, BetweenExprNegated);
                let low = self.add_expr(low)?;
                let high = self.add_expr(high)?;
                self.graph
                    .add(LogicalPlanLanguage::BetweenExpr([expr, negated, low, high]))
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let expr = add_expr_list_node!(self, expr, CaseExprExpr);
                let when_then_expr = when_then_expr
                    .iter()
                    .map(|(when, then)| vec![when, then])
                    .flatten()
                    .collect::<Vec<_>>();
                let when_then_expr =
                    add_expr_list_node!(self, when_then_expr, CaseExprWhenThenExpr);
                let else_expr = add_expr_list_node!(self, else_expr, CaseExprElseExpr);
                self.graph.add(LogicalPlanLanguage::CaseExpr([
                    expr,
                    when_then_expr,
                    else_expr,
                ]))
            }
            Expr::Cast { expr, data_type } => {
                let expr = self.add_expr(expr)?;
                let data_type = add_data_node!(self, data_type, CastExprDataType);
                self.graph
                    .add(LogicalPlanLanguage::CastExpr([expr, data_type]))
            }
            Expr::TryCast { expr, data_type } => {
                let expr = self.add_expr(expr)?;
                let data_type = add_data_node!(self, data_type, TryCastExprDataType);
                self.graph
                    .add(LogicalPlanLanguage::TryCastExpr([expr, data_type]))
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let expr = self.add_expr(expr)?;
                let asc = add_data_node!(self, asc, SortExprAsc);
                let nulls_first = add_data_node!(self, nulls_first, SortExprNullsFirst);
                self.graph
                    .add(LogicalPlanLanguage::SortExpr([expr, asc, nulls_first]))
            }
            Expr::ScalarFunction { fun, args } => {
                let fun = add_data_node!(self, fun, ScalarFunctionExprFun);
                let args = add_expr_list_node!(self, args, ScalarFunctionExprArgs);

                self.graph
                    .add(LogicalPlanLanguage::ScalarFunctionExpr([fun, args]))
            }
            Expr::ScalarUDF { fun, args } => {
                let fun = add_data_node!(self, fun.name, ScalarUDFExprFun);
                let args = add_expr_list_node!(self, args, ScalarUDFExprArgs);
                self.graph
                    .add(LogicalPlanLanguage::ScalarUDFExpr([fun, args]))
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                let fun = add_data_node!(self, fun, AggregateFunctionExprFun);
                let args = add_expr_list_node!(self, args, AggregateFunctionExprArgs);
                let distinct = add_data_node!(self, distinct, AggregateFunctionExprDistinct);
                self.graph.add(LogicalPlanLanguage::AggregateFunctionExpr([
                    fun, args, distinct,
                ]))
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let fun = add_data_node!(self, fun, WindowFunctionExprFun);
                let args = add_expr_list_node!(self, args, WindowFunctionExprArgs);
                let partition_by =
                    add_expr_list_node!(self, partition_by, WindowFunctionExprPartitionBy);
                let order_by = add_expr_list_node!(self, order_by, WindowFunctionExprOrderBy);
                let window_frame =
                    add_data_node!(self, window_frame, WindowFunctionExprWindowFrame);

                self.graph.add(LogicalPlanLanguage::WindowFunctionExpr([
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                ]))
            }
            Expr::AggregateUDF { fun, args } => {
                let fun = add_data_node!(self, fun.name, AggregateUDFExprFun);
                let args = add_expr_list_node!(self, args, AggregateUDFExprArgs);
                self.graph
                    .add(LogicalPlanLanguage::AggregateUDFExpr([fun, args]))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.add_expr(expr)?;
                let list = add_expr_list_node!(self, list, InListExprList);
                let negated = add_data_node!(self, negated, InListExprNegated);
                self.graph
                    .add(LogicalPlanLanguage::InListExpr([expr, list, negated]))
            }
            Expr::Wildcard => self.graph.add(LogicalPlanLanguage::WildcardExpr([])),
        })
    }

    pub fn add_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Id, CubeError> {
        Ok(match plan {
            LogicalPlan::Projection {
                expr,
                input,
                schema: _,
                alias,
            } => {
                let expr = add_expr_list_node!(self, expr, ProjectionExpr);
                let input = self.add_logical_plan(input.as_ref())?;
                let alias = add_data_node!(self, alias, ProjectionAlias);
                self.graph
                    .add(LogicalPlanLanguage::Projection([expr, input, alias]))
            }
            LogicalPlan::Filter { predicate, input } => {
                let predicate = self.add_expr(predicate)?;
                let input = self.add_logical_plan(input.as_ref())?;
                self.graph
                    .add(LogicalPlanLanguage::Filter([predicate, input]))
            }
            LogicalPlan::Window {
                input,
                window_expr,
                schema: _,
            } => {
                let input = self.add_logical_plan(input.as_ref())?;
                let window_expr = add_expr_list_node!(self, window_expr, WindowWindowExpr);
                self.graph
                    .add(LogicalPlanLanguage::Window([input, window_expr]))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema: _,
            } => {
                let input = self.add_logical_plan(input.as_ref())?;
                let group_expr = add_expr_list_node!(self, group_expr, AggregateGroupExpr);
                let aggr_expr = add_expr_list_node!(self, aggr_expr, AggregateAggrExpr);
                self.graph.add(LogicalPlanLanguage::Aggregate([
                    input, group_expr, aggr_expr,
                ]))
            }
            LogicalPlan::Sort { expr, input } => {
                let expr = add_expr_list_node!(self, expr, SortExp);
                let input = self.add_logical_plan(input.as_ref())?;
                self.graph.add(LogicalPlanLanguage::Sort([expr, input]))
            }
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                schema: _,
            } => {
                let left = self.add_logical_plan(left.as_ref())?;
                let right = self.add_logical_plan(right.as_ref())?;
                let left_on = on.iter().map(|(left, _)| left.clone()).collect::<Vec<_>>();
                let left_on = add_data_node!(self, left_on, JoinLeftOn);
                let right_on = on
                    .iter()
                    .map(|(_, right)| right.clone())
                    .collect::<Vec<_>>();
                let right_on = add_data_node!(self, right_on, JoinRightOn);
                let join_type = add_data_node!(self, join_type, JoinJoinType);
                let join_constraint = add_data_node!(self, join_constraint, JoinJoinConstraint);
                self.graph.add(LogicalPlanLanguage::Join([
                    left,
                    right,
                    left_on,
                    right_on,
                    join_type,
                    join_constraint,
                ]))
            }
            LogicalPlan::CrossJoin {
                left,
                right,
                schema: _,
            } => {
                let left = self.add_logical_plan(left.as_ref())?;
                let right = self.add_logical_plan(right.as_ref())?;
                self.graph
                    .add(LogicalPlanLanguage::CrossJoin([left, right]))
            }
            // TODO
            LogicalPlan::Repartition {
                input,
                partitioning_scheme: _,
            } => {
                let input = self.add_logical_plan(input.as_ref())?;
                self.graph.add(LogicalPlanLanguage::Repartition([input]))
            }
            LogicalPlan::Union {
                inputs,
                schema: _,
                alias,
            } => {
                let inputs = add_plan_list_node!(self, inputs, UnionInputs);
                let alias = add_data_node!(self, alias, UnionAlias);
                self.graph.add(LogicalPlanLanguage::Union([inputs, alias]))
            }
            LogicalPlan::TableScan {
                table_name,
                source,
                projection,
                projected_schema: _,
                filters,
                limit,
            } => {
                let source_table_name = add_data_node!(
                    self,
                    self.cube_context
                        .table_name_by_table_provider(source.clone())?,
                    TableScanSourceTableName
                );

                let table_name = add_data_node!(self, table_name, TableScanTableName);
                let projection = add_data_node!(self, projection, TableScanProjection);
                let filters = add_expr_list_node!(self, filters, TableScanFilters);
                let limit = add_data_node!(self, limit, TableScanLimit);
                self.graph.add(LogicalPlanLanguage::TableScan([
                    source_table_name,
                    table_name,
                    projection,
                    filters,
                    limit,
                ]))
            }
            LogicalPlan::EmptyRelation {
                produce_one_row,
                schema: _,
            } => {
                let produce_one_row =
                    add_data_node!(self, produce_one_row, EmptyRelationProduceOneRow);
                self.graph
                    .add(LogicalPlanLanguage::EmptyRelation([produce_one_row]))
            }
            LogicalPlan::Limit { n, input } => {
                let n = add_data_node!(self, n, LimitN);
                let input = self.add_logical_plan(input.as_ref())?;
                self.graph.add(LogicalPlanLanguage::Limit([n, input]))
            }
            LogicalPlan::CreateExternalTable { .. } => {
                panic!("CreateExternalTable is not supported");
            }
            LogicalPlan::Values { .. } => {
                panic!("Values is not supported");
            }
            LogicalPlan::Explain { .. } => {
                panic!("Explain is not supported");
            }
            LogicalPlan::Analyze { .. } => {
                panic!("Analyze is not supported");
            }
            // TODO
            LogicalPlan::Extension { node } => {
                if let Some(_cube_scan) = node.as_any().downcast_ref::<CubeScanNode>() {
                    todo!("LogicalPlanLanguage::Extension");
                    // self.graph.add(LogicalPlanLanguage::Extension([]))
                } else {
                    panic!("Unsupported extension node: {}", node.schema());
                }
            }
        })
    }

    pub fn rewrite_runner(&self) -> Runner<LogicalPlanLanguage, LogicalPlanAnalysis> {
        Runner::<LogicalPlanLanguage, LogicalPlanAnalysis>::new(LogicalPlanAnalysis {
            cube_context: self.cube_context.clone(),
        })
        .with_iter_limit(100)
        .with_node_limit(10000)
        .with_egraph(self.graph.clone())
    }

    pub fn find_best_plan(
        &mut self,
        root: Id,
        auth_context: Arc<AuthContext>,
    ) -> Result<LogicalPlan, CubeError> {
        let runner = self.rewrite_runner();
        let rules = self.rewrite_rules();
        let runner = runner.run(rules.iter());
        let extractor = Extractor::new(&runner.egraph, BestCubePlan);
        let (_, best) = extractor.find_best(root);
        let new_root = Id::from(best.as_ref().len() - 1);
        println!("Best: {:?}", best);
        self.graph = runner.egraph.clone();
        let converter = LanguageToLogicalPlanConverter {
            graph: runner.egraph,
            best_expr: best,
            cube_context: self.cube_context.clone(),
            auth_context,
        };
        converter.to_logical_plan(new_root)
    }

    pub fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            rewrite!("cube-scan";
                "(TableScan ?source_table_name ?table_name ?projection ?filters ?limit)" =>
                "(Extension (CubeScan ?source_table_name CubeScanMembers CubeScanFilters CubeScanOrder))"
                if self.is_cube_table("?source_table_name")
            ),
            rewrite(
                "member-replacer-aggr-tail",
                member_replacer(aggr_aggr_expr_empty_tail(), "?source_table_name"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-group-tail",
                member_replacer(aggr_group_expr_empty_tail(), "?source_table_name"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "dimension-replacer-tail-proj",
                member_replacer(projection_expr_empty_tail(), "?source_table_name"),
                cube_scan_members_empty_tail(),
            ),
            transforming_rewrite(
                "simple-count",
                member_replacer(
                    aggr_aggr_expr(
                        agg_fun_expr("?aggr_fun", vec![literal_expr("?literal")], "?distinct"),
                        "?tail_aggr_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    measure_expr(
                        "?measure_name",
                        agg_fun_expr("?aggr_fun", vec![literal_expr("?literal")], "?distinct"),
                    ),
                    member_replacer("?tail_aggr_expr", "?source_table_name"),
                ),
                self.transform_measure("?source_table_name", None, "?distinct", "?aggr_fun"),
            ),
            transforming_rewrite(
                "named-aggr",
                member_replacer(
                    aggr_aggr_expr(
                        agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                        "?tail_aggr_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    measure_expr(
                        "?measure_name",
                        agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                    ),
                    member_replacer("?tail_aggr_expr", "?source_table_name"),
                ),
                self.transform_measure(
                    "?source_table_name",
                    Some("?column"),
                    "?distinct",
                    "?aggr_fun",
                ),
            ),
            transforming_rewrite(
                "projection-columns-with-alias",
                member_replacer(
                    projection_expr(
                        alias_expr(column_expr("?column"), "?alias"),
                        "?tail_group_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    "?member",
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_projection_member(
                    "?source_table_name",
                    "?column",
                    Some("?alias"),
                    "?member",
                ),
            ),
            transforming_rewrite(
                "projection-columns",
                member_replacer(
                    projection_expr(column_expr("?column"), "?tail_group_expr"),
                    "?source_table_name",
                ),
                cube_scan_members(
                    "?member",
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_projection_member("?source_table_name", "?column", None, "?member"),
            ),
            transforming_rewrite(
                "date-trunc",
                member_replacer(
                    aggr_group_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?tail_group_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    time_dimension_expr(
                        "?time_dimension_name",
                        "?time_dimension_granularity",
                        "?date_range",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                    ),
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_time_dimension(
                    "?source_table_name",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                ),
            ),
            transforming_rewrite(
                "date-trunc-projection",
                member_replacer(
                    projection_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?tail_group_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    time_dimension_expr(
                        "?time_dimension_name",
                        "?time_dimension_granularity",
                        "?date_range",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                    ),
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_time_dimension(
                    "?source_table_name",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                ),
            ),
            transforming_rewrite(
                "time-dimension-alias",
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?original_expr",
                ),
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?alias",
                ),
                self.transform_original_expr_alias("?original_expr", "?alias"),
            ),
            transforming_rewrite(
                "measure-alias",
                measure_expr("?measure", "?original_expr"),
                measure_expr("?measure", "?alias"),
                self.transform_original_expr_alias("?original_expr", "?alias"),
            ),
            transforming_rewrite(
                "dimension-alias",
                dimension_expr("?dimension", "?original_expr"),
                dimension_expr("?dimension", "?alias"),
                self.transform_original_expr_alias("?original_expr", "?alias"),
            ),
            rewrite(
                "push-down-aggregate",
                aggregate(
                    cube_scan(
                        "?source_table_name",
                        cube_scan_members_empty_tail(),
                        "?filters",
                        "?orders",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                cube_scan(
                    "?source_table_name",
                    cube_scan_members(
                        member_replacer("?group_expr", "?source_table_name"),
                        member_replacer("?aggr_expr", "?source_table_name"),
                    ),
                    "?filters",
                    "?orders",
                ),
            ),
            rewrite(
                "push-down-projection-to-empty-scan",
                projection(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        cube_scan_members_empty_tail(),
                        "?filters",
                        "?orders",
                    ),
                    "?alias",
                ),
                cube_scan(
                    "?source_table_name",
                    member_replacer("?expr", "?source_table_name"),
                    "?filters",
                    "?orders",
                ),
            ),
            transforming_rewrite(
                "push-down-projection",
                projection(
                    "?expr",
                    cube_scan("?source_table_name", "?members", "?filters", "?orders"),
                    "?alias",
                ),
                cube_scan(
                    "?source_table_name",
                    column_alias_replacer("?members", "?aliases", "?cube"),
                    "?filters",
                    "?orders",
                ),
                self.push_down_projection(
                    "?source_table_name",
                    "?expr",
                    "?members",
                    "?aliases",
                    "?cube",
                ),
            ),
            rewrite(
                "alias-replacer-split",
                column_alias_replacer(
                    cube_scan_members("?members_left", "?members_right"),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    column_alias_replacer("?members_left", "?aliases", "?cube"),
                    column_alias_replacer("?members_right", "?aliases", "?cube"),
                ),
            ),
            transforming_rewrite(
                "alias-replacer",
                column_alias_replacer(
                    cube_scan_members(measure_expr("?measure", "?expr"), "?tail_group_expr"),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    measure_expr("?measure", "?replaced_alias_expr"),
                    column_alias_replacer("?tail_group_expr", "?aliases", "?cube"),
                ),
                self.replace_projection_alias("?expr", "?aliases", "?cube", "?replaced_alias_expr"),
            ),
            rewrite(
                "alias-replacer-tail",
                column_alias_replacer(cube_scan_members_empty_tail(), "?aliases", "?cube"),
                cube_scan_members_empty_tail(),
            ),
            transforming_rewrite(
                "push-down-sort",
                sort(
                    "?expr",
                    cube_scan("?source_table_name", "?members", "?filters", "?orders"),
                ),
                cube_scan(
                    "?source_table_name",
                    "?members",
                    "?filters",
                    order_replacer("?expr", "?aliases", "?cube"),
                ),
                self.push_down_sort(
                    "?source_table_name",
                    "?expr",
                    "?members",
                    "?aliases",
                    "?cube",
                ),
            ),
            transforming_rewrite(
                "order-replacer",
                order_replacer(
                    sort_exp(
                        sort_expr("?expr", "?asc", "?nulls_first"),
                        "?tail_group_expr",
                    ),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_order(
                    order("?order_member", "?order_asc"),
                    order_replacer("?tail_group_expr", "?aliases", "?cube"),
                ),
                self.transform_order(
                    "?expr",
                    "?asc",
                    "?aliases",
                    "?cube",
                    "?order_member",
                    "?order_asc",
                ),
            ),
            rewrite(
                "order-replacer-tail-proj",
                order_replacer(sort_exp_empty_tail(), "?aliases", "?cube"),
                cube_scan_order_empty_tail(),
            ),
            transforming_rewrite(
                "sort-expr-column-name",
                sort_expr("?expr", "?asc", "?nulls_first"),
                sort_expr("?alias", "?asc", "?nulls_first"),
                self.transform_original_expr_alias("?expr", "?alias"),
            ),
            rewrite!("date-to-date-trunc";
                "(ScalarUDFExpr \
                    ScalarUDFExprFun:date\
                    (ScalarUDFExprArgs (ColumnExpr ?column) ScalarUDFExprArgs)
                )" =>
                "(ScalarFunctionExpr \
                    DateTrunc \
                    (ScalarFunctionExprArgs \
                        (LiteralExpr LiteralExprValue:day) \
                        (ScalarFunctionExprArgs (ColumnExpr ?column) ScalarFunctionExprArgs) \
                    ) \
                )"
            ),
            rewrite(
                "binary-expr-addition-assoc",
                binary_expr(binary_expr("?a", "+", "?b"), "+", "?c"),
                binary_expr("?a", "+", binary_expr("?b", "+", "?c")),
            ),
            rewrite(
                "binary-expr-multi-assoc",
                binary_expr(binary_expr("?a", "*", "?b"), "*", "?c"),
                binary_expr("?a", "*", binary_expr("?b", "*", "?c")),
            ),
            // TODO ?interval ?one
            rewrite(
                "superset-quarter-to-date-trunc",
                binary_expr(
                    binary_expr(
                        udf_expr(
                            "makedate",
                            vec![
                                udf_expr("year", vec![column_expr("?column")]),
                                literal_expr("?one"),
                            ],
                        ),
                        "+",
                        fun_expr(
                            "ToMonthInterval",
                            vec![
                                udf_expr("quarter", vec![column_expr("?column")]),
                                literal_string("quarter"),
                            ],
                        ),
                    ),
                    "-",
                    literal_expr("?interval"),
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("quarter"), column_expr("?column")],
                ),
            ),
            // TODO ?one ?interval
            rewrite(
                "superset-week-to-date-trunc",
                udf_expr(
                    "date",
                    vec![udf_expr(
                        "date_sub",
                        vec![
                            column_expr("?column"),
                            to_day_interval_expr(
                                binary_expr(
                                    udf_expr(
                                        "dayofweek",
                                        vec![udf_expr(
                                            "date_sub",
                                            vec![column_expr("?column"), literal_expr("?interval")],
                                        )],
                                    ),
                                    "-",
                                    literal_expr("?one"),
                                ),
                                literal_string("day"),
                            ),
                        ],
                    )],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("week"), column_expr("?column")],
                ),
            ),
            // TODO ?one ?interval
            rewrite(
                "superset-month-to-date-trunc",
                udf_expr(
                    "date",
                    vec![udf_expr(
                        "date_sub",
                        vec![
                            column_expr("?column"),
                            to_day_interval_expr(
                                binary_expr(
                                    udf_expr("dayofmonth", vec![column_expr("?column")]),
                                    "-",
                                    literal_expr("?one"),
                                ),
                                literal_string("day"),
                            ),
                        ],
                    )],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("month"), column_expr("?column")],
                ),
            ),
            // TODO ?one ?interval
            rewrite(
                "superset-year-to-date-trunc",
                udf_expr(
                    "date",
                    vec![udf_expr(
                        "date_sub",
                        vec![
                            column_expr("?column"),
                            to_day_interval_expr(
                                binary_expr(
                                    udf_expr("dayofyear", vec![column_expr("?column")]),
                                    "-",
                                    literal_expr("?one"),
                                ),
                                literal_string("day"),
                            ),
                        ],
                    )],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("year"), column_expr("?column")],
                ),
            ),
            // TODO ?one ?interval
            rewrite(
                "superset-hour-to-date-trunc",
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date", vec![column_expr("?column")]),
                        to_day_interval_expr(
                            udf_expr("hour", vec![column_expr("?column")]),
                            literal_string("hour"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("hour"), column_expr("?column")],
                ),
            ),
            // TODO ?sixty
            rewrite(
                "superset-minute-to-date-trunc",
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date", vec![column_expr("?column")]),
                        to_day_interval_expr(
                            binary_expr(
                                binary_expr(
                                    udf_expr("hour", vec![column_expr("?column")]),
                                    "*",
                                    "?sixty",
                                ),
                                "+",
                                udf_expr("minute", vec![column_expr("?column")]),
                            ),
                            literal_string("minute"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("minute"), column_expr("?column")],
                ),
            ),
            // TODO ?sixty
            rewrite(
                "superset-second-to-date-trunc",
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date", vec![column_expr("?column")]),
                        to_day_interval_expr(
                            binary_expr(
                                binary_expr(
                                    binary_expr(
                                        udf_expr("hour", vec![column_expr("?column")]),
                                        "*",
                                        "?sixty",
                                    ),
                                    "*",
                                    "?sixty",
                                ),
                                "+",
                                binary_expr(
                                    binary_expr(
                                        udf_expr("minute", vec![column_expr("?column")]),
                                        "*",
                                        "?sixty",
                                    ),
                                    "+",
                                    udf_expr("second", vec![column_expr("?column")]),
                                ),
                            ),
                            literal_string("second"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("second"), column_expr("?column")],
                ),
            ),
        ]
    }

    fn is_cube_table(
        &self,
        var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &Subst) -> bool {
        let var = var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, _, subst| {
            for name in var_iter!(egraph[subst[var]], TableScanSourceTableName) {
                if meta_context
                    .cubes
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name))
                {
                    return true;
                }
            }
            false
        }
    }

    fn transform_original_expr_alias(
        &self,
        original_expr_var: &'static str,
        alias_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = original_expr_var.parse().unwrap();
        let alias_expr_var = alias_expr_var.parse().unwrap();
        move |egraph, subst| {
            let original_expr_id = subst[original_expr_var];
            if !egraph[original_expr_id]
                .nodes
                .iter()
                .any(|node| match node {
                    LogicalPlanLanguage::ColumnExpr(_) => true,
                    _ => false,
                })
            {
                let res = egraph[original_expr_id].data.original_expr.as_ref().ok_or(
                    CubeError::internal(format!(
                        "Original expr wasn't prepared for {:?}",
                        original_expr_id
                    )),
                );
                if let Ok(expr) = res {
                    // TODO unwrap
                    let name = expr.name(&DFSchema::empty()).unwrap();
                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                        ColumnExprColumn(Column::from_name(name)),
                    ));
                    subst.insert(
                        alias_expr_var,
                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn push_down_projection(
        &self,
        table_name_var: &'static str,
        projection_expr_var: &'static str,
        members_var: &'static str,
        aliases_var: &'static str,
        cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let table_name_var = table_name_var.parse().unwrap();
        let projection_expr_var = projection_expr_var.parse().unwrap();
        let members_var = members_var.parse().unwrap();
        let aliases_var = aliases_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], TableScanSourceTableName) {
                if let Some(expr_to_alias) =
                    &egraph.index(subst[projection_expr_var]).data.expr_to_alias
                {
                    let mut relation = WithColumnRelation(table_name.to_string());
                    let column_name_to_alias = expr_to_alias
                        .clone()
                        .into_iter()
                        .map(|(e, a)| (expr_column_name_with_relation(e, &mut relation), a))
                        .collect::<Vec<_>>();
                    println!("column_name_to_alias: {:?}", column_name_to_alias);
                    if let Some(member_name_to_expr) = egraph
                        .index(subst[members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member_name =
                            column_name_to_member_name(member_name_to_expr, table_name.to_string());
                        let table_name = table_name.to_string();
                        println!(
                            "column_name_to_member_name: {:?}",
                            column_name_to_member_name
                        );
                        if column_name_to_alias
                            .iter()
                            .all(|(c, _)| column_name_to_member_name.contains_key(c))
                        {
                            let aliases =
                                egraph.add(LogicalPlanLanguage::ColumnAliasReplacerAliases(
                                    ColumnAliasReplacerAliases(column_name_to_alias.clone()),
                                ));
                            subst.insert(aliases_var, aliases);

                            let cube = egraph.add(LogicalPlanLanguage::ColumnAliasReplacerCube(
                                ColumnAliasReplacerCube(Some(table_name)),
                            ));
                            subst.insert(cube_var, cube);
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn push_down_sort(
        &self,
        table_name_var: &'static str,
        sort_exp_var: &'static str,
        members_var: &'static str,
        aliases_var: &'static str,
        cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let table_name_var = table_name_var.parse().unwrap();
        let sort_exp_var = sort_exp_var.parse().unwrap();
        let members_var = members_var.parse().unwrap();
        let aliases_var = aliases_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], TableScanSourceTableName) {
                if let Some(referenced_expr) =
                    &egraph.index(subst[sort_exp_var]).data.referenced_expr
                {
                    println!("referenced_expr: {:?}", referenced_expr);
                    if let Some(member_name_to_expr) = egraph
                        .index(subst[members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member_name =
                            column_name_to_member_name(member_name_to_expr, table_name.to_string());
                        println!(
                            "column_name_to_member_name: {:?}",
                            column_name_to_member_name
                        );
                        let referenced_columns =
                            referenced_columns(referenced_expr.clone(), table_name.to_string());
                        println!("referenced_columns: {:?}", referenced_columns);
                        let table_name = table_name.to_string();
                        if referenced_columns
                            .iter()
                            .all(|c| column_name_to_member_name.contains_key(c))
                        {
                            subst.insert(
                                aliases_var,
                                egraph.add(LogicalPlanLanguage::OrderReplacerColumnNameToMember(
                                    OrderReplacerColumnNameToMember(
                                        column_name_to_member_name.into_iter().collect(),
                                    ),
                                )),
                            );

                            subst.insert(
                                cube_var,
                                egraph.add(LogicalPlanLanguage::OrderReplacerCube(
                                    OrderReplacerCube(Some(table_name)),
                                )),
                            );
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_order(
        &self,
        expr_var: &'static str,
        asc_var: &'static str,
        column_name_to_member_var: &'static str,
        cube_var: &'static str,
        order_member_var: &'static str,
        order_asc_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = expr_var.parse().unwrap();
        let asc_var = asc_var.parse().unwrap();
        let column_name_to_member_var = column_name_to_member_var.parse().unwrap();
        let order_member_var = order_member_var.parse().unwrap();
        let order_asc_var = order_asc_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        move |egraph, subst| {
            let expr = egraph[subst[expr_var]]
                .data
                .original_expr
                .as_ref()
                .expect(&format!(
                    "Original expr wasn't prepared for {:?}",
                    egraph[subst[expr_var]]
                ));
            for cube in var_iter!(egraph[subst[cube_var]], OrderReplacerCube) {
                let column_name = expr_column_name(expr.clone(), &cube);
                for asc in var_iter!(egraph[subst[asc_var]], SortExprAsc) {
                    let asc = *asc;
                    for column_name_to_member in var_iter!(
                        egraph[subst[column_name_to_member_var]],
                        OrderReplacerColumnNameToMember
                    ) {
                        if let Some((_, member_name)) = column_name_to_member
                            .iter()
                            .find(|(c, _)| c == &column_name)
                        {
                            subst.insert(
                                order_member_var,
                                egraph.add(LogicalPlanLanguage::OrderMember(OrderMember(
                                    member_name.to_string(),
                                ))),
                            );

                            subst.insert(
                                order_asc_var,
                                egraph.add(LogicalPlanLanguage::OrderAsc(OrderAsc(asc))),
                            );
                            return true;
                        }
                    }
                }
            }

            false
        }
    }

    fn replace_projection_alias(
        &self,
        expr_var: &'static str,
        column_name_to_alias: &'static str,
        cube_var: &'static str,
        replaced_alias_expr: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = expr_var.parse().unwrap();
        let column_name_to_alias = column_name_to_alias.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        let replaced_alias_expr = replaced_alias_expr.parse().unwrap();
        move |egraph, subst| {
            let expr = egraph[subst[expr_var]]
                .data
                .original_expr
                .as_ref()
                .expect(&format!(
                    "Original expr wasn't prepared for {:?}",
                    egraph[subst[expr_var]]
                ));
            for cube in var_iter!(egraph[subst[cube_var]], ColumnAliasReplacerCube) {
                let column_name = expr_column_name(expr.clone(), &cube);
                for column_name_to_alias in var_iter!(
                    egraph[subst[column_name_to_alias]],
                    ColumnAliasReplacerAliases
                ) {
                    if let Some((_, new_alias)) =
                        column_name_to_alias.iter().find(|(c, _)| c == &column_name)
                    {
                        let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                            ColumnExprColumn(Column::from_name(new_alias.to_string())),
                        ));
                        subst.insert(
                            replaced_alias_expr,
                            egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                        );
                        return true;
                    }
                }
            }

            false
        }
    }

    fn transform_projection_member(
        &self,
        cube_var: &'static str,
        column_var: &'static str,
        alias_var: Option<&'static str>,
        member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let column_var = column_var.parse().unwrap();
        let alias_var = alias_var.map(|alias_var| alias_var.parse().unwrap());
        let member_var = member_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for member_name in
                var_iter!(egraph[subst[column_var]], ColumnExprColumn).map(|c| c.name.to_string())
            {
                for cube_name in var_iter!(egraph[subst[cube_var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(cube_name))
                    {
                        let column_names = if let Some(alias_var) = &alias_var {
                            var_iter!(egraph[subst[*alias_var]], AliasExprAlias)
                                .map(|s| s.to_string())
                                .collect::<Vec<_>>()
                        } else {
                            vec![member_name.to_string()]
                        };
                        for column_name in column_names {
                            let member_name = format!("{}.{}", cube_name, member_name);
                            if let Some(dimension) = cube
                                .dimensions
                                .iter()
                                .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                            {
                                let dimension_name =
                                    egraph.add(LogicalPlanLanguage::DimensionName(DimensionName(
                                        dimension.name.to_string(),
                                    )));
                                let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(Column::from_name(column_name)),
                                ));
                                let alias_expr =
                                    egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));

                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::Dimension([
                                        dimension_name,
                                        alias_expr,
                                    ])),
                                );
                                return true;
                            }

                            if let Some(measure) = cube
                                .measures
                                .iter()
                                .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                            {
                                let measure_name = egraph.add(LogicalPlanLanguage::MeasureName(
                                    MeasureName(measure.name.to_string()),
                                ));
                                let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(Column::from_name(column_name)),
                                ));
                                let alias_expr =
                                    egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));
                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::Measure([
                                        measure_name,
                                        alias_expr,
                                    ])),
                                );
                                return true;
                            }
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_time_dimension(
        &self,
        cube_var: &'static str,
        dimension_var: &'static str,
        time_dimension_name_var: &'static str,
        granularity_var: &'static str,
        time_dimension_granularity_var: &'static str,
        date_range_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let dimension_var = dimension_var.parse().unwrap();
        let time_dimension_name_var = time_dimension_name_var.parse().unwrap();
        let granularity_var = granularity_var.parse().unwrap();
        let time_dimension_granularity_var = time_dimension_granularity_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for time_dimension_name in var_iter!(egraph[subst[dimension_var]], ColumnExprColumn)
                .map(|c| c.name.to_string())
            {
                for cube_name in var_iter!(egraph[subst[cube_var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(cube_name))
                    {
                        let time_dimension_name = format!("{}.{}", cube_name, time_dimension_name);
                        if let Some(time_dimension) = cube.dimensions.iter().find(|d| {
                            d._type == "time" && d.name.eq_ignore_ascii_case(&time_dimension_name)
                        }) {
                            for granularity in
                                var_iter!(egraph[subst[granularity_var]], LiteralExprValue)
                            {
                                match granularity {
                                    ScalarValue::Utf8(Some(granularity_value)) => {
                                        let granularity_value = granularity_value.to_string();
                                        subst.insert(
                                            time_dimension_name_var,
                                            egraph.add(LogicalPlanLanguage::TimeDimensionName(
                                                TimeDimensionName(time_dimension.name.to_string()),
                                            )),
                                        );
                                        subst.insert(
                                            date_range_var,
                                            egraph.add(
                                                LogicalPlanLanguage::TimeDimensionDateRange(
                                                    TimeDimensionDateRange(None), // TODO
                                                ),
                                            ),
                                        );
                                        subst.insert(
                                            time_dimension_granularity_var,
                                            egraph.add(
                                                LogicalPlanLanguage::TimeDimensionGranularity(
                                                    TimeDimensionGranularity(Some(
                                                        granularity_value,
                                                    )),
                                                ),
                                            ),
                                        );
                                        return true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_measure(
        &self,
        cube_var: &'static str,
        measure_var: Option<&'static str>,
        distinct_var: &'static str,
        fun_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let var = cube_var.parse().unwrap();
        let distinct_var = distinct_var.parse().unwrap();
        let fun_var = fun_var.parse().unwrap();
        let measure_var = measure_var.map(|var| var.parse().unwrap());
        let measure_name_var = "?measure_name".parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for measure_name in measure_var
                .map(|measure_var| {
                    var_iter!(egraph[subst[measure_var]], ColumnExprColumn)
                        .map(|c| c.name.to_string())
                        .collect()
                })
                .unwrap_or(vec!["count".to_string()])
            {
                for cube_name in var_iter!(egraph[subst[var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(cube_name))
                    {
                        for distinct in
                            var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                        {
                            for fun in var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun) {
                                let measure_name = format!("{}.{}", cube_name, measure_name);
                                if let Some(measure) = cube.measures.iter().find(|m| {
                                    measure_name.eq_ignore_ascii_case(&m.name) && {
                                        if let Some(agg_type) = &m.agg_type {
                                            match fun {
                                                AggregateFunction::Count => {
                                                    if *distinct {
                                                        agg_type == "countDistinct"
                                                            || agg_type == "countDistinctApprox"
                                                    } else {
                                                        agg_type == "count"
                                                    }
                                                }
                                                AggregateFunction::Sum => agg_type == "sum",
                                                AggregateFunction::Min => agg_type == "min",
                                                AggregateFunction::Max => agg_type == "max",
                                                AggregateFunction::Avg => agg_type == "avg",
                                                AggregateFunction::ApproxDistinct => {
                                                    agg_type == "countDistinctApprox"
                                                }
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                }) {
                                    subst.insert(
                                        measure_name_var,
                                        egraph.add(LogicalPlanLanguage::MeasureName(MeasureName(
                                            measure.name.to_string(),
                                        ))),
                                    );
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }
}

pub struct WithColumnRelation(String);

impl ExprRewriter for WithColumnRelation {
    fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
        match expr {
            Expr::Column(c) => Ok(Expr::Column(Column {
                name: c.name.to_string(),
                relation: c.relation.or_else(|| Some(self.0.to_string())),
            })),
            e => Ok(e),
        }
    }
}

fn column_name_to_member_name(
    member_name_to_expr: Vec<(String, Expr)>,
    table_name: String,
) -> HashMap<String, String> {
    let mut relation = WithColumnRelation(table_name);
    member_name_to_expr
        .into_iter()
        .map(|(member, expr)| (expr_column_name_with_relation(expr, &mut relation), member))
        .collect::<HashMap<_, _>>()
}

fn referenced_columns(referenced_expr: Vec<Expr>, table_name: String) -> Vec<String> {
    let mut relation = WithColumnRelation(table_name);
    referenced_expr
        .into_iter()
        .map(|expr| expr_column_name_with_relation(expr, &mut relation))
        .collect::<Vec<_>>()
}

fn expr_column_name_with_relation(expr: Expr, relation: &mut WithColumnRelation) -> String {
    expr.rewrite(relation)
        .unwrap()
        .name(&DFSchema::empty())
        .unwrap()
}

fn expr_column_name(expr: Expr, cube: &Option<String>) -> String {
    if let Some(cube) = cube.as_ref() {
        expr_column_name_with_relation(expr, &mut WithColumnRelation(cube.to_string()))
    } else {
        expr.name(&DFSchema::empty()).unwrap()
    }
}

fn rewrite(
    name: &str,
    searcher: String,
    applier: String,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    Rewrite::new(
        name.to_string(),
        searcher.parse::<Pattern<LogicalPlanLanguage>>().unwrap(),
        applier.parse::<Pattern<LogicalPlanLanguage>>().unwrap(),
    )
    .unwrap()
}

fn transforming_rewrite<T>(
    name: &str,
    searcher: String,
    applier: String,
    transform_fn: T,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
        + Sync
        + Send
        + 'static,
{
    Rewrite::new(
        name.to_string(),
        searcher.parse::<Pattern<LogicalPlanLanguage>>().unwrap(),
        TransformingPattern::new(applier.as_str(), transform_fn),
    )
    .unwrap()
}

fn list_expr(list_type: impl Display, list: Vec<impl Display>) -> String {
    let mut current = list_type.to_string();
    for i in list.into_iter().rev() {
        current = format!("({} {} {})", list_type, i, current);
    }
    current
}

fn udf_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    format!(
        "(ScalarUDFExpr ScalarUDFExprFun:{} {})",
        fun_name,
        list_expr("ScalarUDFExprArgs", args)
    )
}

fn fun_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    format!(
        "(ScalarFunctionExpr {} {})",
        fun_name,
        list_expr("ScalarFunctionExprArgs", args)
    )
}

fn agg_fun_expr(fun_name: impl Display, args: Vec<impl Display>, distinct: impl Display) -> String {
    format!(
        "(AggregateFunctionExpr {} {} {})",
        fun_name,
        list_expr("AggregateFunctionExprArgs", args),
        distinct
    )
}

fn aggregate(input: impl Display, group: impl Display, aggr: impl Display) -> String {
    format!("(Aggregate {} {} {})", input, group, aggr)
}

fn aggr_aggr_expr(left: impl Display, right: impl Display) -> String {
    format!("(AggregateAggrExpr {} {})", left, right)
}

fn aggr_aggr_expr_empty_tail() -> String {
    format!("AggregateAggrExpr")
}

fn aggr_group_expr(left: impl Display, right: impl Display) -> String {
    format!("(AggregateGroupExpr {} {})", left, right)
}

fn projection_expr(left: impl Display, right: impl Display) -> String {
    format!("(ProjectionExpr {} {})", left, right)
}

fn sort_exp(left: impl Display, right: impl Display) -> String {
    format!("(SortExp {} {})", left, right)
}

fn sort_exp_empty_tail() -> String {
    format!("SortExp")
}

fn sort_expr(expr: impl Display, asc: impl Display, nulls_first: impl Display) -> String {
    format!("(SortExpr {} {} {})", expr, asc, nulls_first)
}

fn aggr_group_expr_empty_tail() -> String {
    format!("AggregateGroupExpr")
}

fn projection_expr_empty_tail() -> String {
    format!("ProjectionExpr")
}

fn to_day_interval_expr<D: Display>(period: D, unit: D) -> String {
    fun_expr("ToDayInterval", vec![period, unit])
}

fn binary_expr(left: impl Display, op: impl Display, right: impl Display) -> String {
    format!("(BinaryExpr {} {} {})", left, op, right)
}

fn literal_expr(literal: impl Display) -> String {
    format!("(LiteralExpr {})", literal)
}

fn column_expr(column: impl Display) -> String {
    format!("(ColumnExpr {})", column)
}

fn alias_expr(column: impl Display, alias: impl Display) -> String {
    format!("(AliasExpr {} {})", column, alias)
}

fn literal_string(literal_str: impl Display) -> String {
    format!("(LiteralExpr LiteralExprValue:{})", literal_str)
}

fn projection(expr: impl Display, input: impl Display, alias: impl Display) -> String {
    format!("(Projection {} {} {})", expr, input, alias)
}

fn sort(expr: impl Display, input: impl Display) -> String {
    format!("(Sort {} {})", expr, input)
}

fn column_alias_replacer(
    members: impl Display,
    aliases: impl Display,
    cube: impl Display,
) -> String {
    format!("(ColumnAliasReplacer {} {} {})", members, aliases, cube)
}

fn member_replacer(members: impl Display, aliases: impl Display) -> String {
    format!("(MemberReplacer {} {})", members, aliases)
}

fn order_replacer(members: impl Display, aliases: impl Display, cube: impl Display) -> String {
    format!("(OrderReplacer {} {} {})", members, aliases, cube)
}

fn cube_scan_members(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanMembers {} {})", left, right)
}

fn cube_scan_members_empty_tail() -> String {
    format!("CubeScanMembers")
}

fn cube_scan_order(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanOrder {} {})", left, right)
}

fn cube_scan_order_empty_tail() -> String {
    format!("CubeScanOrder")
}

fn order(member: impl Display, asc: impl Display) -> String {
    format!("(Order {} {})", member, asc)
}

fn measure_expr(measure_name: impl Display, expr: impl Display) -> String {
    format!("(Measure {} {})", measure_name, expr)
}

fn dimension_expr(name: impl Display, expr: impl Display) -> String {
    format!("(Dimension {} {})", name, expr)
}

fn time_dimension_expr(
    name: impl Display,
    granularity: impl Display,
    date_range: impl Display,
    expr: impl Display,
) -> String {
    format!(
        "(TimeDimension {} {} {} {})",
        name, granularity, date_range, expr
    )
}

fn cube_scan(
    source_table_name: impl Display,
    members: impl Display,
    filters: impl Display,
    orders: impl Display,
) -> String {
    format!(
        "(Extension (CubeScan {} {} {} {}))",
        source_table_name, members, filters, orders
    )
}

#[derive(Clone, Debug)]
pub struct LogicalPlanData {
    original_expr: Option<Expr>,
    member_name_to_expr: Option<Vec<(String, Expr)>>,
    column_name: Option<String>,
    expr_to_alias: Option<Vec<(Expr, String)>>,
    referenced_expr: Option<Vec<Expr>>,
}

#[derive(Clone)]
pub struct LogicalPlanAnalysis {
    cube_context: CubeContext,
}

pub struct SingleNodeIndex<'a> {
    egraph: &'a EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
}

impl<'a> Index<Id> for SingleNodeIndex<'a> {
    type Output = LogicalPlanLanguage;

    fn index(&self, index: Id) -> &Self::Output {
        assert!(
            self.egraph.index(index).nodes.len() == 1,
            "Single node expected but {:?} found",
            self.egraph.index(index).nodes
        );
        &self.egraph.index(index).nodes[0]
    }
}

impl LogicalPlanAnalysis {
    fn make_original_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Expr> {
        let id_to_expr = |id| {
            egraph[id]
                .data
                .original_expr
                .clone()
                .ok_or(CubeError::internal(
                    "Original expr wasn't prepared".to_string(),
                ))
        };
        let original_expr = if is_expr_node(enode) {
            // TODO .unwrap
            Some(
                node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &id_to_expr,
                    &SingleNodeIndex { egraph },
                )
                .unwrap(),
            )
        } else {
            None
        };
        original_expr
    }

    fn make_member_name_to_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<(String, Expr)>> {
        let column_name = |id| egraph.index(id).data.column_name.clone();
        let id_to_column_name_to_expr = |id| egraph.index(id).data.member_name_to_expr.clone();
        let original_expr = |id| egraph.index(id).data.original_expr.clone();
        let mut map = Vec::new();
        match enode {
            LogicalPlanLanguage::Measure(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let measure_name = var_iter!(egraph[params[0]], MeasureName).next().unwrap();
                    map.push((measure_name.to_string(), expr));
                    println!("Measure: {:?}", map);
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::Dimension(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let dimension_name =
                        var_iter!(egraph[params[0]], DimensionName).next().unwrap();
                    map.push((dimension_name.to_string(), expr));
                    println!("Dimension: {:?}", map);
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::TimeDimension(params) => {
                if let Some(_) = column_name(params[3]) {
                    let expr = original_expr(params[3])?;
                    let time_dimension_name = var_iter!(egraph[params[0]], TimeDimensionName)
                        .next()
                        .unwrap();
                    map.push((time_dimension_name.to_string(), expr));
                    println!("TimeDimension: {:?}", map);
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::CubeScanMembers(params) => {
                for id in params.iter() {
                    map.extend(id_to_column_name_to_expr(*id)?.into_iter());
                }
                println!("CubeScanMembers: {:?}", map);
                Some(map)
            }
            LogicalPlanLanguage::CubeScan(params) => {
                map.extend(id_to_column_name_to_expr(params[1])?.into_iter());
                println!("CubeScan: {:?}", map);
                Some(map)
            }
            _ => None,
        }
    }

    fn make_expr_to_alias(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<(Expr, String)>> {
        let original_expr = |id| egraph.index(id).data.original_expr.clone();
        let id_to_column_name = |id| egraph.index(id).data.column_name.clone();
        let column_name_to_alias = |id| egraph.index(id).data.expr_to_alias.clone();
        let mut map = Vec::new();
        match enode {
            LogicalPlanLanguage::AliasExpr(params) => {
                map.push((original_expr(params[0])?, id_to_column_name(params[1])?));
                Some(map)
            }
            LogicalPlanLanguage::ProjectionExpr(params) => {
                for id in params.iter() {
                    map.extend(column_name_to_alias(*id)?.into_iter());
                }
                Some(map)
            }
            _ => None,
        }
    }

    fn make_referenced_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<Expr>> {
        let referenced_columns = |id| egraph.index(id).data.referenced_expr.clone();
        let original_expr = |id| egraph.index(id).data.original_expr.clone();
        let column_name = |id| egraph.index(id).data.column_name.clone();
        let mut vec = Vec::new();
        match enode {
            LogicalPlanLanguage::SortExpr(params) => {
                if column_name(params[0]).is_some() {
                    let expr = original_expr(params[0])?;
                    vec.push(expr);
                    Some(vec)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::SortExp(params) => {
                for p in params.iter() {
                    vec.extend(referenced_columns(*p)?.into_iter());
                }

                Some(vec)
            }
            _ => None,
        }
    }

    fn make_column_name(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<String> {
        let id_to_column_name = |id| egraph.index(id).data.column_name.clone();
        match enode {
            LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(c)) => Some(c.name.to_string()),
            LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(a)) => Some(a.to_string()),
            LogicalPlanLanguage::ColumnExpr(c) => id_to_column_name(c[0]),
            _ => None,
        }
    }

    fn merge_option_field<T: Clone>(
        &mut self,
        a: &mut LogicalPlanData,
        mut b: LogicalPlanData,
        field: fn(&mut LogicalPlanData) -> &mut Option<T>,
    ) -> (DidMerge, LogicalPlanData) {
        let res = if field(a).is_none() && field(&mut b).is_some() {
            *field(a) = field(&mut b).clone();
            DidMerge(true, false)
        } else if field(a).is_some() {
            DidMerge(false, true)
        } else {
            DidMerge(false, false)
        };
        (res, b)
    }
}

impl Analysis<LogicalPlanLanguage> for LogicalPlanAnalysis {
    type Data = LogicalPlanData;

    fn make(egraph: &EGraph<LogicalPlanLanguage, Self>, enode: &LogicalPlanLanguage) -> Self::Data {
        LogicalPlanData {
            original_expr: Self::make_original_expr(egraph, enode),
            member_name_to_expr: Self::make_member_name_to_expr(egraph, enode),
            column_name: Self::make_column_name(egraph, enode),
            expr_to_alias: Self::make_expr_to_alias(egraph, enode),
            referenced_expr: Self::make_referenced_expr(egraph, enode),
        }
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        let (original_expr, b) = self.merge_option_field(a, b, |d| &mut d.original_expr);
        let (member_name_to_expr, b) =
            self.merge_option_field(a, b, |d| &mut d.member_name_to_expr);
        let (column_name_to_alias, b) = self.merge_option_field(a, b, |d| &mut d.expr_to_alias);
        let (referenced_columns, b) = self.merge_option_field(a, b, |d| &mut d.referenced_expr);
        let (column_name, _) = self.merge_option_field(a, b, |d| &mut d.column_name);
        original_expr
            | member_name_to_expr
            | column_name_to_alias
            | referenced_columns
            | column_name
    }
}

pub struct TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
{
    pattern: Pattern<LogicalPlanLanguage>,
    vars_to_substitute: T,
}

impl<T> TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
{
    pub fn new(pattern: &str, vars_to_substitute: T) -> Self {
        Self {
            pattern: pattern.parse().unwrap(),
            vars_to_substitute,
        }
    }
}

impl<T> Applier<LogicalPlanLanguage, LogicalPlanAnalysis> for TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
{
    fn apply_one(
        &self,
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<LogicalPlanLanguage>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let mut new_subst = subst.clone();
        if (self.vars_to_substitute)(egraph, &mut new_subst) {
            self.pattern
                .apply_one(egraph, eclass, &new_subst, searcher_ast, rule_name)
        } else {
            Vec::new()
        }
    }
}

pub struct BestCubePlan;

impl CostFunction<LogicalPlanLanguage> for BestCubePlan {
    type Cost = (/* Cube nodes */ i64, /* AST size */ usize);
    fn cost<C>(&mut self, enode: &LogicalPlanLanguage, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let this_cube_nodes = match enode {
            LogicalPlanLanguage::CubeScan(_) => -1,
            LogicalPlanLanguage::Measure(_) => -1,
            LogicalPlanLanguage::Dimension(_) => -1,
            LogicalPlanLanguage::TimeDimension(_) => -1,
            _ => 0,
        };
        enode
            .children()
            .iter()
            .fold((this_cube_nodes, 1), |(cube_nodes, nodes), id| {
                let (child_cube_nodes, child_nodes) = costs(*id);
                (cube_nodes + child_cube_nodes, nodes + child_nodes)
            })
    }
}

macro_rules! match_params {
    ($id_expr:expr, $field_variant:ident) => {
        match $id_expr {
            LogicalPlanLanguage::$field_variant(params) => params,
            x => panic!(
                "Expected {} but found {:?}",
                std::stringify!($field_variant),
                x
            ),
        }
    };
}

macro_rules! match_data_node {
    ($node_by_id:expr, $id_expr:expr, $field_variant:ident) => {
        match $node_by_id.index($id_expr.clone()) {
            LogicalPlanLanguage::$field_variant($field_variant(data)) => data.clone(),
            x => panic!(
                "Expected {} but found {:?}",
                std::stringify!($field_variant),
                x
            ),
        }
    };
}

macro_rules! match_list_node {
    ($node_by_id:expr, $id_expr:expr, $field_variant:ident) => {{
        fn match_list(
            node_by_id: &impl Index<Id, Output = LogicalPlanLanguage>,
            id: Id,
            result: &mut Vec<LogicalPlanLanguage>,
        ) -> Result<(), CubeError> {
            match node_by_id.index(id) {
                LogicalPlanLanguage::$field_variant(list) => {
                    for i in list {
                        match_list(node_by_id, *i, result)?;
                    }
                }
                x => {
                    result.push(x.clone());
                }
            }
            Ok(())
        }
        let mut result = Vec::new();
        match_list($node_by_id, $id_expr.clone(), &mut result)?;
        result
    }};
}

macro_rules! match_expr_list_node {
    ($node_by_id:expr, $to_expr:expr, $id_expr:expr, $field_variant:ident) => {{
        fn match_expr_list(
            node_by_id: &impl Index<Id, Output = LogicalPlanLanguage>,
            to_expr: &impl Fn(Id) -> Result<Expr, CubeError>,
            id: Id,
            result: &mut Vec<Expr>,
        ) -> Result<(), CubeError> {
            match node_by_id.index(id) {
                LogicalPlanLanguage::$field_variant(list) => {
                    for i in list {
                        match_expr_list(node_by_id, to_expr, *i, result)?;
                    }
                }
                _ => {
                    result.push(to_expr(id.clone())?);
                }
            }
            Ok(())
        }
        let mut result = Vec::new();
        match_expr_list($node_by_id, $to_expr, $id_expr.clone(), &mut result)?;
        result
    }};
}

pub struct LanguageToLogicalPlanConverter {
    graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    best_expr: RecExpr<LogicalPlanLanguage>,
    cube_context: CubeContext,
    auth_context: Arc<AuthContext>,
}

pub fn is_expr_node(node: &LogicalPlanLanguage) -> bool {
    match node {
        LogicalPlanLanguage::AliasExpr(_) => true,
        LogicalPlanLanguage::ColumnExpr(_) => true,
        LogicalPlanLanguage::ScalarVariableExpr(_) => true,
        LogicalPlanLanguage::LiteralExpr(_) => true,
        LogicalPlanLanguage::BinaryExpr(_) => true,
        LogicalPlanLanguage::NotExpr(_) => true,
        LogicalPlanLanguage::IsNotNullExpr(_) => true,
        LogicalPlanLanguage::IsNullExpr(_) => true,
        LogicalPlanLanguage::NegativeExpr(_) => true,
        LogicalPlanLanguage::BetweenExpr(_) => true,
        LogicalPlanLanguage::CaseExpr(_) => true,
        LogicalPlanLanguage::CastExpr(_) => true,
        LogicalPlanLanguage::TryCastExpr(_) => true,
        LogicalPlanLanguage::SortExpr(_) => true,
        LogicalPlanLanguage::ScalarFunctionExpr(_) => true,
        LogicalPlanLanguage::ScalarUDFExpr(_) => true,
        LogicalPlanLanguage::AggregateFunctionExpr(_) => true,
        LogicalPlanLanguage::WindowFunctionExpr(_) => true,
        LogicalPlanLanguage::AggregateUDFExpr(_) => true,
        LogicalPlanLanguage::InListExpr(_) => true,
        LogicalPlanLanguage::WildcardExpr(_) => true,
        _ => false,
    }
}

pub fn node_to_expr(
    node: &LogicalPlanLanguage,
    cube_context: &CubeContext,
    to_expr: &impl Fn(Id) -> Result<Expr, CubeError>,
    node_by_id: &impl Index<Id, Output = LogicalPlanLanguage>,
) -> Result<Expr, CubeError> {
    Ok(match node {
        LogicalPlanLanguage::AliasExpr(params) => {
            let expr = to_expr(params[0].clone())?;
            let alias = match_data_node!(node_by_id, params[1], AliasExprAlias);
            Expr::Alias(Box::new(expr), alias)
        }
        LogicalPlanLanguage::ColumnExpr(params) => {
            let column = match_data_node!(node_by_id, params[0], ColumnExprColumn);
            Expr::Column(column)
        }
        LogicalPlanLanguage::ScalarVariableExpr(params) => {
            let variable = match_data_node!(node_by_id, params[0], ScalarVariableExprVariable);
            Expr::ScalarVariable(variable)
        }
        LogicalPlanLanguage::LiteralExpr(params) => {
            let value = match_data_node!(node_by_id, params[0], LiteralExprValue);
            Expr::Literal(value)
        }
        LogicalPlanLanguage::BinaryExpr(params) => {
            let left = Box::new(to_expr(params[0].clone())?);
            let op = match_data_node!(node_by_id, params[1], BinaryExprOp);
            let right = Box::new(to_expr(params[2].clone())?);
            Expr::BinaryExpr { left, op, right }
        }
        LogicalPlanLanguage::NotExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            Expr::Not(expr)
        }
        LogicalPlanLanguage::IsNotNullExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            Expr::IsNotNull(expr)
        }
        LogicalPlanLanguage::IsNullExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            Expr::IsNull(expr)
        }
        LogicalPlanLanguage::NegativeExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            Expr::Negative(expr)
        }
        LogicalPlanLanguage::BetweenExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            let negated = match_data_node!(node_by_id, params[1], BetweenExprNegated);
            let low = Box::new(to_expr(params[2].clone())?);
            let high = Box::new(to_expr(params[3].clone())?);
            Expr::Between {
                expr,
                negated,
                low,
                high,
            }
        }
        LogicalPlanLanguage::CaseExpr(params) => {
            let expr = match_expr_list_node!(node_by_id, to_expr, params[0], CaseExprExpr);
            let when_then_expr =
                match_expr_list_node!(node_by_id, to_expr, params[1], CaseExprWhenThenExpr);
            let else_expr = match_expr_list_node!(node_by_id, to_expr, params[2], CaseExprElseExpr);
            Expr::Case {
                expr: expr.into_iter().next().map(|e| Box::new(e)),
                when_then_expr: when_then_expr
                    .into_iter()
                    .chunks(2)
                    .into_iter()
                    .map(|mut chunk| {
                        (
                            Box::new(chunk.next().unwrap()),
                            Box::new(chunk.next().unwrap()),
                        )
                    })
                    .collect::<Vec<_>>(),
                else_expr: else_expr.into_iter().next().map(|e| Box::new(e)),
            }
        }
        LogicalPlanLanguage::CastExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            let data_type = match_data_node!(node_by_id, params[1], CastExprDataType);
            Expr::Cast { expr, data_type }
        }
        LogicalPlanLanguage::TryCastExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            let data_type = match_data_node!(node_by_id, params[1], TryCastExprDataType);
            Expr::TryCast { expr, data_type }
        }
        LogicalPlanLanguage::SortExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            let asc = match_data_node!(node_by_id, params[1], SortExprAsc);
            let nulls_first = match_data_node!(node_by_id, params[2], SortExprNullsFirst);
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            }
        }
        LogicalPlanLanguage::ScalarFunctionExpr(params) => {
            let fun = match_data_node!(node_by_id, params[0], ScalarFunctionExprFun);
            let args =
                match_expr_list_node!(node_by_id, to_expr, params[1], ScalarFunctionExprArgs);
            Expr::ScalarFunction { fun, args }
        }
        LogicalPlanLanguage::ScalarUDFExpr(params) => {
            let fun_name = match_data_node!(node_by_id, params[0], ScalarUDFExprFun);
            let args = match_expr_list_node!(node_by_id, to_expr, params[1], ScalarUDFExprArgs);
            let fun = cube_context
                .get_function_meta(&fun_name)
                .ok_or(CubeError::user(format!(
                    "Scalar UDF '{}' is not found",
                    fun_name
                )))?;
            Expr::ScalarUDF { fun, args }
        }
        LogicalPlanLanguage::AggregateFunctionExpr(params) => {
            let fun = match_data_node!(node_by_id, params[0], AggregateFunctionExprFun);
            let args =
                match_expr_list_node!(node_by_id, to_expr, params[1], AggregateFunctionExprArgs);
            let distinct = match_data_node!(node_by_id, params[2], AggregateFunctionExprDistinct);
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            }
        }
        LogicalPlanLanguage::WindowFunctionExpr(params) => {
            let fun = match_data_node!(node_by_id, params[0], WindowFunctionExprFun);
            let args =
                match_expr_list_node!(node_by_id, to_expr, params[1], WindowFunctionExprArgs);
            let partition_by = match_expr_list_node!(
                node_by_id,
                to_expr,
                params[2],
                WindowFunctionExprPartitionBy
            );
            let order_by =
                match_expr_list_node!(node_by_id, to_expr, params[3], WindowFunctionExprOrderBy);
            let window_frame =
                match_data_node!(node_by_id, params[4], WindowFunctionExprWindowFrame);
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }
        }
        LogicalPlanLanguage::AggregateUDFExpr(params) => {
            let fun_name = match_data_node!(node_by_id, params[0], AggregateUDFExprFun);
            let args = match_expr_list_node!(node_by_id, to_expr, params[1], AggregateUDFExprArgs);
            let fun = cube_context
                .get_aggregate_meta(&fun_name)
                .ok_or(CubeError::user(format!(
                    "Aggregate UDF '{}' is not found",
                    fun_name
                )))?;
            Expr::AggregateUDF { fun, args }
        }
        LogicalPlanLanguage::InListExpr(params) => {
            let expr = Box::new(to_expr(params[0].clone())?);
            let list = match_expr_list_node!(node_by_id, to_expr, params[1], InListExprList);
            let negated = match_data_node!(node_by_id, params[2], InListExprNegated);
            Expr::InList {
                expr,
                list,
                negated,
            }
        }
        LogicalPlanLanguage::WildcardExpr(_) => Expr::Wildcard,
        x => panic!("Unexpected expression node: {:?}", x),
    })
}

impl LanguageToLogicalPlanConverter {
    pub fn to_expr(&self, id: Id) -> Result<Expr, CubeError> {
        let node = self.best_expr.index(id);
        let to_expr = |id| self.to_expr(id);
        node_to_expr(node, &self.cube_context, &to_expr, &self.best_expr)
    }

    pub fn to_logical_plan(&self, id: Id) -> Result<LogicalPlan, CubeError> {
        let node_by_id = &self.best_expr;
        let node = node_by_id.index(id);
        let to_expr = &|id| self.to_expr(id);
        Ok(match node {
            LogicalPlanLanguage::Projection(params) => {
                let expr = match_expr_list_node!(node_by_id, to_expr, params[0], ProjectionExpr);
                let input = Arc::new(self.to_logical_plan(params[1])?);
                let alias = match_data_node!(node_by_id, params[2], ProjectionAlias);
                let input_schema = DFSchema::new(exprlist_to_fields(&expr, input.schema())?)?;
                let schema = match alias {
                    Some(ref alias) => input_schema.replace_qualifier(alias.as_str()),
                    None => input_schema,
                };
                LogicalPlan::Projection {
                    expr,
                    input,
                    alias,
                    schema: DFSchemaRef::new(schema),
                }
            }
            LogicalPlanLanguage::Filter(params) => {
                let predicate = self.to_expr(params[0])?;
                let input = Arc::new(self.to_logical_plan(params[1])?);
                LogicalPlan::Filter { predicate, input }
            }
            LogicalPlanLanguage::Window(params) => {
                let input = Arc::new(self.to_logical_plan(params[0])?);
                let window_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[1], WindowWindowExpr);
                let mut window_fields: Vec<DFField> =
                    exprlist_to_fields(window_expr.iter(), input.schema())?;
                window_fields.extend_from_slice(input.schema().fields());
                LogicalPlan::Window {
                    input,
                    window_expr,
                    schema: Arc::new(DFSchema::new(window_fields)?),
                }
            }
            LogicalPlanLanguage::Aggregate(params) => {
                let input = Arc::new(self.to_logical_plan(params[0])?);
                let group_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[1], AggregateGroupExpr);
                let aggr_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[2], AggregateAggrExpr);
                let group_expr = normalize_cols(group_expr, &input)?;
                let aggr_expr = normalize_cols(aggr_expr, &input)?;
                let all_expr = group_expr.iter().chain(aggr_expr.iter());
                let schema = Arc::new(DFSchema::new(exprlist_to_fields(
                    all_expr,
                    input.schema(),
                )?)?);
                LogicalPlan::Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                }
            }
            LogicalPlanLanguage::Sort(params) => {
                let expr = match_expr_list_node!(node_by_id, to_expr, params[0], SortExp);
                let input = Arc::new(self.to_logical_plan(params[1])?);
                LogicalPlan::Sort { expr, input }
            }
            LogicalPlanLanguage::Join(params) => {
                let left = Arc::new(self.to_logical_plan(params[0])?);
                let right = Arc::new(self.to_logical_plan(params[1])?);
                let left_on = match_data_node!(node_by_id, params[2], JoinLeftOn);
                let right_on = match_data_node!(node_by_id, params[3], JoinRightOn);
                let join_type = match_data_node!(node_by_id, params[4], JoinJoinType);
                let join_constraint = match_data_node!(node_by_id, params[5], JoinJoinConstraint);
                let schema = Arc::new(build_join_schema(
                    left.schema(),
                    right.schema(),
                    &join_type,
                )?);
                LogicalPlan::Join {
                    left,
                    right,
                    on: left_on.into_iter().zip_eq(right_on.into_iter()).collect(),
                    join_type,
                    join_constraint,
                    schema,
                }
            }
            LogicalPlanLanguage::CrossJoin(params) => {
                let left = Arc::new(self.to_logical_plan(params[0])?);
                let right = Arc::new(self.to_logical_plan(params[1])?);
                let schema = Arc::new(left.schema().join(right.schema())?);
                LogicalPlan::CrossJoin {
                    left,
                    right,
                    schema,
                }
            }
            // // TODO
            // LogicalPlan::Repartition { input, partitioning_scheme: _ } => {
            //     let input = self.add_logical_plan(input.as_ref())?;
            //     self.graph.add(LogicalPlanLanguage::Repartition([input]))
            // }
            // LogicalPlan::Union { inputs, schema: _, alias } => {
            //     let inputs = inputs.iter().map(|e| self.add_logical_plan(e)).collect::<Result<Vec<_>, _>>()?;
            //     let inputs = self.graph.add(LogicalPlanLanguage::UnionInputs(inputs));
            //     let alias = self.graph.add(LogicalPlanLanguage::UnionAlias(UnionAlias(alias.clone())));
            //     self.graph.add(LogicalPlanLanguage::Union([inputs, alias]))
            // }
            LogicalPlanLanguage::TableScan(params) => {
                let source_table_name =
                    match_data_node!(node_by_id, params[0], TableScanSourceTableName);
                let table_name = match_data_node!(node_by_id, params[1], TableScanTableName);
                let projection = match_data_node!(node_by_id, params[2], TableScanProjection);
                let filters =
                    match_expr_list_node!(node_by_id, to_expr, params[3], TableScanFilters);
                let limit = match_data_node!(node_by_id, params[4], TableScanLimit);
                let table_parts = source_table_name.split(".").collect::<Vec<_>>();
                let table_reference = if table_parts.len() == 2 {
                    TableReference::Partial {
                        schema: table_parts[0],
                        table: table_parts[1],
                    }
                } else if table_parts.len() == 3 {
                    TableReference::Full {
                        catalog: table_parts[0],
                        schema: table_parts[1],
                        table: table_parts[2],
                    }
                } else {
                    TableReference::from(source_table_name.as_str())
                };
                let provider = self
                    .cube_context
                    .get_table_provider(table_reference)
                    .ok_or(CubeError::user(format!(
                        "Table '{}' is not found",
                        source_table_name
                    )))?;
                let schema = provider.schema();

                let projected_schema = projection
                    .as_ref()
                    .map(|p| {
                        DFSchema::new(
                            p.iter()
                                .map(|i| {
                                    DFField::from_qualified(&table_name, schema.field(*i).clone())
                                })
                                .collect(),
                        )
                    })
                    .unwrap_or_else(|| DFSchema::try_from_qualified_schema(&table_name, &schema))?;
                LogicalPlan::TableScan {
                    table_name,
                    source: provider,
                    projection,
                    projected_schema: Arc::new(projected_schema),
                    filters,
                    limit,
                }
            }
            LogicalPlanLanguage::EmptyRelation(params) => {
                let produce_one_row =
                    match_data_node!(node_by_id, params[0], EmptyRelationProduceOneRow);
                LogicalPlan::EmptyRelation {
                    produce_one_row,
                    schema: Arc::new(DFSchema::empty()),
                } // TODO
            }
            LogicalPlanLanguage::Limit(params) => {
                let n = match_data_node!(node_by_id, params[0], LimitN);
                let input = Arc::new(self.to_logical_plan(params[1])?);
                LogicalPlan::Limit { n, input }
            }
            // LogicalPlan::CreateExternalTable { .. } => {
            //     panic!("CreateExternalTable is not supported");
            // }
            // LogicalPlan::Values { .. } => {
            //     panic!("Values is not supported");
            // }
            // LogicalPlan::Explain { .. } => {
            //     panic!("Explain is not supported");
            // }
            // LogicalPlan::Analyze { .. } => {
            //     panic!("Analyze is not supported");
            // }
            LogicalPlanLanguage::Extension(params) => {
                let node = match self.best_expr.index(params[0]) {
                    LogicalPlanLanguage::CubeScan(cube_scan_params) => {
                        let members =
                            match_list_node!(node_by_id, cube_scan_params[1], CubeScanMembers);
                        let order =
                            match_list_node!(node_by_id, cube_scan_params[3], CubeScanOrder);
                        // TODO filters
                        // TODO
                        let mut query = V1LoadRequestQuery::new();
                        let mut fields = Vec::new();
                        let mut query_measures = Vec::new();
                        let mut query_time_dimensions = Vec::new();
                        let mut query_order = Vec::new();
                        let mut query_dimensions = Vec::new();

                        for m in members {
                            match m {
                                LogicalPlanLanguage::Measure(measure_params) => {
                                    let measure = match_data_node!(
                                        node_by_id,
                                        measure_params[0],
                                        MeasureName
                                    );
                                    let expr = self.to_expr(measure_params[1])?;
                                    query_measures.push(measure);
                                    fields.push(DFField::new(
                                        None,
                                        // TODO empty schema
                                        &expr.name(&DFSchema::empty())?,
                                        DataType::Int64,
                                        // TODO actually nullable. Just to fit tests
                                        false,
                                    ));
                                }
                                LogicalPlanLanguage::TimeDimension(params) => {
                                    let dimension =
                                        match_data_node!(node_by_id, params[0], TimeDimensionName);
                                    let granularity = match_data_node!(
                                        node_by_id,
                                        params[1],
                                        TimeDimensionGranularity
                                    );
                                    let date_range = match_data_node!(
                                        node_by_id,
                                        params[2],
                                        TimeDimensionDateRange
                                    );
                                    let expr = self.to_expr(params[3])?;
                                    query_time_dimensions.push(V1LoadRequestQueryTimeDimension {
                                        dimension,
                                        granularity,
                                        date_range: date_range.map(|date_range| {
                                            serde_json::Value::Array(
                                                date_range
                                                    .into_iter()
                                                    .map(|d| serde_json::Value::String(d))
                                                    .collect(),
                                            )
                                        }),
                                    });
                                    fields.push(DFField::new(
                                        None,
                                        // TODO empty schema
                                        &expr.name(&DFSchema::empty())?,
                                        DataType::Timestamp(TimeUnit::Millisecond, None),
                                        // TODO actually nullable. Just to fit tests
                                        false,
                                    ));
                                }
                                LogicalPlanLanguage::Dimension(params) => {
                                    let dimension =
                                        match_data_node!(node_by_id, params[0], DimensionName);
                                    let expr = self.to_expr(params[1])?;
                                    query_dimensions.push(dimension);
                                    fields.push(DFField::new(
                                        None,
                                        // TODO empty schema
                                        &expr.name(&DFSchema::empty())?,
                                        // TODO
                                        DataType::Utf8,
                                        // TODO actually nullable. Just to fit tests
                                        false,
                                    ));
                                }
                                x => panic!("Expected dimension but found {:?}", x),
                            }
                        }

                        for o in order {
                            let order_params = match_params!(o, Order);
                            let order_member =
                                match_data_node!(node_by_id, order_params[0], OrderMember);
                            let order_asc = match_data_node!(node_by_id, order_params[1], OrderAsc);
                            query_order.push(vec![
                                order_member,
                                if order_asc {
                                    "asc".to_string()
                                } else {
                                    "desc".to_string()
                                },
                            ])
                        }

                        query.measures = Some(query_measures);
                        query.dimensions = Some(query_dimensions);
                        query.time_dimensions = if query_time_dimensions.len() > 0 {
                            Some(query_time_dimensions)
                        } else {
                            None
                        };
                        query.order = if query_order.len() > 0 {
                            Some(query_order)
                        } else {
                            None
                        };
                        query.segments = Some(Vec::new());
                        Arc::new(CubeScanNode::new(
                            Arc::new(DFSchema::new(fields)?),
                            query,
                            self.auth_context.clone(),
                        ))
                    }
                    x => panic!("Unexpected extension node: {:?}", x),
                };
                LogicalPlan::Extension { node }
            }
            x => panic!("Unexpected logical plan node: {:?}", x),
        })
    }
}
