use crate::{
    compile::{
        engine::{
            df::scan::{CubeScanNode, CubeScanOptions, CubeScanWrapperNode, MemberField},
            provider::CubeContext,
        },
        rewrite::{
            analysis::LogicalPlanAnalysis, rewriter::Rewriter, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, AggregateSplit, AggregateUDFExprFun, AliasExprAlias,
            AnyExprOp, BetweenExprNegated, BinaryExprOp, CastExprDataType, ChangeUserMemberValue,
            ColumnExprColumn, CubeScanLimit, CubeScanOffset, DimensionName,
            EmptyRelationProduceOneRow, FilterMemberMember, FilterMemberOp, FilterMemberValues,
            FilterOpOp, InListExprNegated, JoinJoinConstraint, JoinJoinType, JoinLeftOn,
            JoinRightOn, LikeExprEscapeChar, LikeExprLikeType, LikeExprNegated, LikeType,
            LimitFetch, LimitSkip, LiteralExprValue, LiteralMemberRelation, LiteralMemberValue,
            LogicalPlanLanguage, MeasureName, MemberErrorError, OrderAsc, OrderMember,
            OuterColumnExprColumn, OuterColumnExprDataType, ProjectionAlias, ProjectionSplit,
            ScalarFunctionExprFun, ScalarUDFExprFun, ScalarVariableExprDataType,
            ScalarVariableExprVariable, SegmentMemberMember, SortExprAsc, SortExprNullsFirst,
            TableScanFetch, TableScanProjection, TableScanSourceTableName, TableScanTableName,
            TableUDFExprFun, TimeDimensionDateRange, TimeDimensionGranularity, TimeDimensionName,
            TryCastExprDataType, UnionAlias, WindowFunctionExprFun, WindowFunctionExprWindowFrame,
        },
    },
    sql::AuthContextRef,
    CubeError,
};
use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    catalog::TableReference,
    logical_plan::{
        build_join_schema, build_table_udf_schema, exprlist_to_fields, normalize_cols,
        plan::{Aggregate, Extension, Filter, Join, Projection, Sort, TableUDFs, Window},
        CrossJoin, DFField, DFSchema, DFSchemaRef, Distinct, EmptyRelation, Expr, Like, Limit,
        LogicalPlan, LogicalPlanBuilder, TableScan, Union,
    },
    physical_plan::planner::DefaultPhysicalPlanner,
    scalar::ScalarValue,
    sql::planner::ContextProvider,
};
use egg::{EGraph, Id, RecExpr};
use itertools::Itertools;
use serde_json::json;
use std::{collections::HashMap, env, ops::Index, sync::Arc};

pub use super::rewriter::CubeRunner;

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

pub struct LogicalPlanToLanguageConverter {
    graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    cube_context: Arc<CubeContext>,
}

impl LogicalPlanToLanguageConverter {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            graph: EGraph::<LogicalPlanLanguage, LogicalPlanAnalysis>::new(
                LogicalPlanAnalysis::new(
                    cube_context.clone(),
                    Arc::new(DefaultPhysicalPlanner::default()),
                ),
            ),
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
            Expr::OuterColumn(data_type, column) => {
                let data_type = add_data_node!(self, data_type, OuterColumnExprDataType);
                let column = add_data_node!(self, column, OuterColumnExprColumn);
                self.graph
                    .add(LogicalPlanLanguage::OuterColumnExpr([data_type, column]))
            }
            Expr::ScalarVariable(data_type, variable) => {
                let data_type = add_data_node!(self, data_type, ScalarVariableExprDataType);
                let variable = add_data_node!(self, variable, ScalarVariableExprVariable);
                self.graph.add(LogicalPlanLanguage::ScalarVariableExpr([
                    data_type, variable,
                ]))
            }
            Expr::Literal(value) => {
                let value = add_data_node!(self, value, LiteralExprValue);
                self.graph.add(LogicalPlanLanguage::LiteralExpr([value]))
            }
            Expr::AnyExpr { left, op, right } => {
                let left = self.add_expr(left)?;
                let op = add_data_node!(self, op, AnyExprOp);
                let right = self.add_expr(right)?;
                self.graph
                    .add(LogicalPlanLanguage::AnyExpr([left, op, right]))
            }
            Expr::BinaryExpr { left, op, right } => {
                let left = self.add_expr(left)?;
                let op = add_data_node!(self, op, BinaryExprOp);
                let right = self.add_expr(right)?;
                self.graph
                    .add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
            }
            ast @ Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            })
            | ast @ Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            })
            | ast @ Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                let like_type = add_data_node!(
                    self,
                    match ast {
                        Expr::Like(_) => LikeType::Like,
                        Expr::ILike(_) => LikeType::ILike,
                        Expr::SimilarTo(_) => LikeType::SimilarTo,
                        _ => panic!("Expected LIKE, ILIKE, SIMILAR TO, got: {}", ast),
                    },
                    LikeExprLikeType
                );
                let negated = add_data_node!(self, negated, LikeExprNegated);
                let expr = self.add_expr(expr)?;
                let pattern = self.add_expr(pattern)?;
                let escape_char = add_data_node!(self, escape_char, LikeExprEscapeChar);
                self.graph.add(LogicalPlanLanguage::LikeExpr([
                    like_type,
                    negated,
                    expr,
                    pattern,
                    escape_char,
                ]))
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
            Expr::TableUDF { fun, args } => {
                let fun = add_data_node!(self, fun.name, TableUDFExprFun);
                let args = add_expr_list_node!(self, args, TableUDFExprArgs);
                self.graph
                    .add(LogicalPlanLanguage::TableUDFExpr([fun, args]))
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
            Expr::GetIndexedField { expr, key } => {
                let expr = self.add_expr(expr)?;
                let key = self.add_expr(key)?;
                self.graph
                    .add(LogicalPlanLanguage::GetIndexedFieldExpr([expr, key]))
            }
            // TODO: Support all
            _ => unimplemented!("Unsupported node type: {:?}", expr),
        })
    }

    pub fn add_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Id, CubeError> {
        Ok(match plan {
            LogicalPlan::Projection(node) => {
                let expr = add_expr_list_node!(self, node.expr, ProjectionExpr);
                let input = self.add_logical_plan(node.input.as_ref())?;
                let alias = add_data_node!(self, node.alias, ProjectionAlias);
                let split = add_data_node!(self, false, ProjectionSplit);
                self.graph
                    .add(LogicalPlanLanguage::Projection([expr, input, alias, split]))
            }
            LogicalPlan::Filter(node) => {
                let predicate = self.add_expr(&node.predicate)?;
                let input = self.add_logical_plan(node.input.as_ref())?;
                self.graph
                    .add(LogicalPlanLanguage::Filter([predicate, input]))
            }
            LogicalPlan::Window(node) => {
                let input = self.add_logical_plan(node.input.as_ref())?;
                let window_expr = add_expr_list_node!(self, node.window_expr, WindowWindowExpr);
                self.graph
                    .add(LogicalPlanLanguage::Window([input, window_expr]))
            }
            LogicalPlan::Aggregate(node) => {
                let input = self.add_logical_plan(node.input.as_ref())?;
                let group_expr = add_expr_list_node!(self, node.group_expr, AggregateGroupExpr);
                let aggr_expr = add_expr_list_node!(self, node.aggr_expr, AggregateAggrExpr);
                let split = add_data_node!(self, false, AggregateSplit);
                self.graph.add(LogicalPlanLanguage::Aggregate([
                    input, group_expr, aggr_expr, split,
                ]))
            }
            LogicalPlan::Sort(node) => {
                let expr = add_expr_list_node!(self, node.expr, SortExp);
                let input = self.add_logical_plan(node.input.as_ref())?;
                self.graph.add(LogicalPlanLanguage::Sort([expr, input]))
            }
            LogicalPlan::Join(node) => {
                let left = self.add_logical_plan(node.left.as_ref())?;
                let right = self.add_logical_plan(node.right.as_ref())?;
                let left_on = node
                    .on
                    .iter()
                    .map(|(left, _)| left.clone())
                    .collect::<Vec<_>>();
                let left_on = add_data_node!(self, left_on, JoinLeftOn);
                let right_on = node
                    .on
                    .iter()
                    .map(|(_, right)| right.clone())
                    .collect::<Vec<_>>();
                let right_on = add_data_node!(self, right_on, JoinRightOn);
                let join_type = add_data_node!(self, node.join_type, JoinJoinType);
                let join_constraint =
                    add_data_node!(self, node.join_constraint, JoinJoinConstraint);
                self.graph.add(LogicalPlanLanguage::Join([
                    left,
                    right,
                    left_on,
                    right_on,
                    join_type,
                    join_constraint,
                ]))
            }
            LogicalPlan::CrossJoin(node) => {
                let left = self.add_logical_plan(node.left.as_ref())?;
                let right = self.add_logical_plan(node.right.as_ref())?;
                self.graph
                    .add(LogicalPlanLanguage::CrossJoin([left, right]))
            }
            // TODO
            LogicalPlan::Repartition(node) => {
                let input = self.add_logical_plan(node.input.as_ref())?;
                self.graph.add(LogicalPlanLanguage::Repartition([input]))
            }
            LogicalPlan::Union(node) => {
                let inputs = add_plan_list_node!(self, node.inputs, UnionInputs);
                let alias = add_data_node!(self, node.alias, UnionAlias);
                self.graph.add(LogicalPlanLanguage::Union([inputs, alias]))
            }
            LogicalPlan::Subquery(node) => {
                let input = self.add_logical_plan(node.input.as_ref())?;
                let subqueries = add_plan_list_node!(self, node.subqueries, SubquerySubqueries);
                self.graph
                    .add(LogicalPlanLanguage::Subquery([input, subqueries]))
            }
            LogicalPlan::TableUDFs(node) => {
                let expr = add_expr_list_node!(self, node.expr, TableUDFsExpr);
                let input = self.add_logical_plan(node.input.as_ref())?;
                self.graph
                    .add(LogicalPlanLanguage::TableUDFs([expr, input]))
            }
            LogicalPlan::TableScan(node) => {
                let source_table_name = add_data_node!(
                    self,
                    self.cube_context
                        .table_name_by_table_provider(node.source.clone())?,
                    TableScanSourceTableName
                );

                let table_name = add_data_node!(self, node.table_name, TableScanTableName);
                let projection = add_data_node!(self, node.projection, TableScanProjection);
                let filters = add_expr_list_node!(self, node.filters, TableScanFilters);
                let fetch = add_data_node!(self, node.fetch, TableScanFetch);
                self.graph.add(LogicalPlanLanguage::TableScan([
                    source_table_name,
                    table_name,
                    projection,
                    filters,
                    fetch,
                ]))
            }
            LogicalPlan::EmptyRelation(rel) => {
                let produce_one_row =
                    add_data_node!(self, rel.produce_one_row, EmptyRelationProduceOneRow);
                self.graph
                    .add(LogicalPlanLanguage::EmptyRelation([produce_one_row]))
            }
            LogicalPlan::Limit(limit) => {
                let skip = add_data_node!(self, limit.skip, LimitSkip);
                let fetch = add_data_node!(self, limit.fetch, LimitFetch);
                let input = self.add_logical_plan(limit.input.as_ref())?;
                self.graph
                    .add(LogicalPlanLanguage::Limit([skip, fetch, input]))
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
            LogicalPlan::Extension(ext) => {
                if let Some(_cube_scan) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    todo!("LogicalPlanLanguage::Extension");
                    // self.graph.add(LogicalPlanLanguage::Extension([]))
                } else {
                    panic!("Unsupported extension node: {}", ext.node.schema());
                }
            }
            LogicalPlan::Distinct(distinct) => {
                let input = self.add_logical_plan(distinct.input.as_ref())?;
                self.graph.add(LogicalPlanLanguage::Distinct([input]))
            }
            // TODO: Support all
            _ => unimplemented!("Unsupported node type: {:?}", plan),
        })
    }

    pub fn take_rewriter(self) -> Rewriter {
        Rewriter::new(self.graph, self.cube_context)
    }

    pub fn take_runner(self) -> CubeRunner {
        Rewriter::rewrite_runner(self.cube_context, self.graph)
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

#[macro_export]
macro_rules! match_data_node {
    ($node_by_id:expr, $id_expr:expr, $field_variant:ident) => {
        match $node_by_id.index($id_expr.clone()) {
            LogicalPlanLanguage::$field_variant($field_variant(data)) => data.clone(),
            x => {
                return Err(CubeError::internal(format!(
                    "Expected {} but found {:?}",
                    std::stringify!($field_variant),
                    x
                )))
            }
        }
    };
}

macro_rules! match_list_node_ids {
    ($node_by_id:expr, $id_expr:expr, $field_variant:ident) => {{
        fn match_list(
            node_by_id: &impl Index<Id, Output = LogicalPlanLanguage>,
            id: Id,
            result: &mut Vec<Id>,
        ) -> Result<(), CubeError> {
            match node_by_id.index(id) {
                LogicalPlanLanguage::$field_variant(list) => {
                    for i in list {
                        match_list(node_by_id, *i, result)?;
                    }
                }
                _ => {
                    result.push(id);
                }
            }
            Ok(())
        }
        let mut result = Vec::new();
        match_list($node_by_id, $id_expr.clone(), &mut result)?;
        result
    }};
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
    best_expr: RecExpr<LogicalPlanLanguage>,
    cube_context: Arc<CubeContext>,
    auth_context: AuthContextRef,
}

pub fn is_expr_node(node: &LogicalPlanLanguage) -> bool {
    match node {
        LogicalPlanLanguage::AliasExpr(_) => true,
        LogicalPlanLanguage::ColumnExpr(_) => true,
        LogicalPlanLanguage::ScalarVariableExpr(_) => true,
        LogicalPlanLanguage::LiteralExpr(_) => true,
        LogicalPlanLanguage::BinaryExpr(_) => true,
        LogicalPlanLanguage::AnyExpr(_) => true,
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
        LogicalPlanLanguage::TableUDFExpr(_) => true,
        LogicalPlanLanguage::InListExpr(_) => true,
        LogicalPlanLanguage::WildcardExpr(_) => true,
        LogicalPlanLanguage::OuterColumnExpr(_) => true,
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
        LogicalPlanLanguage::OuterColumnExpr(params) => {
            let data_type = match_data_node!(node_by_id, params[0], OuterColumnExprDataType);
            let column = match_data_node!(node_by_id, params[1], OuterColumnExprColumn);
            Expr::OuterColumn(data_type, column)
        }
        LogicalPlanLanguage::ScalarVariableExpr(params) => {
            let data_type = match_data_node!(node_by_id, params[0], ScalarVariableExprDataType);
            let variable = match_data_node!(node_by_id, params[1], ScalarVariableExprVariable);
            Expr::ScalarVariable(data_type, variable)
        }
        LogicalPlanLanguage::LiteralExpr(params) => {
            let value = match_data_node!(node_by_id, params[0], LiteralExprValue);
            Expr::Literal(value)
        }
        LogicalPlanLanguage::AnyExpr(params) => {
            let left = Box::new(to_expr(params[0].clone())?);
            let op = match_data_node!(node_by_id, params[1], AnyExprOp);
            let right = Box::new(to_expr(params[2].clone())?);
            Expr::AnyExpr { left, op, right }
        }
        LogicalPlanLanguage::BinaryExpr(params) => {
            let left = Box::new(to_expr(params[0].clone())?);
            let op = match_data_node!(node_by_id, params[1], BinaryExprOp);
            let right = Box::new(to_expr(params[2].clone())?);
            Expr::BinaryExpr { left, op, right }
        }
        LogicalPlanLanguage::LikeExpr(params) => {
            let like_type = match_data_node!(node_by_id, params[0], LikeExprLikeType);
            let negated = match_data_node!(node_by_id, params[1], LikeExprNegated);
            let expr = Box::new(to_expr(params[2].clone())?);
            let pattern = Box::new(to_expr(params[3].clone())?);
            let escape_char = match_data_node!(node_by_id, params[4], LikeExprEscapeChar);
            let like_expr = Like {
                negated,
                expr,
                pattern,
                escape_char,
            };
            match like_type {
                LikeType::Like => Expr::Like(like_expr),
                LikeType::ILike => Expr::ILike(like_expr),
                LikeType::SimilarTo => Expr::SimilarTo(like_expr),
            }
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
        LogicalPlanLanguage::TableUDFExpr(params) => {
            let fun_name = match_data_node!(node_by_id, params[0], TableUDFExprFun);
            let args = match_expr_list_node!(node_by_id, to_expr, params[1], TableUDFExprArgs);
            let fun = cube_context
                .get_table_function_meta(&fun_name)
                .ok_or(CubeError::user(format!(
                    "Table UDF '{}' is not found",
                    fun_name
                )))?;
            Expr::TableUDF { fun, args }
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
        LogicalPlanLanguage::GetIndexedFieldExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            let key = Box::new(to_expr(params[1])?);
            Expr::GetIndexedField { expr, key }
        }
        x => panic!("Unexpected expression node: {:?}", x),
    })
}

impl LanguageToLogicalPlanConverter {
    pub fn new(
        best_expr: RecExpr<LogicalPlanLanguage>,
        cube_context: Arc<CubeContext>,
        auth_context: AuthContextRef,
    ) -> Self {
        Self {
            best_expr,
            cube_context,
            auth_context,
        }
    }

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
                let input_schema = DFSchema::new_with_metadata(
                    exprlist_to_fields(&expr, input.schema())?,
                    HashMap::new(),
                )?;
                let schema = match alias {
                    Some(ref alias) => input_schema.replace_qualifier(alias.as_str()),
                    None => input_schema,
                };

                LogicalPlan::Projection(Projection {
                    expr,
                    input,
                    alias,
                    schema: DFSchemaRef::new(schema),
                })
            }
            LogicalPlanLanguage::Filter(params) => {
                let predicate = self.to_expr(params[0])?;
                let input = Arc::new(self.to_logical_plan(params[1])?);

                LogicalPlan::Filter(Filter { predicate, input })
            }
            LogicalPlanLanguage::Window(params) => {
                let input = Arc::new(self.to_logical_plan(params[0])?);
                let window_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[1], WindowWindowExpr);
                let mut window_fields: Vec<DFField> =
                    exprlist_to_fields(window_expr.iter(), input.schema())?;
                window_fields.extend_from_slice(input.schema().fields());

                LogicalPlan::Window(Window {
                    input,
                    window_expr,
                    schema: Arc::new(DFSchema::new_with_metadata(window_fields, HashMap::new())?),
                })
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
                let schema = Arc::new(DFSchema::new_with_metadata(
                    exprlist_to_fields(all_expr, input.schema())?,
                    HashMap::new(),
                )?);

                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                })
            }
            LogicalPlanLanguage::Sort(params) => {
                let expr = match_expr_list_node!(node_by_id, to_expr, params[0], SortExp);
                let input = Arc::new(self.to_logical_plan(params[1])?);

                LogicalPlan::Sort(Sort { expr, input })
            }
            LogicalPlanLanguage::Join(params) => {
                let left_on = match_data_node!(node_by_id, params[2], JoinLeftOn);
                let right_on = match_data_node!(node_by_id, params[3], JoinRightOn);
                let left = self.to_logical_plan(params[0]);
                let right = self.to_logical_plan(params[1]);

                if self.is_cube_scan_node(params[0]) && self.is_cube_scan_node(params[1]) {
                    if left_on.iter().any(|c| c.name == "__cubeJoinField")
                        || right_on.iter().any(|c| c.name == "__cubeJoinField")
                    {
                        return Err(CubeError::internal(
                            "Can not join Cubes. This is most likely due to one of the following reasons:\n\
                            • one of the cubes contains a group by\n\
                            • one of the cubes contains a measure\n\
                            • the cube on the right contains a filter, sorting or limits\n".to_string(),
                        ));
                    } else {
                        return Err(CubeError::internal(
                            "Use __cubeJoinField to join Cubes".to_string(),
                        ));
                    }
                }

                let left = Arc::new(left?);
                let right = Arc::new(right?);

                let join_type = match_data_node!(node_by_id, params[4], JoinJoinType);
                let join_constraint = match_data_node!(node_by_id, params[5], JoinJoinConstraint);
                let schema = Arc::new(build_join_schema(
                    left.schema(),
                    right.schema(),
                    &join_type,
                )?);

                LogicalPlan::Join(Join {
                    left,
                    right,
                    on: left_on.into_iter().zip_eq(right_on.into_iter()).collect(),
                    join_type,
                    join_constraint,
                    schema,
                    // TODO: Pass to Graph
                    null_equals_null: true,
                })
            }
            LogicalPlanLanguage::CrossJoin(params) => {
                if self.is_cube_scan_node(params[0]) && self.is_cube_scan_node(params[1]) {
                    return Err(CubeError::internal(
                        "Can not join Cubes. This is most likely due to one of the following reasons:\n\
                        • one of the cubes contains a group by\n\
                        • one of the cubes contains a measure\n\
                        • the cube on the right contains a filter, sorting or limits\n".to_string(),
                    ));
                }

                let left = Arc::new(self.to_logical_plan(params[0])?);
                let right = Arc::new(self.to_logical_plan(params[1])?);
                let schema = Arc::new(left.schema().join(right.schema())?);

                LogicalPlan::CrossJoin(CrossJoin {
                    left,
                    right,
                    schema,
                })
            }
            // // TODO
            // LogicalPlan::Repartition { input, partitioning_scheme: _ } => {
            //     let input = self.add_logical_plan(input.as_ref())?;
            //     self.graph.add(LogicalPlanLanguage::Repartition([input]))
            // }
            LogicalPlanLanguage::Subquery(params) => {
                let input = self.to_logical_plan(params[0])?;
                let subqueries = match_list_node_ids!(node_by_id, params[1], SubquerySubqueries)
                    .into_iter()
                    .map(|n| self.to_logical_plan(n))
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .subquery(subqueries)?
                    .build()?
            }
            LogicalPlanLanguage::TableUDFs(params) => {
                let expr = match_expr_list_node!(node_by_id, to_expr, params[0], TableUDFsExpr);
                let input = Arc::new(self.to_logical_plan(params[1])?);
                let schema = build_table_udf_schema(&input, expr.as_slice())?;

                LogicalPlan::TableUDFs(TableUDFs {
                    expr,
                    input,
                    schema,
                })
            }
            LogicalPlanLanguage::TableScan(params) => {
                let source_table_name =
                    match_data_node!(node_by_id, params[0], TableScanSourceTableName);
                let table_name = match_data_node!(node_by_id, params[1], TableScanTableName);
                let projection = match_data_node!(node_by_id, params[2], TableScanProjection);
                let filters =
                    match_expr_list_node!(node_by_id, to_expr, params[3], TableScanFilters);
                let fetch = match_data_node!(node_by_id, params[4], TableScanFetch);
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
                        DFSchema::new_with_metadata(
                            p.iter()
                                .map(|i| {
                                    DFField::from_qualified(&table_name, schema.field(*i).clone())
                                })
                                .collect(),
                            HashMap::new(),
                        )
                    })
                    .unwrap_or_else(|| DFSchema::try_from_qualified_schema(&table_name, &schema))?;

                LogicalPlan::TableScan(TableScan {
                    table_name,
                    source: provider,
                    projection,
                    projected_schema: Arc::new(projected_schema),
                    filters,
                    fetch,
                })
            }
            LogicalPlanLanguage::EmptyRelation(params) => {
                let produce_one_row =
                    match_data_node!(node_by_id, params[0], EmptyRelationProduceOneRow);

                // TODO
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row,
                    schema: Arc::new(DFSchema::empty()),
                })
            }
            LogicalPlanLanguage::Limit(params) => {
                let skip = match_data_node!(node_by_id, params[0], LimitSkip);
                let fetch = match_data_node!(node_by_id, params[1], LimitFetch);
                let input = Arc::new(self.to_logical_plan(params[2])?);
                LogicalPlan::Limit(Limit { skip, fetch, input })
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
                                    query_measures.push(measure.to_string());
                                    let data_type = self
                                        .cube_context
                                        .meta
                                        .find_df_data_type(measure.to_string())
                                        .ok_or(CubeError::internal(format!(
                                            "Can't find measure '{}'",
                                            measure
                                        )))?;
                                    fields.push((
                                        DFField::new(
                                            expr_relation(&expr),
                                            &expr_name(&expr)?,
                                            data_type,
                                            true,
                                        ),
                                        MemberField::Member(measure.to_string()),
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
                                        dimension: dimension.to_string(),
                                        granularity: granularity.clone(),
                                        date_range: date_range.map(|date_range| {
                                            serde_json::Value::Array(
                                                date_range
                                                    .into_iter()
                                                    .map(|d| serde_json::Value::String(d))
                                                    .collect(),
                                            )
                                        }),
                                    });
                                    if let Some(granularity) = &granularity {
                                        fields.push((
                                            DFField::new(
                                                expr_relation(&expr),
                                                // TODO empty schema
                                                &expr_name(&expr)?,
                                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                                                true,
                                            ),
                                            MemberField::Member(format!(
                                                "{}.{}",
                                                dimension, granularity
                                            )),
                                        ));
                                    }
                                }
                                LogicalPlanLanguage::Dimension(params) => {
                                    let dimension =
                                        match_data_node!(node_by_id, params[0], DimensionName);
                                    let expr = self.to_expr(params[1])?;
                                    let data_type = self
                                        .cube_context
                                        .meta
                                        .find_df_data_type(dimension.to_string())
                                        .ok_or(CubeError::internal(format!(
                                            "Can't find dimension '{}'",
                                            dimension
                                        )))?;
                                    query_dimensions.push(dimension.to_string());
                                    fields.push((
                                        DFField::new(
                                            expr_relation(&expr),
                                            // TODO empty schema
                                            &expr_name(&expr)?,
                                            data_type,
                                            true,
                                        ),
                                        MemberField::Member(dimension),
                                    ));
                                }
                                LogicalPlanLanguage::Segment(params) => {
                                    let expr = self.to_expr(params[1])?;
                                    fields.push((
                                        DFField::new(
                                            expr_relation(&expr),
                                            // TODO empty schema
                                            &expr_name(&expr)?,
                                            DataType::Boolean,
                                            true,
                                        ),
                                        MemberField::Literal(ScalarValue::Boolean(None)),
                                    ));
                                }
                                LogicalPlanLanguage::ChangeUser(params) => {
                                    let expr = self.to_expr(params[1])?;
                                    fields.push((
                                        DFField::new(
                                            expr_relation(&expr),
                                            // TODO empty schema
                                            &expr_name(&expr)?,
                                            DataType::Utf8,
                                            true,
                                        ),
                                        MemberField::Literal(ScalarValue::Utf8(None)),
                                    ));
                                }
                                LogicalPlanLanguage::LiteralMember(params) => {
                                    let value =
                                        match_data_node!(node_by_id, params[0], LiteralMemberValue);
                                    let expr = self.to_expr(params[1])?;
                                    let relation = match_data_node!(
                                        node_by_id,
                                        params[2],
                                        LiteralMemberRelation
                                    );
                                    fields.push((
                                        DFField::new(
                                            relation.as_deref(),
                                            &expr_name(&expr)?,
                                            value.get_datatype(),
                                            true,
                                        ),
                                        MemberField::Literal(value),
                                    ));
                                }
                                LogicalPlanLanguage::VirtualField(params) => {
                                    let expr = self.to_expr(params[2])?;
                                    fields.push((
                                        DFField::new(
                                            expr_relation(&expr),
                                            // TODO empty schema
                                            &expr_name(&expr)?,
                                            DataType::Utf8,
                                            true,
                                        ),
                                        MemberField::Literal(ScalarValue::Utf8(None)),
                                    ));
                                }
                                LogicalPlanLanguage::MemberError(params) => {
                                    let error =
                                        match_data_node!(node_by_id, params[0], MemberErrorError);
                                    return Err(CubeError::user(error.to_string()));
                                }
                                LogicalPlanLanguage::AllMembers(_) => {
                                    return Err(CubeError::internal(
                                        "Can't detect Cube query and it may be not supported yet"
                                            .to_string(),
                                    ));
                                }
                                x => panic!("Expected dimension but found {:?}", x),
                            }
                        }

                        let filters =
                            match_list_node!(node_by_id, cube_scan_params[2], CubeScanFilters);

                        fn to_filter(
                            query_time_dimensions: &mut Vec<V1LoadRequestQueryTimeDimension>,
                            filters: Vec<LogicalPlanLanguage>,
                            node_by_id: &impl Index<Id, Output = LogicalPlanLanguage>,
                        ) -> Result<
                            (
                                Vec<V1LoadRequestQueryFilterItem>,
                                Vec<String>,
                                Option<String>,
                            ),
                            CubeError,
                        > {
                            let mut result = Vec::new();
                            let mut segments_result = Vec::new();
                            let mut change_user_result = Vec::new();

                            for f in filters {
                                match f {
                                    LogicalPlanLanguage::FilterOp(params) => {
                                        let filters = match_list_node!(
                                            node_by_id,
                                            params[0],
                                            FilterOpFilters
                                        );
                                        let op =
                                            match_data_node!(node_by_id, params[1], FilterOpOp);
                                        let (filters, segments, change_user) =
                                            to_filter(query_time_dimensions, filters, node_by_id)?;
                                        match op.as_str() {
                                            "and" => {
                                                result.push(V1LoadRequestQueryFilterItem {
                                                    member: None,
                                                    operator: None,
                                                    values: None,
                                                    or: None,
                                                    and: Some(
                                                        filters
                                                            .into_iter()
                                                            .map(|f| serde_json::json!(f))
                                                            .collect(),
                                                    ),
                                                });
                                                segments_result.extend(segments);

                                                if change_user.is_some() {
                                                    change_user_result.extend(change_user);
                                                }
                                            }
                                            "or" => {
                                                result.push(V1LoadRequestQueryFilterItem {
                                                    member: None,
                                                    operator: None,
                                                    values: None,
                                                    or: Some(
                                                        filters
                                                            .into_iter()
                                                            .map(|f| serde_json::json!(f))
                                                            .collect(),
                                                    ),
                                                    and: None,
                                                });
                                                if !segments.is_empty() {
                                                    return Err(CubeError::internal(
                                                        "Can't use OR operator with segments"
                                                            .to_string(),
                                                    ));
                                                }

                                                if change_user.is_some() {
                                                    return Err(CubeError::internal(
                                                        "Can't use OR operator with __user column"
                                                            .to_string(),
                                                    ));
                                                }
                                            }
                                            x => panic!("Unsupported filter operator: {}", x),
                                        }
                                    }
                                    LogicalPlanLanguage::FilterMember(params) => {
                                        let member = match_data_node!(
                                            node_by_id,
                                            params[0],
                                            FilterMemberMember
                                        );
                                        let op =
                                            match_data_node!(node_by_id, params[1], FilterMemberOp);
                                        let values = match_data_node!(
                                            node_by_id,
                                            params[2],
                                            FilterMemberValues
                                        );
                                        if op == "inDateRange" {
                                            let existing_time_dimension = query_time_dimensions
                                                .iter_mut()
                                                .find_map(|mut td| {
                                                    if td.dimension == member
                                                        && td.date_range.is_none()
                                                    {
                                                        td.date_range = Some(json!(values));
                                                        Some(td)
                                                    } else {
                                                        None
                                                    }
                                                });
                                            if existing_time_dimension.is_none() {
                                                let dimension = V1LoadRequestQueryTimeDimension {
                                                    dimension: member.to_string(),
                                                    granularity: None,
                                                    date_range: Some(json!(values)),
                                                };
                                                query_time_dimensions.push(dimension);
                                            }
                                        } else {
                                            result.push(V1LoadRequestQueryFilterItem {
                                                member: Some(member),
                                                operator: Some(op),
                                                values: if !values.is_empty() {
                                                    Some(values)
                                                } else {
                                                    None
                                                },
                                                or: None,
                                                and: None,
                                            });
                                        }
                                    }
                                    LogicalPlanLanguage::SegmentMember(params) => {
                                        let member = match_data_node!(
                                            node_by_id,
                                            params[0],
                                            SegmentMemberMember
                                        );
                                        segments_result.push(member);
                                    }
                                    LogicalPlanLanguage::ChangeUserMember(params) => {
                                        let member = match_data_node!(
                                            node_by_id,
                                            params[0],
                                            ChangeUserMemberValue
                                        );
                                        change_user_result.push(member);
                                    }
                                    x => panic!("Expected filter but found {:?}", x),
                                }
                            }

                            if change_user_result.len() > 1 {
                                return Err(CubeError::internal(
                                    "Unable to use multiple __user in one Cube query".to_string(),
                                ));
                            }

                            Ok((result, segments_result, change_user_result.pop()))
                        }

                        let (filters, segments, change_user) =
                            to_filter(&mut query_time_dimensions, filters, node_by_id)?;

                        query.filters = if filters.len() > 0 {
                            Some(filters)
                        } else {
                            None
                        };

                        query.segments = Some(segments);

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

                        if fields.len() == 0 {
                            return Err(CubeError::internal(
                                "Can't detect Cube query and it may be not supported yet"
                                    .to_string(),
                            ));
                        }

                        query.measures = Some(query_measures.into_iter().unique().collect());
                        query.dimensions = Some(query_dimensions.into_iter().unique().collect());
                        query.time_dimensions = if query_time_dimensions.len() > 0 {
                            Some(
                                query_time_dimensions
                                    .into_iter()
                                    .unique_by(|td| {
                                        (td.dimension.to_string(), td.granularity.clone())
                                    })
                                    .collect(),
                            )
                        } else {
                            None
                        };
                        query.order = if query_order.len() > 0 {
                            Some(query_order)
                        } else {
                            None
                        };
                        let cube_scan_query_limit = env::var("CUBEJS_DB_QUERY_LIMIT")
                            .map(|v| v.parse::<usize>().unwrap())
                            .unwrap_or(50000);
                        let fail_on_max_limit_hit = env::var("CUBESQL_FAIL_ON_MAX_LIMIT_HIT")
                            .map(|v| v.to_lowercase() == "true")
                            .unwrap_or(false);
                        let mut limit_was_changed = false;
                        query.limit = match match_data_node!(
                            node_by_id,
                            cube_scan_params[4],
                            CubeScanLimit
                        ) {
                            Some(n) => {
                                if n > cube_scan_query_limit {
                                    limit_was_changed = true;
                                }
                                Some(n)
                            }
                            None => {
                                if fail_on_max_limit_hit {
                                    limit_was_changed = true;
                                    Some(cube_scan_query_limit)
                                } else {
                                    None
                                }
                            }
                        }
                        .map(|n| n as i32);

                        let max_records = if fail_on_max_limit_hit && limit_was_changed {
                            Some(cube_scan_query_limit)
                        } else {
                            None
                        };

                        let offset =
                            match_data_node!(node_by_id, cube_scan_params[5], CubeScanOffset)
                                .map(|offset| offset as i32);
                        if offset.is_some() {
                            query.offset = offset;
                        }

                        fields = fields
                            .into_iter()
                            .unique_by(|(f, _)| f.qualified_name())
                            .collect();

                        let member_fields = fields.iter().map(|(_, m)| m.clone()).collect();

                        Arc::new(CubeScanNode::new(
                            Arc::new(DFSchema::new_with_metadata(
                                fields.into_iter().map(|(f, _)| f).collect(),
                                HashMap::new(),
                            )?),
                            member_fields,
                            query,
                            self.auth_context.clone(),
                            CubeScanOptions {
                                change_user,
                                max_records,
                            },
                        ))
                    }
                    x => panic!("Unexpected extension node: {:?}", x),
                };

                LogicalPlan::Extension(Extension { node })
            }
            LogicalPlanLanguage::CubeScanWrapper(params) => {
                let input = Arc::new(self.to_logical_plan(params[0])?);
                LogicalPlan::Extension(Extension {
                    node: Arc::new(CubeScanWrapperNode::new(input)),
                })
            }
            LogicalPlanLanguage::Union(params) => {
                let inputs = match_list_node_ids!(node_by_id, params[0], UnionInputs)
                    .into_iter()
                    .map(|n| self.to_logical_plan(n))
                    .collect::<Result<Vec<_>, _>>()?;

                let alias = match_data_node!(node_by_id, params[1], UnionAlias);

                let schema = inputs[0].schema().as_ref().clone();
                let schema = match alias {
                    Some(ref alias) => schema.replace_qualifier(alias.as_str()),
                    None => schema.strip_qualifiers(),
                };

                LogicalPlan::Union(Union {
                    inputs,
                    schema: Arc::new(schema),
                    alias,
                })
            }
            LogicalPlanLanguage::Distinct(params) => {
                let input = Arc::new(self.to_logical_plan(params[0])?);

                LogicalPlan::Distinct(Distinct { input })
            }
            x => panic!("Unexpected logical plan node: {:?}", x),
        })
    }

    fn is_cube_scan_node(&self, node_id: Id) -> bool {
        let node_by_id = &self.best_expr;
        match node_by_id.index(node_id) {
            LogicalPlanLanguage::Extension(params) => match node_by_id.index(params[0]) {
                LogicalPlanLanguage::CubeScan(_) => return true,
                _ => (),
            },
            _ => (),
        }

        return false;
    }
}

pub fn expr_name(expr: &Expr) -> Result<String, CubeError> {
    match expr {
        Expr::Column(c) => Ok(c.name.to_string()),
        _ => Ok(expr.name(&DFSchema::empty())?),
    }
}

pub fn expr_relation(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => c.relation.as_ref().map(|s| s.as_str()),
        _ => None,
    }
}
