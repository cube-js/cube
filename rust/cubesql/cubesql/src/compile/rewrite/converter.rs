pub use super::rewriter::CubeRunner;
use crate::{
    compile::{
        engine::df::{
            scan::{CubeScanNode, CubeScanOptions, MemberField},
            wrapper::{CubeScanWrapperNode, WrappedSelectNode},
        },
        rewrite::{
            analysis::LogicalPlanAnalysis,
            extract_exprlist_from_groupping_set,
            rewriter::{CubeEGraph, Rewriter},
            AggregateFunctionExprDistinct, AggregateFunctionExprFun, AggregateSplit,
            AggregateUDFExprFun, AliasExprAlias, AnyExprAll, AnyExprOp, BetweenExprNegated,
            BinaryExprOp, CastExprDataType, ChangeUserMemberValue, ColumnExprColumn,
            CubeScanAliasToCube, CubeScanJoinHints, CubeScanLimit, CubeScanOffset,
            CubeScanUngrouped, CubeScanWrapped, DimensionName, EmptyRelationDerivedSourceTableName,
            EmptyRelationIsWrappable, EmptyRelationProduceOneRow, FilterMemberMember,
            FilterMemberOp, FilterMemberValues, FilterOpOp, GroupingSetExprType, GroupingSetType,
            InListExprNegated, InSubqueryExprNegated, JoinJoinConstraint, JoinJoinType, JoinLeftOn,
            JoinNullEqualsNull, JoinRightOn, LikeExprEscapeChar, LikeExprLikeType, LikeExprNegated,
            LikeType, LimitFetch, LimitSkip, LiteralExprValue, LiteralMemberRelation,
            LiteralMemberValue, LogicalPlanLanguage, MeasureName, MemberErrorError, OrderAsc,
            OrderMember, OuterColumnExprColumn, OuterColumnExprDataType, ProjectionAlias,
            ProjectionSplit, QueryParamIndex, ScalarFunctionExprFun, ScalarUDFExprFun,
            ScalarVariableExprDataType, ScalarVariableExprVariable, SegmentMemberMember,
            SortExprAsc, SortExprNullsFirst, SubqueryTypes, TableScanFetch, TableScanProjection,
            TableScanSourceTableName, TableScanTableName, TableUDFExprFun, TimeDimensionDateRange,
            TimeDimensionGranularity, TimeDimensionName, TryCastExprDataType, UnionAlias,
            ValuesValues, WindowFunctionExprFun, WindowFunctionExprWindowFrame, WrappedSelectAlias,
            WrappedSelectDistinct, WrappedSelectJoinJoinType, WrappedSelectLimit,
            WrappedSelectOffset, WrappedSelectPushToCube, WrappedSelectSelectType,
            WrappedSelectType,
        },
        CubeContext,
    },
    sql::AuthContextRef,
    transport::{SpanId, V1CubeMetaExt},
    CubeError,
};
use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    catalog::TableReference,
    error::DataFusionError,
    logical_plan::{
        build_join_schema, build_table_udf_schema, exprlist_to_fields,
        exprlist_to_fields_from_schema, normalize_col as df_normalize_col,
        plan::{Aggregate, Extension, Filter, Join, Projection, Sort, TableUDFs, Window},
        replace_col_to_expr, Column, CrossJoin, DFField, DFSchema, DFSchemaRef, Distinct,
        EmptyRelation, Expr, ExprRewritable, ExprRewriter, GroupingSet, Like, Limit, LogicalPlan,
        LogicalPlanBuilder, Repartition, Subquery, TableScan, Union,
    },
    physical_plan::planner::DefaultPhysicalPlanner,
    scalar::ScalarValue,
    sql::planner::ContextProvider,
};
use egg::{Id, RecExpr};
use itertools::Itertools;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Index,
    sync::{Arc, LazyLock},
};

macro_rules! add_data_node {
    ($converter:expr, $value_expr:expr, $field_variant:ident) => {
        $converter
            .graph
            .add(LogicalPlanLanguage::$field_variant($field_variant(
                $value_expr.clone(),
            )))
    };
}

macro_rules! add_expr_data_node {
    ($graph:expr, $value_expr:expr, $field_variant:ident) => {
        $graph.add(LogicalPlanLanguage::$field_variant($field_variant(
            $value_expr.clone(),
        )))
    };
}

macro_rules! add_expr_list_node {
    ($graph:expr, $value_expr:expr, $query_params:expr, $field_variant:ident, $flat_list:expr) => {{
        let list = $value_expr
            .iter()
            .map(|expr| Self::add_expr_replace_params($graph, expr, $query_params, $flat_list))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = $graph.add(LogicalPlanLanguage::$field_variant(Vec::new()));
        for i in list.into_iter().rev() {
            current = $graph.add(LogicalPlanLanguage::$field_variant(vec![i, current]));
        }
        current
    }};
}

macro_rules! add_expr_flat_list_node {
    ($graph:expr, $value_expr:expr, $query_params:expr, $field_variant:ident, $flat_list:expr) => {{
        let list = $value_expr
            .iter()
            .map(|expr| Self::add_expr_replace_params($graph, expr, $query_params, $flat_list))
            .collect::<Result<Vec<_>, _>>()?;
        if $flat_list {
            $graph.add(LogicalPlanLanguage::$field_variant(list))
        } else {
            let mut current = $graph.add(LogicalPlanLanguage::$field_variant(Vec::new()));
            for i in list.into_iter().rev() {
                current = $graph.add(LogicalPlanLanguage::$field_variant(vec![i, current]));
            }
            current
        }
    }};
}

macro_rules! add_binary_expr_list_node {
    ($graph:expr, $value_expr:expr, $query_params:expr, $field_variant:ident, $flat_list:expr) => {{
        if $flat_list {
            add_expr_flat_list_node!(
                $graph,
                $value_expr,
                $query_params,
                $field_variant,
                $flat_list
            )
        } else {
            fn to_binary_tree(graph: &mut CubeEGraph, list: &[Id]) -> Id {
                if list.len() == 0 {
                    graph.add(LogicalPlanLanguage::$field_variant(Vec::new()))
                } else if list.len() == 1 {
                    let empty = graph.add(LogicalPlanLanguage::$field_variant(Vec::new()));
                    graph.add(LogicalPlanLanguage::$field_variant(vec![list[0], empty]))
                } else if list.len() == 2 {
                    graph.add(LogicalPlanLanguage::$field_variant(vec![list[0], list[1]]))
                } else {
                    let middle = list.len() / 2;
                    let left = to_binary_tree(graph, &list[..middle]);
                    let right = to_binary_tree(graph, &list[middle..]);
                    graph.add(LogicalPlanLanguage::$field_variant(vec![left, right]))
                }
            }
            let list = $value_expr
                .iter()
                .map(|expr| Self::add_expr_replace_params($graph, expr, $query_params, $flat_list))
                .collect::<Result<Vec<_>, _>>()?;
            to_binary_tree($graph, &list)
        }
    }};
}

macro_rules! add_plan_list_node {
    ($converter:expr, $value_expr:expr, $query_params:expr, $ctx:expr, $field_variant:ident) => {{
        let list = $value_expr
            .iter()
            .map(|expr| $converter.add_logical_plan_replace_params(expr, $query_params, $ctx))
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

static EXCLUDED_PARAM_VALUES: LazyLock<HashSet<ScalarValue>> = LazyLock::new(|| {
    vec![
        ScalarValue::Utf8(Some("second".to_string())),
        ScalarValue::Utf8(Some("minute".to_string())),
        ScalarValue::Utf8(Some("hour".to_string())),
        ScalarValue::Utf8(Some("day".to_string())),
        ScalarValue::Utf8(Some("week".to_string())),
        ScalarValue::Utf8(Some("month".to_string())),
        ScalarValue::Utf8(Some("year".to_string())),
    ]
    .into_iter()
    .chain((0..50).map(|i| ScalarValue::Int64(Some(i))))
    .collect()
});

pub struct LogicalPlanToLanguageConverter {
    graph: CubeEGraph,
    cube_context: Arc<CubeContext>,
    flat_list: bool,
}

#[derive(Default, Clone)]
pub struct LogicalPlanToLanguageContext {
    subquery_source_table_name: Option<String>,
}

impl LogicalPlanToLanguageConverter {
    pub fn new(cube_context: Arc<CubeContext>, flat_list: bool) -> Self {
        Self {
            graph: CubeEGraph::new(LogicalPlanAnalysis::new(
                cube_context.clone(),
                Arc::new(DefaultPhysicalPlanner::default()),
            )),
            cube_context,
            flat_list,
        }
    }

    pub fn add_expr(graph: &mut CubeEGraph, expr: &Expr, flat_list: bool) -> Result<Id, CubeError> {
        // TODO: reference self?
        Self::add_expr_replace_params(graph, expr, &mut None, flat_list)
    }

    pub fn add_expr_replace_params(
        graph: &mut CubeEGraph,
        expr: &Expr,
        query_params: &mut Option<HashMap<usize, ScalarValue>>,
        flat_list: bool,
    ) -> Result<Id, CubeError> {
        Ok(match expr {
            Expr::Alias(expr, alias) => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let alias = add_expr_data_node!(graph, alias, AliasExprAlias);
                graph.add(LogicalPlanLanguage::AliasExpr([expr, alias]))
            }
            Expr::Column(column) => {
                let column = add_expr_data_node!(graph, column, ColumnExprColumn);
                graph.add(LogicalPlanLanguage::ColumnExpr([column]))
            }
            Expr::OuterColumn(data_type, column) => {
                let data_type = add_expr_data_node!(graph, data_type, OuterColumnExprDataType);
                let column = add_expr_data_node!(graph, column, OuterColumnExprColumn);
                graph.add(LogicalPlanLanguage::OuterColumnExpr([data_type, column]))
            }
            Expr::ScalarVariable(data_type, variable) => {
                let data_type = add_expr_data_node!(graph, data_type, ScalarVariableExprDataType);
                let variable = add_expr_data_node!(graph, variable, ScalarVariableExprVariable);
                graph.add(LogicalPlanLanguage::ScalarVariableExpr([
                    data_type, variable,
                ]))
            }
            Expr::Literal(value) => {
                if let Some(ref mut query_params) = query_params {
                    if !EXCLUDED_PARAM_VALUES.contains(value) && !value.is_null() {
                        let param_index = query_params.len();
                        query_params.insert(param_index, value.clone());
                        let index = add_expr_data_node!(graph, param_index, QueryParamIndex);
                        graph.add(LogicalPlanLanguage::QueryParam([index]))
                    } else {
                        let value = add_expr_data_node!(graph, value, LiteralExprValue);
                        graph.add(LogicalPlanLanguage::LiteralExpr([value]))
                    }
                } else {
                    let value = add_expr_data_node!(graph, value, LiteralExprValue);
                    graph.add(LogicalPlanLanguage::LiteralExpr([value]))
                }
            }
            Expr::AnyExpr {
                left,
                op,
                right,
                all,
            } => {
                let left = Self::add_expr_replace_params(graph, left, query_params, flat_list)?;
                let op = add_expr_data_node!(graph, op, AnyExprOp);
                let right = Self::add_expr_replace_params(graph, right, query_params, flat_list)?;
                let all = add_expr_data_node!(graph, all, AnyExprAll);

                graph.add(LogicalPlanLanguage::AnyExpr([left, op, right, all]))
            }
            Expr::BinaryExpr { left, op, right } => {
                let left = Self::add_expr_replace_params(graph, left, query_params, flat_list)?;
                let op = add_expr_data_node!(graph, op, BinaryExprOp);
                let right = Self::add_expr_replace_params(graph, right, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::BinaryExpr([left, op, right]))
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
                let like_type = add_expr_data_node!(
                    graph,
                    match ast {
                        Expr::Like(_) => LikeType::Like,
                        Expr::ILike(_) => LikeType::ILike,
                        Expr::SimilarTo(_) => LikeType::SimilarTo,
                        _ => panic!("Expected LIKE, ILIKE, SIMILAR TO, got: {}", ast),
                    },
                    LikeExprLikeType
                );
                let negated = add_expr_data_node!(graph, negated, LikeExprNegated);
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let pattern =
                    Self::add_expr_replace_params(graph, pattern, query_params, flat_list)?;
                let escape_char = add_expr_data_node!(graph, escape_char, LikeExprEscapeChar);
                graph.add(LogicalPlanLanguage::LikeExpr([
                    like_type,
                    negated,
                    expr,
                    pattern,
                    escape_char,
                ]))
            }
            Expr::Not(expr) => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::NotExpr([expr]))
            }
            Expr::IsNotNull(expr) => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::IsNotNullExpr([expr]))
            }
            Expr::IsNull(expr) => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::IsNullExpr([expr]))
            }
            Expr::Negative(expr) => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::NegativeExpr([expr]))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let negated = add_expr_data_node!(graph, negated, BetweenExprNegated);
                let low = Self::add_expr_replace_params(graph, low, query_params, flat_list)?;
                let high = Self::add_expr_replace_params(graph, high, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::BetweenExpr([expr, negated, low, high]))
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let expr = add_expr_list_node!(graph, expr, query_params, CaseExprExpr, flat_list);
                let when_then_expr = when_then_expr
                    .iter()
                    .flat_map(|(when, then)| [when, then])
                    .collect::<Vec<_>>();
                let when_then_expr = add_expr_list_node!(
                    graph,
                    when_then_expr,
                    query_params,
                    CaseExprWhenThenExpr,
                    flat_list
                );
                let else_expr = add_expr_list_node!(
                    graph,
                    else_expr,
                    query_params,
                    CaseExprElseExpr,
                    flat_list
                );
                graph.add(LogicalPlanLanguage::CaseExpr([
                    expr,
                    when_then_expr,
                    else_expr,
                ]))
            }
            Expr::Cast { expr, data_type } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let data_type = add_expr_data_node!(graph, data_type, CastExprDataType);
                graph.add(LogicalPlanLanguage::CastExpr([expr, data_type]))
            }
            Expr::TryCast { expr, data_type } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let data_type = add_expr_data_node!(graph, data_type, TryCastExprDataType);
                graph.add(LogicalPlanLanguage::TryCastExpr([expr, data_type]))
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let asc = add_expr_data_node!(graph, asc, SortExprAsc);
                let nulls_first = add_expr_data_node!(graph, nulls_first, SortExprNullsFirst);
                graph.add(LogicalPlanLanguage::SortExpr([expr, asc, nulls_first]))
            }
            Expr::ScalarFunction { fun, args } => {
                let fun = add_expr_data_node!(graph, fun, ScalarFunctionExprFun);
                let args = add_expr_flat_list_node!(
                    graph,
                    args,
                    query_params,
                    ScalarFunctionExprArgs,
                    flat_list
                );

                graph.add(LogicalPlanLanguage::ScalarFunctionExpr([fun, args]))
            }
            Expr::ScalarUDF { fun, args } => {
                let fun = add_expr_data_node!(graph, fun.name, ScalarUDFExprFun);
                let args =
                    add_expr_list_node!(graph, args, query_params, ScalarUDFExprArgs, flat_list);
                graph.add(LogicalPlanLanguage::ScalarUDFExpr([fun, args]))
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                let fun = add_expr_data_node!(graph, fun, AggregateFunctionExprFun);
                let args = add_expr_list_node!(
                    graph,
                    args,
                    query_params,
                    AggregateFunctionExprArgs,
                    flat_list
                );
                let distinct = add_expr_data_node!(graph, distinct, AggregateFunctionExprDistinct);
                graph.add(LogicalPlanLanguage::AggregateFunctionExpr([
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
                let fun = add_expr_data_node!(graph, fun, WindowFunctionExprFun);
                let args = add_expr_list_node!(
                    graph,
                    args,
                    query_params,
                    WindowFunctionExprArgs,
                    flat_list
                );
                let partition_by = add_expr_list_node!(
                    graph,
                    partition_by,
                    query_params,
                    WindowFunctionExprPartitionBy,
                    flat_list
                );
                let order_by = add_expr_list_node!(
                    graph,
                    order_by,
                    query_params,
                    WindowFunctionExprOrderBy,
                    flat_list
                );
                let window_frame =
                    add_expr_data_node!(graph, window_frame, WindowFunctionExprWindowFrame);

                graph.add(LogicalPlanLanguage::WindowFunctionExpr([
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                ]))
            }
            Expr::AggregateUDF { fun, args } => {
                let fun = add_expr_data_node!(graph, fun.name, AggregateUDFExprFun);
                let args =
                    add_expr_list_node!(graph, args, query_params, AggregateUDFExprArgs, flat_list);
                graph.add(LogicalPlanLanguage::AggregateUDFExpr([fun, args]))
            }
            Expr::TableUDF { fun, args } => {
                let fun = add_expr_data_node!(graph, fun.name, TableUDFExprFun);
                let args =
                    add_expr_list_node!(graph, args, query_params, TableUDFExprArgs, flat_list);
                graph.add(LogicalPlanLanguage::TableUDFExpr([fun, args]))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let list =
                    add_expr_flat_list_node!(graph, list, query_params, InListExprList, flat_list);
                let negated = add_expr_data_node!(graph, negated, InListExprNegated);
                graph.add(LogicalPlanLanguage::InListExpr([expr, list, negated]))
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let subquery =
                    Self::add_expr_replace_params(graph, subquery, query_params, flat_list)?;
                let negated = add_expr_data_node!(graph, negated, InSubqueryExprNegated);

                graph.add(LogicalPlanLanguage::InSubqueryExpr([
                    expr, subquery, negated,
                ]))
            }
            Expr::Wildcard => graph.add(LogicalPlanLanguage::WildcardExpr([])),
            Expr::GetIndexedField { expr, key } => {
                let expr = Self::add_expr_replace_params(graph, expr, query_params, flat_list)?;
                let key = Self::add_expr_replace_params(graph, key, query_params, flat_list)?;
                graph.add(LogicalPlanLanguage::GetIndexedFieldExpr([expr, key]))
            }
            Expr::GroupingSet(groupping_set) => match groupping_set {
                GroupingSet::Rollup(members) => {
                    let members = add_expr_flat_list_node!(
                        graph,
                        members,
                        query_params,
                        GroupingSetExprMembers,
                        flat_list
                    );
                    let expr_type =
                        add_expr_data_node!(graph, GroupingSetType::Rollup, GroupingSetExprType);
                    graph.add(LogicalPlanLanguage::GroupingSetExpr([members, expr_type]))
                }
                GroupingSet::Cube(members) => {
                    let members = add_binary_expr_list_node!(
                        graph,
                        members,
                        query_params,
                        GroupingSetExprMembers,
                        false
                    );
                    let expr_type =
                        add_expr_data_node!(graph, GroupingSetType::Cube, GroupingSetExprType);
                    graph.add(LogicalPlanLanguage::GroupingSetExpr([members, expr_type]))
                }
                _ => unimplemented!("Unsupported grouping set type: {:?}", expr),
            },
            // TODO: Support all
            _ => unimplemented!("Unsupported node type: {:?}", expr),
        })
    }

    pub fn add_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Id, CubeError> {
        self.add_logical_plan_replace_params(
            plan,
            &mut None,
            &LogicalPlanToLanguageContext::default(),
        )
    }

    pub fn add_logical_plan_replace_params(
        &mut self,
        plan: &LogicalPlan,
        query_params: &mut Option<HashMap<usize, ScalarValue>>,
        ctx: &LogicalPlanToLanguageContext,
    ) -> Result<Id, CubeError> {
        Ok(match plan {
            LogicalPlan::Projection(node) => {
                let expr = add_binary_expr_list_node!(
                    &mut self.graph,
                    node.expr,
                    query_params,
                    ProjectionExpr,
                    self.flat_list
                );
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                let alias = add_data_node!(self, node.alias, ProjectionAlias);
                let split = add_data_node!(self, false, ProjectionSplit);
                self.graph
                    .add(LogicalPlanLanguage::Projection([expr, input, alias, split]))
            }
            LogicalPlan::Filter(node) => {
                let predicate = Self::add_expr_replace_params(
                    &mut self.graph,
                    &node.predicate,
                    query_params,
                    self.flat_list,
                )?;
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                self.graph
                    .add(LogicalPlanLanguage::Filter([predicate, input]))
            }
            LogicalPlan::Window(node) => {
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                let window_expr = add_expr_flat_list_node!(
                    &mut self.graph,
                    node.window_expr,
                    query_params,
                    WindowWindowExpr,
                    self.flat_list
                );
                self.graph
                    .add(LogicalPlanLanguage::Window([input, window_expr]))
            }
            LogicalPlan::Aggregate(node) => {
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                let group_expr = add_binary_expr_list_node!(
                    &mut self.graph,
                    node.group_expr,
                    query_params,
                    AggregateGroupExpr,
                    self.flat_list
                );
                let aggr_expr = add_binary_expr_list_node!(
                    &mut self.graph,
                    node.aggr_expr,
                    query_params,
                    AggregateAggrExpr,
                    self.flat_list
                );
                let split = add_data_node!(self, false, AggregateSplit);
                self.graph.add(LogicalPlanLanguage::Aggregate([
                    input, group_expr, aggr_expr, split,
                ]))
            }
            LogicalPlan::Sort(node) => {
                let expr = add_expr_list_node!(
                    &mut self.graph,
                    node.expr,
                    query_params,
                    SortExp,
                    self.flat_list
                );
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                self.graph.add(LogicalPlanLanguage::Sort([expr, input]))
            }
            LogicalPlan::Join(node) => {
                let left =
                    self.add_logical_plan_replace_params(node.left.as_ref(), query_params, ctx)?;
                let right =
                    self.add_logical_plan_replace_params(node.right.as_ref(), query_params, ctx)?;
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
                let null_equals_null =
                    add_data_node!(self, node.null_equals_null, JoinNullEqualsNull);
                self.graph.add(LogicalPlanLanguage::Join([
                    left,
                    right,
                    left_on,
                    right_on,
                    join_type,
                    join_constraint,
                    null_equals_null,
                ]))
            }
            LogicalPlan::CrossJoin(node) => {
                let left =
                    self.add_logical_plan_replace_params(node.left.as_ref(), query_params, ctx)?;
                let right =
                    self.add_logical_plan_replace_params(node.right.as_ref(), query_params, ctx)?;
                self.graph
                    .add(LogicalPlanLanguage::CrossJoin([left, right]))
            }
            // TODO
            LogicalPlan::Repartition(node) => {
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                self.graph.add(LogicalPlanLanguage::Repartition([input]))
            }
            LogicalPlan::Union(node) => {
                let inputs = add_plan_list_node!(self, node.inputs, query_params, ctx, UnionInputs);
                let alias = add_data_node!(self, node.alias, UnionAlias);
                self.graph.add(LogicalPlanLanguage::Union([inputs, alias]))
            }
            LogicalPlan::Subquery(node) => {
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
                let subquery_source_table_name =
                    self.find_source_table_name(node.input.as_ref())?;
                let mut sub_ctx = ctx.clone();
                sub_ctx.subquery_source_table_name = subquery_source_table_name;
                let subqueries = add_plan_list_node!(
                    self,
                    node.subqueries,
                    query_params,
                    &sub_ctx.clone(),
                    SubquerySubqueries
                );

                let types = add_data_node!(self, node.types, SubqueryTypes);
                self.graph
                    .add(LogicalPlanLanguage::Subquery([input, subqueries, types]))
            }
            LogicalPlan::TableUDFs(node) => {
                let expr = add_expr_list_node!(
                    &mut self.graph,
                    node.expr,
                    query_params,
                    TableUDFsExpr,
                    self.flat_list
                );
                let input =
                    self.add_logical_plan_replace_params(node.input.as_ref(), query_params, ctx)?;
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
                let filters = add_expr_list_node!(
                    &mut self.graph,
                    node.filters,
                    query_params,
                    TableScanFilters,
                    self.flat_list
                );
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
                let derived_source_table_name = add_data_node!(
                    self,
                    ctx.subquery_source_table_name,
                    EmptyRelationDerivedSourceTableName
                );
                let is_wrappable = add_data_node!(
                    self,
                    ctx.subquery_source_table_name.is_some(),
                    EmptyRelationIsWrappable
                );

                self.graph.add(LogicalPlanLanguage::EmptyRelation([
                    produce_one_row,
                    derived_source_table_name,
                    is_wrappable,
                ]))
            }
            LogicalPlan::Limit(limit) => {
                let skip = add_data_node!(self, limit.skip, LimitSkip);
                let fetch = add_data_node!(self, limit.fetch, LimitFetch);
                let input =
                    self.add_logical_plan_replace_params(limit.input.as_ref(), query_params, ctx)?;
                self.graph
                    .add(LogicalPlanLanguage::Limit([skip, fetch, input]))
            }
            LogicalPlan::Values(values) => {
                let values = add_data_node!(self, values.values, ValuesValues);
                self.graph.add(LogicalPlanLanguage::Values([values]))
            }
            LogicalPlan::CreateExternalTable { .. } => {
                panic!("CreateExternalTable is not supported");
            }
            LogicalPlan::Explain { .. } => {
                panic!("Explain is not supported");
            }
            LogicalPlan::Analyze { .. } => {
                panic!("Analyze is not supported");
            }
            // TODO
            LogicalPlan::Extension(ext) => {
                panic!("Unsupported extension node: {}", ext.node.schema());
            }
            LogicalPlan::Distinct(distinct) => {
                let input = self.add_logical_plan_replace_params(
                    distinct.input.as_ref(),
                    query_params,
                    ctx,
                )?;
                self.graph.add(LogicalPlanLanguage::Distinct([input]))
            }
            // TODO: Support all
            _ => unimplemented!("Unsupported node type: {:?}", plan),
        })
    }
    fn find_source_table_name(&self, plan: &LogicalPlan) -> Result<Option<String>, CubeError> {
        Ok(match plan {
            LogicalPlan::Projection(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Filter(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Window(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Aggregate(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Sort(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Join(node) => self.find_source_table_name(node.left.as_ref())?,
            LogicalPlan::CrossJoin(node) => self.find_source_table_name(node.left.as_ref())?,
            // TODO
            LogicalPlan::Repartition(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Union(node) => self.find_source_table_name(&node.inputs[0])?,
            LogicalPlan::Subquery(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::TableUDFs(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::TableScan(node) => Some(
                self.cube_context
                    .table_name_by_table_provider(node.source.clone())?,
            ),
            LogicalPlan::Limit(node) => self.find_source_table_name(node.input.as_ref())?,
            LogicalPlan::Distinct(node) => self.find_source_table_name(node.input.as_ref())?,
            _ => None,
        })
    }

    pub fn take_egraph(self) -> CubeEGraph {
        self.graph
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
                    result.push(to_expr(id)?);
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
    span_id: Option<Arc<SpanId>>,
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
            let expr = to_expr(params[0])?;
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
            let left = Box::new(to_expr(params[0])?);
            let op = match_data_node!(node_by_id, params[1], AnyExprOp);
            let right = Box::new(to_expr(params[2])?);
            let all = match_data_node!(node_by_id, params[3], AnyExprAll);
            Expr::AnyExpr {
                left,
                op,
                right,
                all,
            }
        }
        LogicalPlanLanguage::BinaryExpr(params) => {
            let left = Box::new(to_expr(params[0])?);
            let op = match_data_node!(node_by_id, params[1], BinaryExprOp);
            let right = Box::new(to_expr(params[2])?);
            Expr::BinaryExpr { left, op, right }
        }
        LogicalPlanLanguage::LikeExpr(params) => {
            let like_type = match_data_node!(node_by_id, params[0], LikeExprLikeType);
            let negated = match_data_node!(node_by_id, params[1], LikeExprNegated);
            let expr = Box::new(to_expr(params[2])?);
            let pattern = Box::new(to_expr(params[3])?);
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
            let expr = Box::new(to_expr(params[0])?);
            Expr::Not(expr)
        }
        LogicalPlanLanguage::IsNotNullExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            Expr::IsNotNull(expr)
        }
        LogicalPlanLanguage::IsNullExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            Expr::IsNull(expr)
        }
        LogicalPlanLanguage::NegativeExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            Expr::Negative(expr)
        }
        LogicalPlanLanguage::BetweenExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            let negated = match_data_node!(node_by_id, params[1], BetweenExprNegated);
            let low = Box::new(to_expr(params[2])?);
            let high = Box::new(to_expr(params[3])?);
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
            let expr = Box::new(to_expr(params[0])?);
            let data_type = match_data_node!(node_by_id, params[1], CastExprDataType);
            Expr::Cast { expr, data_type }
        }
        LogicalPlanLanguage::TryCastExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            let data_type = match_data_node!(node_by_id, params[1], TryCastExprDataType);
            Expr::TryCast { expr, data_type }
        }
        LogicalPlanLanguage::SortExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
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
            let expr = Box::new(to_expr(params[0])?);
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
        LogicalPlanLanguage::QueryParam(_) => {
            return Err(CubeError::user(
                "QueryParam can't be evaluated as an Expr node".to_string(),
            ));
        }
        LogicalPlanLanguage::InSubqueryExpr(params) => {
            let expr = Box::new(to_expr(params[0])?);
            let subquery = Box::new(to_expr(params[1])?);
            let negated = match_data_node!(node_by_id, params[2], InSubqueryExprNegated);
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            }
        }
        LogicalPlanLanguage::GroupingSetExpr(params) => {
            let members =
                match_expr_list_node!(node_by_id, to_expr, params[0], GroupingSetExprMembers);
            let expr_type = match_data_node!(node_by_id, params[1], GroupingSetExprType);

            match expr_type {
                GroupingSetType::Rollup => Expr::GroupingSet(GroupingSet::Rollup(members)),
                GroupingSetType::Cube => Expr::GroupingSet(GroupingSet::Cube(members)),
            }
        }
        x => panic!("Unexpected expression node: {:?}", x),
    })
}

impl LanguageToLogicalPlanConverter {
    pub fn new(
        best_expr: RecExpr<LogicalPlanLanguage>,
        cube_context: Arc<CubeContext>,
        auth_context: AuthContextRef,
        span_id: Option<Arc<SpanId>>,
    ) -> Self {
        Self {
            best_expr,
            cube_context,
            auth_context,
            span_id,
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
                let expr =
                    replace_qualified_col_with_flat_name_if_missing(expr, input.schema(), true)?;
                let alias = match_data_node!(node_by_id, params[2], ProjectionAlias);
                let input_schema = DFSchema::new_with_metadata(
                    exprlist_to_fields(&expr, &input)?,
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
                let window_expr = replace_qualified_col_with_flat_name_if_missing(
                    window_expr,
                    input.schema(),
                    true,
                )?;
                let mut window_fields: Vec<DFField> =
                    exprlist_to_fields(window_expr.iter(), &input)?;
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
                let group_expr = normalize_cols(
                    replace_qualified_col_with_flat_name_if_missing(
                        group_expr,
                        input.schema(),
                        true,
                    )?,
                    &input,
                )?;
                let aggr_expr = normalize_cols(
                    replace_qualified_col_with_flat_name_if_missing(
                        aggr_expr,
                        input.schema(),
                        true,
                    )?,
                    &input,
                )?;
                let all_expr = group_expr.iter().chain(aggr_expr.iter());
                let schema = Arc::new(DFSchema::new_with_metadata(
                    exprlist_to_fields(all_expr, &input)?,
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
                let expr =
                    replace_qualified_col_with_flat_name_if_missing(expr, input.schema(), true)?;

                LogicalPlan::Sort(Sort { expr, input })
            }
            LogicalPlanLanguage::Join(params) => {
                let left_on = match_data_node!(node_by_id, params[2], JoinLeftOn);
                let right_on = match_data_node!(node_by_id, params[3], JoinRightOn);
                let left = self.to_logical_plan(params[0])?;
                let right = self.to_logical_plan(params[1])?;

                // It's OK to join two grouped queries: expected row count is not that high, so
                // SQL API can, potentially, evaluate it completely
                // We don't really want it, so cost function should make WrappedSelect preferable
                // but still, we don't want to hard error on that
                // But if any one of join sides is ungroued, SQL API does not have much of a choice
                // but to process every row from ungrouped query, and that's Not Good
                if Self::have_ungrouped_cube_scan_inside(&left)
                    || Self::have_ungrouped_cube_scan_inside(&right)
                {
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

                let left = Arc::new(left);
                let right = Arc::new(right);

                let join_type = match_data_node!(node_by_id, params[4], JoinJoinType);
                let join_constraint = match_data_node!(node_by_id, params[5], JoinJoinConstraint);
                let schema = Arc::new(build_join_schema(
                    left.schema(),
                    right.schema(),
                    &join_type,
                )?);

                let null_equals_null = match_data_node!(node_by_id, params[6], JoinNullEqualsNull);

                LogicalPlan::Join(Join {
                    left,
                    right,
                    on: left_on.into_iter().zip_eq(right_on).collect(),
                    join_type,
                    join_constraint,
                    schema,
                    null_equals_null,
                })
            }
            LogicalPlanLanguage::CrossJoin(params) => {
                let left = self.to_logical_plan(params[0])?;
                let right = self.to_logical_plan(params[1])?;

                // See comment in Join conversion
                // Note that DF can generate Filter(CrossJoin(...)) for complex join conditions
                // But, from memory or dataset perspective it's the same: DF would buffer left side completely
                // And then iterate over right side, evaluting predicate
                // Regular join would use hash partitioning here, so it would be quicker, and utilize less CPU,
                // but transfer and buffering will be the same
                if Self::have_ungrouped_cube_scan_inside(&left)
                    || Self::have_ungrouped_cube_scan_inside(&right)
                {
                    return Err(CubeError::internal(
                        "Can not join Cubes. This is most likely due to one of the following reasons:\n\
                        • one of the cubes contains a group by\n\
                        • one of the cubes contains a measure\n\
                        • the cube on the right contains a filter, sorting or limits\n".to_string(),
                    ));
                }

                let left = Arc::new(left);
                let right = Arc::new(right);
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
                let types = match_data_node!(node_by_id, params[2], SubqueryTypes);
                LogicalPlanBuilder::from(input)
                    .subquery(subqueries, types)?
                    .build()?
            }
            LogicalPlanLanguage::TableUDFs(params) => {
                let expr = match_expr_list_node!(node_by_id, to_expr, params[0], TableUDFsExpr);
                let input = Arc::new(self.to_logical_plan(params[1])?);
                let expr =
                    replace_qualified_col_with_flat_name_if_missing(expr, input.schema(), true)?;
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
                panic!("Unexpected extension node: {:?}", params[0])
            }
            LogicalPlanLanguage::CubeScan(cube_scan_params) => {
                let alias_to_cube =
                    match_data_node!(node_by_id, cube_scan_params[0], CubeScanAliasToCube);
                let members = match_list_node!(node_by_id, cube_scan_params[1], CubeScanMembers);
                let order = match_list_node!(node_by_id, cube_scan_params[3], CubeScanOrder);
                let wrapped = match_data_node!(node_by_id, cube_scan_params[8], CubeScanWrapped);
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
                            let measure =
                                match_data_node!(node_by_id, measure_params[0], MeasureName);
                            let expr = self.to_expr(measure_params[1])?;
                            query_measures.push(measure.to_string());
                            let data_type =
                                self.cube_context.meta.find_df_data_type(&measure).ok_or(
                                    CubeError::internal(format!(
                                        "Can't find measure '{}'",
                                        measure
                                    )),
                                )?;
                            fields.push((
                                DFField::new(
                                    expr_relation(&expr),
                                    &expr_name(&expr)?,
                                    data_type,
                                    true,
                                ),
                                MemberField::regular(measure.to_string()),
                            ));
                        }
                        LogicalPlanLanguage::TimeDimension(params) => {
                            let dimension =
                                match_data_node!(node_by_id, params[0], TimeDimensionName);
                            let granularity =
                                match_data_node!(node_by_id, params[1], TimeDimensionGranularity);
                            let date_range =
                                match_data_node!(node_by_id, params[2], TimeDimensionDateRange);
                            let expr = self.to_expr(params[3])?;
                            let query_time_dimension = V1LoadRequestQueryTimeDimension {
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
                            };
                            if !query_time_dimensions.contains(&query_time_dimension) {
                                query_time_dimensions.push(query_time_dimension);
                            }
                            if let Some(granularity) = granularity {
                                fields.push((
                                    DFField::new(
                                        expr_relation(&expr),
                                        // TODO empty schema
                                        &expr_name(&expr)?,
                                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                                        true,
                                    ),
                                    MemberField::time_dimension(dimension.to_string(), granularity),
                                ));
                            }
                        }
                        LogicalPlanLanguage::Dimension(params) => {
                            let dimension = match_data_node!(node_by_id, params[0], DimensionName);
                            let expr = self.to_expr(params[1])?;
                            let data_type =
                                self.cube_context.meta.find_df_data_type(&dimension).ok_or(
                                    CubeError::internal(format!(
                                        "Can't find dimension '{}'",
                                        dimension
                                    )),
                                )?;
                            query_dimensions.push(dimension.to_string());
                            fields.push((
                                DFField::new(
                                    expr_relation(&expr),
                                    // TODO empty schema
                                    &expr_name(&expr)?,
                                    data_type,
                                    true,
                                ),
                                MemberField::regular(dimension),
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
                            let value = match_data_node!(node_by_id, params[0], LiteralMemberValue);
                            let expr = self.to_expr(params[1])?;
                            let relation =
                                match_data_node!(node_by_id, params[2], LiteralMemberRelation);
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
                            let error = match_data_node!(node_by_id, params[0], MemberErrorError);
                            return Err(CubeError::user(error.to_string()));
                        }
                        LogicalPlanLanguage::AllMembers(_) => {
                            if !wrapped {
                                return Err(CubeError::internal(
                                    "Can't detect Cube query and it may be not supported yet"
                                        .to_string(),
                                ));
                            } else {
                                for (alias, cube) in alias_to_cube.iter() {
                                    let cube = self
                                        .cube_context
                                        .meta
                                        .find_cube_with_name(cube)
                                        .ok_or_else(|| {
                                            CubeError::user(format!("Can't find cube '{}'", cube))
                                        })?;
                                    for column in cube.get_columns() {
                                        if self
                                            .cube_context
                                            .meta
                                            .is_synthetic_field(column.member_name())
                                        {
                                            fields.push((
                                                DFField::new(
                                                    Some(&alias),
                                                    column.get_name(),
                                                    column.get_column_type().to_arrow(),
                                                    true,
                                                ),
                                                MemberField::Literal(ScalarValue::Utf8(None)),
                                            ));
                                        } else {
                                            fields.push((
                                                DFField::new(
                                                    Some(&alias),
                                                    column.get_name(),
                                                    column.get_column_type().to_arrow(),
                                                    true,
                                                ),
                                                MemberField::regular(
                                                    column.member_name().to_string(),
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        x => panic!("Expected dimension but found {:?}", x),
                    }
                }

                let filters = match_list_node!(node_by_id, cube_scan_params[2], CubeScanFilters);

                fn to_filter(
                    query_time_dimensions: &mut Vec<V1LoadRequestQueryTimeDimension>,
                    filters: Vec<LogicalPlanLanguage>,
                    node_by_id: &impl Index<Id, Output = LogicalPlanLanguage>,
                    is_in_or: bool,
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
                                let filters =
                                    match_list_node!(node_by_id, params[0], FilterOpFilters);
                                let op = match_data_node!(node_by_id, params[1], FilterOpOp);
                                let is_and_op = op == "and";
                                let (filters, segments, change_user) = to_filter(
                                    query_time_dimensions,
                                    filters,
                                    node_by_id,
                                    !is_in_or || !is_and_op,
                                )?;
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
                                                "Can't use OR operator with segments".to_string(),
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
                                let member =
                                    match_data_node!(node_by_id, params[0], FilterMemberMember);
                                let op = match_data_node!(node_by_id, params[1], FilterMemberOp);
                                let values =
                                    match_data_node!(node_by_id, params[2], FilterMemberValues);
                                if !is_in_or && op == "inDateRange" {
                                    let existing_time_dimensions: Vec<_> = query_time_dimensions
                                        .iter_mut()
                                        .filter_map(|td| {
                                            if td.dimension == member && td.date_range.is_none() {
                                                td.date_range = Some(json!(values));
                                                Some(td)
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();
                                    if existing_time_dimensions.len() == 0 {
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
                                let member =
                                    match_data_node!(node_by_id, params[0], SegmentMemberMember);
                                segments_result.push(member);
                            }
                            LogicalPlanLanguage::ChangeUserMember(params) => {
                                let member =
                                    match_data_node!(node_by_id, params[0], ChangeUserMemberValue);
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
                    to_filter(&mut query_time_dimensions, filters, node_by_id, false)?;

                query.filters = if filters.len() > 0 {
                    Some(filters)
                } else {
                    None
                };

                query.segments = Some(segments);

                for o in order {
                    let order_params = match_params!(o, Order);
                    let order_member = match_data_node!(node_by_id, order_params[0], OrderMember);
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

                if !wrapped && fields.len() == 0 {
                    return Err(CubeError::internal(
                        "Can't detect Cube query and it may be not supported yet".to_string(),
                    ));
                }

                query.measures = Some(query_measures.into_iter().unique().collect());
                query.dimensions = Some(query_dimensions.into_iter().unique().collect());
                query.time_dimensions = if query_time_dimensions.len() > 0 {
                    Some(
                        query_time_dimensions
                            .into_iter()
                            .unique_by(|td| {
                                (
                                    td.dimension.to_string(),
                                    td.granularity.clone(),
                                    td.date_range
                                        .as_ref()
                                        .map(|range| serde_json::to_string(range).unwrap()),
                                )
                            })
                            .collect(),
                    )
                } else {
                    None
                };

                let cube_scan_query_limit =
                    self.cube_context
                        .sessions
                        .server
                        .config_obj
                        .non_streaming_query_max_row_limit() as usize;
                let fail_on_max_limit_hit = env::var("CUBESQL_FAIL_ON_MAX_LIMIT_HIT")
                    .map(|v| v.to_lowercase() == "true")
                    .unwrap_or(false);
                let mut limit_was_changed = false;
                query.limit =
                    match match_data_node!(node_by_id, cube_scan_params[4], CubeScanLimit) {
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

                let offset = match_data_node!(node_by_id, cube_scan_params[5], CubeScanOffset)
                    .map(|offset| offset as i32);
                if offset.is_some() {
                    query.offset = offset;
                }

                fields = fields
                    .into_iter()
                    .unique_by(|(f, _)| f.qualified_name())
                    .collect();

                let ungrouped =
                    match_data_node!(node_by_id, cube_scan_params[9], CubeScanUngrouped);

                if ungrouped {
                    query.ungrouped = Some(true);
                }

                let join_hints =
                    match_data_node!(node_by_id, cube_scan_params[10], CubeScanJoinHints);
                if join_hints.len() > 0 {
                    query.join_hints = Some(join_hints);
                }

                query.order = if !query_order.is_empty() {
                    Some(query_order)
                } else {
                    // If no order was specified in client SQL,
                    // there should be no order implicitly added.
                    // in case when CUBESQL_SQL_NO_IMPLICIT_ORDER it is set to true - no implicit order is
                    // added for all queries.
                    // We need to return empty array so the processing in
                    // BaseQuery.js won't automatically add default order

                    let cube_no_implicit_order = self
                        .cube_context
                        .sessions
                        .server
                        .config_obj
                        .no_implicit_order();

                    if cube_no_implicit_order || query.ungrouped == Some(true) {
                        Some(vec![])
                    } else {
                        None
                    }
                };

                let member_fields = fields.iter().map(|(_, m)| m.clone()).collect();

                let node = Arc::new(CubeScanNode::new(
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
                    alias_to_cube.into_iter().map(|(_, c)| c).unique().collect(),
                    self.span_id.clone(),
                ));

                LogicalPlan::Extension(Extension { node })
            }
            LogicalPlanLanguage::CubeScanWrapper(params) => {
                let input = Arc::new(self.to_logical_plan(params[0])?);
                LogicalPlan::Extension(Extension {
                    node: Arc::new(CubeScanWrapperNode::new(
                        input,
                        self.cube_context.meta.clone(),
                        self.auth_context.clone(),
                        self.span_id.clone(),
                        self.cube_context.sessions.server.config_obj.clone(),
                    )),
                })
            }
            LogicalPlanLanguage::WrappedSelect(params) => {
                let select_type = match_data_node!(node_by_id, params[0], WrappedSelectSelectType);
                let projection_expr = match_expr_list_node!(
                    node_by_id,
                    to_expr,
                    params[1],
                    WrappedSelectProjectionExpr
                );
                let subqueries =
                    match_list_node_ids!(node_by_id, params[2], WrappedSelectSubqueries)
                        .into_iter()
                        .map(|j| {
                            let input = Arc::new(self.to_logical_plan(j)?);
                            Ok(input)
                        })
                        .collect::<Result<Vec<_>, CubeError>>()?;
                let group_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[3], WrappedSelectGroupExpr);
                let aggr_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[4], WrappedSelectAggrExpr);
                let window_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[5], WrappedSelectWindowExpr);
                let from = Arc::new(self.to_logical_plan(params[6])?);
                let joins = match_list_node!(node_by_id, params[7], WrappedSelectJoins)
                    .into_iter()
                    .map(|j| {
                        if let LogicalPlanLanguage::WrappedSelectJoin(params) = j {
                            let input = Arc::new(self.to_logical_plan(params[0])?);
                            let join_expr = to_expr(params[1])?;
                            let join_type =
                                match_data_node!(node_by_id, params[2], WrappedSelectJoinJoinType);
                            Ok((input, join_expr, join_type))
                        } else {
                            panic!("Unexpected join node: {:?}", j)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let filter_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[8], WrappedSelectFilterExpr);
                let having_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[9], WrappedSelectHavingExpr);
                let limit = match_data_node!(node_by_id, params[10], WrappedSelectLimit);
                let offset = match_data_node!(node_by_id, params[11], WrappedSelectOffset);
                let order_expr =
                    match_expr_list_node!(node_by_id, to_expr, params[12], WrappedSelectOrderExpr);
                let alias = match_data_node!(node_by_id, params[13], WrappedSelectAlias);
                let distinct = match_data_node!(node_by_id, params[14], WrappedSelectDistinct);
                let push_to_cube =
                    match_data_node!(node_by_id, params[15], WrappedSelectPushToCube);

                let filter_expr = normalize_cols(
                    replace_qualified_col_with_flat_name_if_missing(
                        filter_expr,
                        from.schema(),
                        true,
                    )?,
                    &from,
                )?;
                let group_expr = normalize_cols(
                    replace_qualified_col_with_flat_name_if_missing(
                        group_expr,
                        from.schema(),
                        true,
                    )?,
                    &from,
                )?;
                let aggr_expr = normalize_cols(
                    replace_qualified_col_with_flat_name_if_missing(
                        aggr_expr,
                        from.schema(),
                        true,
                    )?,
                    &from,
                )?;
                let projection_expr = if projection_expr.is_empty()
                    && matches!(select_type, WrappedSelectType::Projection)
                {
                    from.schema()
                        .fields()
                        .iter()
                        .chain(
                            joins
                                .iter()
                                .flat_map(|(j, _, _)| j.schema().fields().iter()),
                        )
                        .map(|f| Expr::Column(f.qualified_column()))
                        .collect::<Vec<_>>()
                } else {
                    normalize_cols(
                        replace_qualified_col_with_flat_name_if_missing(
                            projection_expr,
                            from.schema(),
                            true,
                        )?,
                        &from,
                    )?
                };
                let all_expr_without_window = match select_type {
                    WrappedSelectType::Projection => projection_expr.clone(),
                    WrappedSelectType::Aggregate => {
                        extract_exprlist_from_groupping_set(&group_expr)
                            .iter()
                            .chain(aggr_expr.iter())
                            .cloned()
                            .collect()
                    }
                };
                // TODO support asterisk query?
                let all_expr_without_window = if all_expr_without_window.is_empty() {
                    from.schema()
                        .fields()
                        .iter()
                        .map(|f| Expr::Column(f.qualified_column()))
                        .collect::<Vec<_>>()
                } else {
                    all_expr_without_window
                };

                let mut subqueries_schema = DFSchema::empty();
                for subquery in subqueries.iter() {
                    subqueries_schema.merge(subquery.schema());
                }
                let mut joins_schema = DFSchema::empty();
                for join in joins.iter() {
                    joins_schema.merge(join.0.schema());
                }
                let schema_with_subqueries = from
                    .schema()
                    .join(&subqueries_schema)?
                    .join(&joins_schema)?;

                let without_window_fields = exprlist_to_fields_from_schema(
                    all_expr_without_window.iter(),
                    &schema_with_subqueries,
                )?;
                let replace_map = all_expr_without_window
                    .iter()
                    .zip(without_window_fields.iter())
                    .flat_map(|(e, f)| {
                        vec![
                            (
                                Column {
                                    relation: alias.clone(),
                                    name: f.name().clone(),
                                },
                                e.clone(),
                            ),
                            (
                                Column {
                                    relation: None,
                                    name: f.name().clone(),
                                },
                                e.clone(),
                            ),
                        ]
                    })
                    .collect::<Vec<_>>();
                let replace_map = replace_map
                    .iter()
                    .map(|(c, e)| (c, e))
                    .collect::<HashMap<_, _>>();
                let without_window_fields_schema = Arc::new(DFSchema::new_with_metadata(
                    without_window_fields.clone(),
                    HashMap::new(),
                )?);
                let window_expr_rebased = replace_qualified_col_with_flat_name_if_missing(
                    window_expr,
                    &without_window_fields_schema,
                    true,
                )?
                .iter()
                .map(|e| {
                    let original_expr_name = e.name(&without_window_fields_schema)?;
                    let new_expr = match replace_col_to_expr(e.clone(), &replace_map)? {
                        Expr::Alias(expr, _) => Expr::Alias(expr, original_expr_name),
                        expr => Expr::Alias(Box::new(expr), original_expr_name),
                    };
                    Ok::<_, DataFusionError>(new_expr)
                })
                .collect::<Result<Vec<_>, _>>()?;
                let order_expr_rebased = replace_qualified_col_with_flat_name_if_missing(
                    order_expr,
                    &without_window_fields_schema,
                    false,
                )?
                .iter()
                .map(|e| replace_col_to_expr(e.clone(), &replace_map))
                .collect::<Result<Vec<_>, _>>()?;
                let schema = DFSchema::new_with_metadata(
                    // TODO support joins schema
                    without_window_fields
                        .into_iter()
                        .chain(
                            exprlist_to_fields_from_schema(
                                window_expr_rebased.iter(),
                                &schema_with_subqueries,
                            )?
                            .into_iter(),
                        )
                        .collect(),
                    HashMap::new(),
                )?;

                let schema = match alias {
                    Some(ref alias) => schema.replace_qualifier(alias.as_str()),
                    None => schema,
                };

                LogicalPlan::Extension(Extension {
                    node: Arc::new(WrappedSelectNode::new(
                        Arc::new(schema),
                        select_type,
                        projection_expr,
                        subqueries,
                        group_expr,
                        aggr_expr,
                        window_expr_rebased,
                        from,
                        joins,
                        filter_expr,
                        having_expr,
                        limit,
                        offset,
                        order_expr_rebased,
                        alias,
                        distinct,
                        push_to_cube,
                    )),
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
            LogicalPlanLanguage::Values(values) => {
                let values = match_data_node!(node_by_id, values[0], ValuesValues);

                LogicalPlanBuilder::values(values)?.build()?
            }
            x => panic!("Unexpected logical plan node: {:?}", x),
        })
    }

    fn have_ungrouped_cube_scan_inside(node: &LogicalPlan) -> bool {
        match node {
            LogicalPlan::Projection(Projection { input, .. })
            | LogicalPlan::Filter(Filter { input, .. })
            | LogicalPlan::Window(Window { input, .. })
            | LogicalPlan::Aggregate(Aggregate { input, .. })
            | LogicalPlan::Sort(Sort { input, .. })
            | LogicalPlan::Repartition(Repartition { input, .. })
            | LogicalPlan::Limit(Limit { input, .. }) => {
                Self::have_ungrouped_cube_scan_inside(input)
            }
            LogicalPlan::Join(Join { left, right, .. })
            | LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                Self::have_ungrouped_cube_scan_inside(left)
                    || Self::have_ungrouped_cube_scan_inside(right)
            }
            LogicalPlan::Union(Union { inputs, .. }) => {
                inputs.iter().any(Self::have_ungrouped_cube_scan_inside)
            }
            LogicalPlan::Subquery(Subquery {
                input, subqueries, ..
            }) => {
                Self::have_ungrouped_cube_scan_inside(input)
                    || subqueries.iter().any(Self::have_ungrouped_cube_scan_inside)
            }
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(cube_scan) = node.as_any().downcast_ref::<CubeScanNode>() {
                    cube_scan.request.ungrouped == Some(true)
                } else if let Some(cube_scan_wrapper) =
                    node.as_any().downcast_ref::<CubeScanWrapperNode>()
                {
                    cube_scan_wrapper.has_ungrouped_scan()
                } else {
                    false
                }
            }
            _ => false,
        }
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

/// This function replaces fully qualified names with flat names in case of it is missing in the input from schema.
/// This is required to make sure schema matches when flatten replacers were applied to Aggregate nodes.
/// Columns referenced within Aggregate node would have fully qualified names in output schema while all other expressions won't contain qualifiers.
/// So aliases, which are introduced by flatten replacers on top of referenced columns, won't be able to have fully qualified names.
/// Due to this there's no way for flatten replacer to generate compilable schema.
/// Instead flatten replacers would introduce flat name aliases in a format of `qualifier.column` (`.` will be part of a string).
/// This function then replaces fully qualified references with flat name references repairing the schema.
/// TODO: introduce fully qualified names for aliases in Datafusion and remove this function.
fn replace_qualified_col_with_flat_name_if_missing(
    expr: Vec<Expr>,
    schema: &Arc<DFSchema>,
    original_alias: bool,
) -> Result<Vec<Expr>, CubeError> {
    struct FlattenColumnReplacer<'a> {
        schema: &'a Arc<DFSchema>,
        original_alias: bool,
    }

    impl<'a> ExprRewriter for FlattenColumnReplacer<'a> {
        fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
            if let Expr::Column(c) = expr {
                if self.schema.field_from_column(&c).is_err() {
                    if self
                        .schema
                        .field_with_unqualified_name(&c.flat_name())
                        .is_ok()
                    {
                        if self.original_alias {
                            Ok(Expr::Alias(
                                Box::new(Expr::Column(Column {
                                    name: c.flat_name(),
                                    relation: None,
                                })),
                                c.name.to_string(),
                            ))
                        } else {
                            Ok(Expr::Column(Column {
                                name: c.flat_name(),
                                relation: None,
                            }))
                        }
                    } else {
                        Ok(Expr::Column(c))
                    }
                } else {
                    Ok(Expr::Column(c))
                }
            } else {
                Ok(expr)
            }
        }
    }

    expr.into_iter()
        .map(|e| {
            e.rewrite(&mut FlattenColumnReplacer {
                schema,
                original_alias,
            })
            .map_err(|e| CubeError::from(e))
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Recursively normalize all Column expressions in a list of expression trees
fn normalize_cols(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>, CubeError> {
    exprs
        .into_iter()
        .map(|e| normalize_col(e.into(), plan))
        .collect()
}

/// Recursively call [`df_normalize_col`] on all Column expressions
/// in the `expr` expression tree, realiasing the expressions if the name is different.
fn normalize_col(expr: Expr, plan: &LogicalPlan) -> Result<Expr, CubeError> {
    if let Expr::Alias(_, _) = expr {
        return Ok(df_normalize_col(expr, plan)?);
    }
    let original_expr_name = expr_name(&expr)?;
    let mut normalized_expr = df_normalize_col(expr, plan)?;
    let normalized_expr_name = expr_name(&normalized_expr)?;
    if original_expr_name != normalized_expr_name {
        normalized_expr = normalized_expr.alias(&original_expr_name);
    }
    Ok(normalized_expr)
}
