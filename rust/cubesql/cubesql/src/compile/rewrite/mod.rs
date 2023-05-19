pub mod analysis;
pub mod converter;
mod cost;
pub mod language;
pub mod rewriter;
pub mod rules;

use crate::{compile::rewrite::analysis::LogicalPlanAnalysis, CubeError};
use datafusion::{
    arrow::datatypes::DataType,
    error::DataFusionError,
    logical_plan::{
        window_frames::WindowFrame, Column, DFSchema, Expr, ExprRewritable, ExprRewriter,
        JoinConstraint, JoinType, Operator,
    },
    physical_plan::{
        aggregates::AggregateFunction, functions::BuiltinScalarFunction,
        window_functions::WindowFunction,
    },
    scalar::ScalarValue,
};
use egg::{
    rewrite, Applier, EGraph, Id, Pattern, PatternAst, Rewrite, SearchMatches, Searcher, Subst,
    Symbol, Var,
};
use std::{
    fmt::{self, Display, Formatter},
    ops::Index,
    slice::Iter,
    str::FromStr,
};

// trace_macros!(true);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum LikeType {
    Like,
    ILike,
    SimilarTo,
}

impl Display for LikeType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let join_type = match self {
            LikeType::Like => "Like",
            LikeType::ILike => "ILike",
            LikeType::SimilarTo => "SimilarTo",
        };
        write!(f, "{}", join_type)
    }
}

crate::plan_to_language! {
    pub enum LogicalPlanLanguage {
        Projection {
            expr: Vec<Expr>,
            input: Arc<LogicalPlan>,
            schema: DFSchemaRef,
            alias: Option<String>,
            split: bool,
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
            split: bool,
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
        Subquery {
            input: Arc<LogicalPlan>,
            subqueries: Vec<LogicalPlan>,
            schema: DFSchemaRef,
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
            fetch: Option<usize>,
        },
        EmptyRelation {
            produce_one_row: bool,
            schema: DFSchemaRef,
        },
        Limit {
            skip: Option<usize>,
            fetch: Option<usize>,
            input: Arc<LogicalPlan>,
        },
        TableUDFs {
            expr: Vec<Expr>,
            input: Arc<LogicalPlan>,
            schema: DFSchemaRef,
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
        OuterColumnExpr {
            data_type: DataType,
            column: Column,
        },
        ScalarVariableExpr {
            data_type: DataType,
            variable: Vec<String>,
        },
        LiteralExpr { value: ScalarValue, },
        BinaryExpr {
            left: Box<Expr>,
            op: Operator,
            right: Box<Expr>,
        },
        AnyExpr {
            left: Box<Expr>,
            op: Operator,
            right: Box<Expr>,
        },
        LikeExpr {
            like_type: LikeType,
            negated: bool,
            expr: Box<Expr>,
            pattern: Box<Expr>,
            escape_char: Option<char>,
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
        TableUDFExpr {
            fun: Arc<TableUDF>,
            args: Vec<Expr>,
        },
        InListExpr {
            expr: Box<Expr>,
            list: Vec<Expr>,
            negated: bool,
        },
        WildcardExpr {},
        GetIndexedFieldExpr {
            expr: Box<Expr>,
            key: Box<Expr>,
        },
        CubeScan {
            alias_to_cube: Vec<(String, String)>,
            members: Vec<LogicalPlan>,
            filters: Vec<LogicalPlan>,
            order: Vec<LogicalPlan>,
            limit: Option<usize>,
            offset: Option<usize>,
            aliases: Option<Vec<(String, String)>>,
            split: bool,
            can_pushdown_join: bool,
            wrapped: bool,
        },
        CubeScanWrapper {
            input: Arc<LogicalPlan>,
            finalized: bool,
        },
        AllMembers {
            cube: String,
            alias: String,
        },
        Distinct {
            input: Arc<LogicalPlan>,
        },
        Measure {
            name: String,
            expr: Arc<Expr>,
        },
        Dimension {
            name: String,
            expr: Arc<Expr>,
        },
        Segment {
            name: String,
            expr: Arc<Expr>,
        },
        ChangeUser {
            cube: String,
            expr: Arc<Expr>,
        },
        VirtualField {
            name: String,
            cube: String,
            expr: Arc<Expr>,
        },
        LiteralMember {
            value: ScalarValue,
            expr: Arc<Expr>,
            relation: Option<String>,
        },
        Order {
            member: String,
            asc: bool,
        },
        FilterMember {
            member: String,
            op: String,
            values: Vec<String>,
        },
        SegmentMember {
            member: String,
        },
        ChangeUserMember {
            value: String,
        },
        MemberError {
            error: String,
            priority: usize,
            expr: Arc<Expr>,
            alias_to_cube: Vec<((String, String), String)>,
        },
        FilterOp {
            filters: Vec<LogicalPlan>,
            op: String,
        },
        TimeDimension {
            name: String,
            granularity: Option<String>,
            date_range: Option<Vec<String>>,
            expr: Arc<Expr>,
        },
        MemberAlias {
            name: String,
        },
        MemberReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<((String, String), String)>,
            aliases: Vec<(String, String)>,
        },
        MemberPushdownReplacer {
            members: Vec<LogicalPlan>,
            old_members: Arc<LogicalPlan>,
            alias_to_cube: Vec<((String, String), String)>,
        },
        MergedMembersReplacer {
            members: Vec<LogicalPlan>,
        },
        ListConcatPushdownReplacer {
            members: Arc<LogicalPlan>,
        },
        ListConcatPushupReplacer {
            members: Arc<LogicalPlan>,
        },
        TimeDimensionDateRangeReplacer {
            members: Vec<LogicalPlan>,
            member: String,
            date_range: Vec<String>,
        },
        FilterReplacer {
            filters: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
            members: Vec<LogicalPlan>,
            aliases: Vec<(String, String)>,
        },
        FilterCastUnwrapReplacer {
            filters: Vec<LogicalPlan>,
        },
        OrderReplacer {
            sort_expr: Vec<LogicalPlan>,
            column_name_to_member: Vec<(String, Option<String>)>,
        },
        InnerAggregateSplitReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        OuterProjectionSplitReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        OuterAggregateSplitReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        GroupExprSplitReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        GroupAggregateSplitReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        WrapperPushdownReplacer {
            member: Arc<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        WrapperPullupReplacer {
            member: Arc<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        // NOTE: converting this to a list might provide rewrite improvements
        CaseExprReplacer {
            members: Vec<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
        },
        EventNotification {
            name: String,
            members: Vec<LogicalPlan>,
            meta: Option<Vec<(String, String)>>,
        },
    }
}

// trace_macros!(false);

#[macro_export]
macro_rules! var_iter {
    ($eclass:expr, $field_variant:ident) => {{
        $eclass.nodes.iter().filter_map(|node| match node {
            LogicalPlanLanguage::$field_variant($field_variant(v)) => Some(v),
            _ => None,
        })
    }};
}

#[macro_export]
macro_rules! var_list_iter {
    ($eclass:expr, $field_variant:ident) => {{
        $eclass.nodes.iter().filter_map(|node| match node {
            LogicalPlanLanguage::$field_variant(v) => Some(v),
            _ => None,
        })
    }};
}

#[macro_export]
macro_rules! var {
    ($var_str:expr) => {
        $var_str.parse().unwrap()
    };
}

pub struct WithColumnRelation(Option<String>);

impl ExprRewriter for WithColumnRelation {
    fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
        match expr {
            Expr::Column(c) => Ok(Expr::Column(Column {
                name: c.name.to_string(),
                relation: if let Some(rel) = self.0.as_ref() {
                    c.relation.or_else(|| Some(rel.to_string()))
                } else {
                    None
                },
            })),
            e => Ok(e),
        }
    }
}

fn column_name_to_member_vec(
    member_name_to_expr: Vec<(Option<String>, Expr)>,
) -> Vec<(String, Option<String>)> {
    let mut relation = WithColumnRelation(None);
    member_name_to_expr
        .into_iter()
        .map(|(member, expr)| {
            vec![
                (expr_column_name(expr.clone(), &None), member.clone()),
                (expr_column_name_with_relation(expr, &mut relation), member),
            ]
        })
        .flatten()
        .collect::<Vec<_>>()
}

fn column_name_to_member_to_aliases(
    column_name_to_member: Vec<(String, Option<String>)>,
) -> Vec<(String, String)> {
    column_name_to_member
        .into_iter()
        .filter(|(_, member)| member.is_some())
        .map(|(column_name, member)| (column_name, member.unwrap()))
        .collect::<Vec<_>>()
}

fn member_name_by_alias(
    egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    id: Id,
    alias: &str,
) -> Option<String> {
    if let Some(member_name_to_expr) = egraph.index(id).data.member_name_to_expr.as_ref() {
        let column_name_to_member = column_name_to_member_vec(member_name_to_expr.clone());
        column_name_to_member
            .into_iter()
            .find(|(cn, _)| cn == alias)
            .map(|(_, member)| member)
            .flatten()
    } else {
        None
    }
}

fn referenced_columns(referenced_expr: Vec<Expr>) -> Vec<String> {
    referenced_expr
        .into_iter()
        .map(|expr| expr_column_name(expr, &None))
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
        expr_column_name_with_relation(expr, &mut WithColumnRelation(Some(cube.to_string())))
    } else {
        expr.name(&DFSchema::empty()).unwrap()
    }
}

pub fn rewrite(
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

pub fn transforming_rewrite<T>(
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
        TransformingPattern::new(applier.as_str(), move |egraph, _, subst| {
            transform_fn(egraph, subst)
        }),
    )
    .unwrap()
}

pub fn transforming_rewrite_with_root<T>(
    name: &str,
    searcher: String,
    applier: String,
    transform_fn: T,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool
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

pub fn transforming_chain_rewrite<T>(
    name: &str,
    main_searcher: String,
    chain: Vec<(&str, String)>,
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
        ChainSearcher {
            main: main_searcher.parse().unwrap(),
            chain: chain
                .into_iter()
                .map(|(var, pattern)| (var.parse().unwrap(), pattern.parse().unwrap()))
                .collect(),
        },
        TransformingPattern::new(applier.as_str(), move |egraph, _, subst| {
            transform_fn(egraph, subst)
        }),
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
    udf_expr_var_arg(fun_name, list_expr("ScalarUDFExprArgs", args))
}

fn udf_expr_var_arg(fun_name: impl Display, arg_list: impl Display) -> String {
    let prefix = if fun_name.to_string().starts_with("?") {
        ""
    } else {
        "ScalarUDFExprFun:"
    };
    format!("(ScalarUDFExpr {}{} {})", prefix, fun_name, arg_list)
}

fn udf_fun_expr_args(left: impl Display, right: impl Display) -> String {
    format!("(ScalarUDFExprArgs {} {})", left, right)
}

fn udf_fun_expr_args_empty_tail() -> String {
    "ScalarUDFExprArgs".to_string()
}

fn fun_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    fun_expr_var_arg(fun_name, list_expr("ScalarFunctionExprArgs", args))
}

fn fun_expr_var_arg(fun_name: impl Display, arg_list: impl Display) -> String {
    let prefix = if fun_name.to_string().starts_with("?") {
        ""
    } else {
        "ScalarFunctionExprFun:"
    };
    format!("(ScalarFunctionExpr {}{} {})", prefix, fun_name, arg_list)
}

fn scalar_fun_expr_args(left: impl Display, right: impl Display) -> String {
    format!("(ScalarFunctionExprArgs {} {})", left, right)
}

fn scalar_fun_expr_args_empty_tail() -> String {
    "ScalarFunctionExprArgs".to_string()
}

fn agg_fun_expr(fun_name: impl Display, args: Vec<impl Display>, distinct: impl Display) -> String {
    let prefix = if fun_name.to_string().starts_with("?") {
        ""
    } else {
        "AggregateFunctionExprFun:"
    };
    format!(
        "(AggregateFunctionExpr {}{} {} {})",
        prefix,
        fun_name,
        list_expr("AggregateFunctionExprArgs", args),
        distinct
    )
}

fn udaf_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    format!(
        "(AggregateUDFExpr {} {})",
        fun_name,
        list_expr("AggregateUDFExprArgs", args),
    )
}

fn limit(skip: impl Display, fetch: impl Display, input: impl Display) -> String {
    format!("(Limit {} {} {})", skip, fetch, input)
}

fn aggregate(
    input: impl Display,
    group: impl Display,
    aggr: impl Display,
    split: impl Display,
) -> String {
    format!("(Aggregate {} {} {} {})", input, group, aggr, split)
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
    let prefix = if op.to_string().starts_with("?") {
        ""
    } else {
        "BinaryExprOp:"
    };
    format!("(BinaryExpr {} {}{} {})", left, prefix, op, right)
}

fn inlist_expr(expr: impl Display, list: impl Display, negated: impl Display) -> String {
    format!("(InListExpr {} {} {})", expr, list, negated)
}

fn between_expr(
    expr: impl Display,
    negated: impl Display,
    low: impl Display,
    high: impl Display,
) -> String {
    format!("(BetweenExpr {} {} {} {})", expr, negated, low, high)
}

fn like_expr(
    like_type: impl Display,
    negated: impl Display,
    expr: impl Display,
    pattern: impl Display,
    escape_char: impl Display,
) -> String {
    format!(
        "(LikeExpr {} {} {} {} {})",
        like_type, negated, expr, pattern, escape_char
    )
}

fn negative_expr(expr: impl Display) -> String {
    format!("(NegativeExpr {})", expr)
}

fn not_expr(expr: impl Display) -> String {
    format!("(NotExpr {})", expr)
}

fn is_null_expr(expr: impl Display) -> String {
    format!("(IsNullExpr {})", expr)
}

fn is_not_null_expr(expr: impl Display) -> String {
    format!("(IsNotNullExpr {})", expr)
}

fn literal_expr(literal: impl Display) -> String {
    format!("(LiteralExpr {})", literal)
}

fn column_expr(column: impl Display) -> String {
    format!("(ColumnExpr {})", column)
}

fn cast_expr(expr: impl Display, data_type: impl Display) -> String {
    format!("(CastExpr {} {})", expr, data_type)
}

fn cast_expr_explicit(expr: impl Display, data_type: DataType) -> String {
    format!("(CastExpr {} (CastExprDataType:{}))", expr, data_type)
}

fn alias_expr(column: impl Display, alias: impl Display) -> String {
    format!("(AliasExpr {} {})", column, alias)
}

fn case_expr_var_arg(
    expr: impl Display,
    when_then: impl Display,
    else_expr: impl Display,
) -> String {
    format!("(CaseExpr {} {} {})", expr, when_then, else_expr)
}

fn case_expr<D: Display>(
    expr: Option<String>,
    when_then: Vec<(D, D)>,
    else_expr: Option<String>,
) -> String {
    case_expr_var_arg(
        case_expr_expr(expr),
        list_expr(
            "CaseExprWhenThenExpr",
            when_then
                .into_iter()
                .map(|(when, then)| vec![when, then])
                .flatten()
                .collect(),
        ),
        case_expr_else_expr(else_expr),
    )
}

fn case_expr_expr(expr: Option<String>) -> String {
    list_expr(
        "CaseExprExpr",
        match expr {
            Some(expr) => vec![expr],
            None => vec![],
        },
    )
}

fn case_expr_when_then_expr(left: impl Display, right: impl Display) -> String {
    format!("(CaseExprWhenThenExpr {} {})", left, right)
}

fn case_expr_when_then_expr_empty_tail() -> String {
    format!("CaseExprWhenThenExpr")
}

fn case_expr_else_expr(else_expr: Option<String>) -> String {
    list_expr(
        "CaseExprElseExpr",
        match else_expr {
            Some(else_expr) => vec![else_expr],
            None => vec![],
        },
    )
}

fn literal_string(literal_str: impl Display) -> String {
    format!("(LiteralExpr LiteralExprValue:s:{})", literal_str)
}

fn literal_int(literal_number: i64) -> String {
    format!("(LiteralExpr LiteralExprValue:i:{})", literal_number)
}

fn literal_float(literal_float: f64) -> String {
    format!("(LiteralExpr LiteralExprValue:f:{})", literal_float)
}

fn literal_bool(literal_bool: bool) -> String {
    format!("(LiteralExpr LiteralExprValue:b:{})", literal_bool)
}

fn projection(
    expr: impl Display,
    input: impl Display,
    alias: impl Display,
    split: impl Display,
) -> String {
    format!("(Projection {} {} {} {})", expr, input, alias, split)
}

fn sort(expr: impl Display, input: impl Display) -> String {
    format!("(Sort {} {})", expr, input)
}

fn filter(expr: impl Display, input: impl Display) -> String {
    format!("(Filter {} {})", expr, input)
}

fn join(
    left: impl Display,
    right: impl Display,
    left_on: impl Display,
    right_on: impl Display,
    join_type: impl Display,
    join_constraint: impl Display,
) -> String {
    let join_type_prefix = if join_type.to_string().starts_with("?") {
        ""
    } else {
        "JoinJoinType:"
    };
    let join_constraint_prefix = if join_constraint.to_string().starts_with("?") {
        ""
    } else {
        "JoinJoinConstraint:"
    };
    format!(
        "(Join {} {} {} {} {}{} {}{})",
        left,
        right,
        left_on,
        right_on,
        join_type_prefix,
        join_type,
        join_constraint_prefix,
        join_constraint,
    )
}

fn cross_join(left: impl Display, right: impl Display) -> String {
    format!("(CrossJoin {} {})", left, right)
}

fn member_replacer(
    members: impl Display,
    cube_aliases: impl Display,
    aliases: impl Display,
) -> String {
    format!("(MemberReplacer {} {} {})", members, cube_aliases, aliases)
}

fn member_pushdown_replacer(
    members: impl Display,
    old_members: impl Display,
    alias_to_cube: impl Display,
) -> String {
    format!(
        "(MemberPushdownReplacer {} {} {})",
        members, old_members, alias_to_cube
    )
}

fn merged_members_replacer(members: impl Display) -> String {
    format!("(MergedMembersReplacer {})", members)
}

fn list_concat_pushdown_replacer(members: impl Display) -> String {
    format!("(ListConcatPushdownReplacer {})", members)
}

fn list_concat_pushup_replacer(members: impl Display) -> String {
    format!("(ListConcatPushupReplacer {})", members)
}

fn time_dimension_date_range_replacer(
    members: impl Display,
    time_dimension_member: impl Display,
    date_range: impl Display,
) -> String {
    format!(
        "(TimeDimensionDateRangeReplacer {} {} {})",
        members, time_dimension_member, date_range
    )
}

fn order_replacer(members: impl Display, aliases: impl Display) -> String {
    format!("(OrderReplacer {} {})", members, aliases)
}

fn filter_replacer(
    members: impl Display,
    alias_to_cube: impl Display,
    cube_members: impl Display,
    aliases: impl Display,
) -> String {
    format!(
        "(FilterReplacer {} {} {} {})",
        members, alias_to_cube, cube_members, aliases
    )
}

fn filter_cast_unwrap_replacer(members: impl Display) -> String {
    format!("(FilterCastUnwrapReplacer {})", members)
}

fn inner_aggregate_split_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!(
        "(InnerAggregateSplitReplacer {} {})",
        members, alias_to_cube
    )
}

fn outer_projection_split_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!(
        "(OuterProjectionSplitReplacer {} {})",
        members, alias_to_cube
    )
}

fn outer_aggregate_split_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!(
        "(OuterAggregateSplitReplacer {} {})",
        members, alias_to_cube
    )
}

fn group_expr_split_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!("(GroupExprSplitReplacer {} {})", members, alias_to_cube)
}

fn group_aggregate_split_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!(
        "(GroupAggregateSplitReplacer {} {})",
        members, alias_to_cube
    )
}

fn case_expr_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!("(CaseExprReplacer {} {})", members, alias_to_cube)
}

fn wrapper_pushdown_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!("(WrapperPushdownReplacer {} {})", members, alias_to_cube)
}

fn wrapper_pullup_replacer(members: impl Display, alias_to_cube: impl Display) -> String {
    format!("(WrapperPullupReplacer {} {})", members, alias_to_cube)
}

fn event_notification(name: impl Display, members: impl Display, meta: impl Display) -> String {
    format!("(EventNotification {} {} {})", name, members, meta)
}

fn cube_scan_members(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanMembers {} {})", left, right)
}

fn cube_scan_members_empty_tail() -> String {
    format!("CubeScanMembers")
}

fn all_members(cube: impl Display, alias: impl Display) -> String {
    format!("(AllMembers {} {})", cube, alias)
}

fn cube_scan_filters(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanFilters {} {})", left, right)
}

fn cube_scan_filters_empty_tail() -> String {
    format!("CubeScanFilters")
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

fn filter_op(filters: impl Display, op: impl Display) -> String {
    format!("(FilterOp {} {})", filters, op)
}

fn filter_op_filters(left: impl Display, right: impl Display) -> String {
    format!("(FilterOpFilters {} {})", left, right)
}

fn filter_op_filters_empty_tail() -> String {
    format!("FilterOpFilters")
}

fn filter_member(member: impl Display, op: impl Display, values: impl Display) -> String {
    format!("(FilterMember {} {} {})", member, op, values)
}

fn segment_member(member: impl Display) -> String {
    format!("(SegmentMember {})", member)
}

fn change_user_member(member: impl Display) -> String {
    format!("(ChangeUserMember {})", member)
}

fn measure_expr(measure_name: impl Display, expr: impl Display) -> String {
    format!("(Measure {} {})", measure_name, expr)
}

fn dimension_expr(name: impl Display, expr: impl Display) -> String {
    format!("(Dimension {} {})", name, expr)
}

fn segment_expr(name: impl Display, expr: impl Display) -> String {
    format!("(Segment {} {})", name, expr)
}

fn change_user_expr(cube: impl Display, expr: impl Display) -> String {
    format!("(ChangeUser {} {})", cube, expr)
}

fn literal_member(value: impl Display, expr: impl Display, relation: impl Display) -> String {
    format!("(LiteralMember {} {} {})", value, expr, relation)
}

fn virtual_field_expr(name: impl Display, cube: impl Display, expr: impl Display) -> String {
    format!("(VirtualField {} {} {})", name, cube, expr)
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

fn table_scan(
    source_table_name: impl Display,
    table_name: impl Display,
    projection: impl Display,
    filters: impl Display,
    fetch: impl Display,
) -> String {
    format!(
        "(TableScan {} {} {} {} {})",
        source_table_name, table_name, projection, filters, fetch
    )
}

fn cube_scan(
    alias_to_cube: impl Display,
    members: impl Display,
    filters: impl Display,
    orders: impl Display,
    limit: impl Display,
    offset: impl Display,
    aliases: impl Display,
    split: impl Display,
    can_pushdown_join: impl Display,
    wrapped: impl Display,
) -> String {
    format!(
        "(Extension (CubeScan {} {} {} {} {} {} {} {} {} {}))",
        alias_to_cube,
        members,
        filters,
        orders,
        limit,
        offset,
        aliases,
        split,
        can_pushdown_join,
        wrapped
    )
}

fn cube_scan_wrapper(input: impl Display, finalized: impl Display) -> String {
    format!("(CubeScanWrapper {} {})", input, finalized)
}

pub fn original_expr_name(
    egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    id: Id,
) -> Option<String> {
    egraph[id].data.original_expr.as_ref().map(|e| match e {
        Expr::Column(c) => c.name.to_string(),
        _ => e.name(&DFSchema::empty()).unwrap(),
    })
}

fn search_match_chained<'a>(
    egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    cur_match: SearchMatches<'a, LogicalPlanLanguage>,
    chain: Iter<(Var, Pattern<LogicalPlanLanguage>)>,
) -> Option<SearchMatches<'a, LogicalPlanLanguage>> {
    let mut chain = chain.clone();
    let mut matches_to_merge = Vec::new();
    if let Some((var, pattern)) = chain.next() {
        for subst in cur_match.substs.iter() {
            if let Some(id) = subst.get(var.clone()) {
                if let Some(next_match) = pattern.search_eclass(egraph, id.clone()) {
                    let chain_matches = search_match_chained(
                        egraph,
                        SearchMatches {
                            eclass: cur_match.eclass.clone(),
                            substs: next_match
                                .substs
                                .iter()
                                .map(|next_subst| {
                                    let mut new_subst = subst.clone();
                                    for pattern_var in pattern.vars().into_iter() {
                                        if let Some(pattern_var_value) = next_subst.get(pattern_var)
                                        {
                                            new_subst
                                                .insert(pattern_var, pattern_var_value.clone());
                                        }
                                    }
                                    new_subst
                                })
                                .collect::<Vec<_>>(),
                            // TODO merge
                            ast: cur_match.ast.clone(),
                        },
                        chain.clone(),
                    );
                    matches_to_merge.extend(chain_matches);
                }
            }
        }
        if !matches_to_merge.is_empty() {
            let mut substs = Vec::new();
            for m in matches_to_merge {
                substs.extend(m.substs.clone());
            }
            Some(SearchMatches {
                eclass: cur_match.eclass.clone(),
                substs,
                // TODO merge
                ast: cur_match.ast.clone(),
            })
        } else {
            None
        }
    } else {
        Some(cur_match)
    }
}

pub struct ChainSearcher {
    main: Pattern<LogicalPlanLanguage>,
    chain: Vec<(Var, Pattern<LogicalPlanLanguage>)>,
}

impl Searcher<LogicalPlanLanguage, LogicalPlanAnalysis> for ChainSearcher {
    fn search(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> Vec<SearchMatches<LogicalPlanLanguage>> {
        let matches = self.main.search(egraph);
        let mut result = Vec::new();
        for m in matches {
            if let Some(m) = self.search_match_chained(egraph, m, self.chain.iter()) {
                result.push(m);
            }
        }
        result
    }

    fn search_eclass(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
    ) -> Option<SearchMatches<LogicalPlanLanguage>> {
        if let Some(m) = self.main.search_eclass(egraph, eclass) {
            self.search_match_chained(egraph, m, self.chain.iter())
        } else {
            None
        }
    }

    fn vars(&self) -> Vec<Var> {
        let mut vars = self.main.vars();
        for (_, p) in self.chain.iter() {
            vars.extend(p.vars());
        }
        vars
    }
}

impl ChainSearcher {
    fn search_match_chained<'a>(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        cur_match: SearchMatches<'a, LogicalPlanLanguage>,
        chain: Iter<(Var, Pattern<LogicalPlanLanguage>)>,
    ) -> Option<SearchMatches<'a, LogicalPlanLanguage>> {
        search_match_chained(egraph, cur_match, chain)
    }
}

pub struct TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool,
{
    pattern: Pattern<LogicalPlanLanguage>,
    vars_to_substitute: T,
}

impl<T> TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool,
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
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool,
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
        if (self.vars_to_substitute)(egraph, eclass, &mut new_subst) {
            self.pattern
                .apply_one(egraph, eclass, &new_subst, searcher_ast, rule_name)
        } else {
            Vec::new()
        }
    }
}
