pub mod analysis;
pub mod converter;
mod cost;
pub mod language;
pub mod rewriter;
pub mod rules;

use crate::{
    compile::rewrite::analysis::{LogicalPlanAnalysis, Member, OriginalExpr},
    CubeError,
};
use datafusion::{
    arrow::datatypes::DataType,
    error::DataFusionError,
    logical_plan::{
        plan::SubqueryType, window_frames::WindowFrame, Column, DFSchema, Expr, ExprRewritable,
        ExprRewriter, GroupingSet, JoinConstraint, JoinType, Operator, RewriteRecursion,
    },
    physical_plan::{
        aggregates::AggregateFunction, functions::BuiltinScalarFunction, windows::WindowFunction,
    },
    scalar::ScalarValue,
};
use egg::{
    rewrite, Applier, EGraph, Id, Language, Pattern, PatternAst, Rewrite, SearchMatches, Searcher,
    Subst, Symbol, Var,
};
use itertools::Itertools;
use std::{
    fmt::{self, Display, Formatter},
    ops::Index,
    slice::Iter,
    str::FromStr,
    sync::Arc,
};

use self::analysis::{LogicalPlanData, MemberNameToExpr};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum WrappedSelectType {
    Projection,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum GroupingSetType {
    Rollup,
    Cube,
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
            types: Vec<SubqueryType>,
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
            derived_source_table_name: Option<String>,
            is_wrappable: bool,
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
            all: bool,
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
        InSubqueryExpr {
            expr: Box<Expr>,
            subquery: Box<Expr>,
            negated: bool,
        },
        WildcardExpr {},
        GetIndexedFieldExpr {
            expr: Box<Expr>,
            key: Box<Expr>,
        },

        WrappedSelect {
            select_type: WrappedSelectType,
            projection_expr: Vec<Expr>,
            subqueries: Vec<LogicalPlan>,
            group_expr: Vec<Expr>,
            aggr_expr: Vec<Expr>,
            window_expr: Vec<Expr>,
            from: Arc<LogicalPlan>,
            joins: Vec<LogicalPlan>,
            filter_expr: Vec<Expr>,
            having_expr: Vec<Expr>,
            limit: Option<usize>,
            offset: Option<usize>,
            order_expr: Vec<Expr>,
            alias: Option<String>,
            distinct: bool,
            ungrouped: bool,
            ungrouped_scan: bool,
        },
        WrappedSelectJoin {
            input: Arc<LogicalPlan>,
            expr: Arc<Expr>,
            join_type: JoinType,
        },

        CubeScan {
            alias_to_cube: Vec<(String, String)>,
            members: Vec<LogicalPlan>,
            filters: Vec<LogicalPlan>,
            order: Vec<LogicalPlan>,
            limit: Option<usize>,
            offset: Option<usize>,
            split: bool,
            can_pushdown_join: bool,
            wrapped: bool,
            ungrouped: bool,
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
        FilterSimplifyReplacer {
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
        AggregateSplitPushDownReplacer {
            expr: Arc<Expr>,
            list_node: Arc<Expr>,
            alias_to_cube: Vec<(String, String)>,
        },
        AggregateSplitPullUpReplacer {
            inner_expr: Arc<Expr>,
            outer_expr: Arc<Expr>,
            list_node: Arc<Expr>,
            alias_to_cube: Vec<(String, String)>,
        },
        ProjectionSplitPushDownReplacer {
            expr: Arc<Expr>,
            list_node: Arc<Expr>,
            alias_to_cube: Vec<(String, String)>,
        },
        ProjectionSplitPullUpReplacer {
            inner_expr: Arc<Expr>,
            outer_expr: Arc<Expr>,
            list_node: Arc<Expr>,
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
            ungrouped: bool,
            in_projection: bool,
            cube_members: Vec<LogicalPlan>,
        },
        WrapperPullupReplacer {
            member: Arc<LogicalPlan>,
            alias_to_cube: Vec<(String, String)>,
            ungrouped: bool,
            in_projection: bool,
            cube_members: Vec<LogicalPlan>,
        },
        FlattenPushdownReplacer {
            expr: Arc<Expr>,
            inner_expr: Vec<Expr>,
            inner_alias: Option<String>,
            top_level: bool,
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
        GroupingSetExpr {
            members: Vec<Expr>,
            type: GroupingSetType,
        },
        QueryParam {
            index: usize,
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
                name: c.name,
                relation: if let Some(rel) = self.0.as_ref() {
                    c.relation.or_else(|| Some(rel.to_string()))
                } else {
                    None
                },
            })),
            e => Ok(e),
        }
    }

    // As a rewriter, it seems we only care about the top-level of the expression,
    // this function defn tells the rewriter to not recurse into the children of the expression
    fn pre_visit(&mut self, _expr: &Expr) -> datafusion::error::Result<RewriteRecursion> {
        Ok(RewriteRecursion::Mutate)
    }
}

// TODO(mwillsey) this should one day be replaced by LogicalPlan::find_member
pub fn column_name_to_member_vec(
    member_name_to_expr: Vec<(Option<String>, Member, Expr)>,
) -> Vec<(String, Option<String>)> {
    let mut relation = WithColumnRelation(None);
    member_name_to_expr
        .into_iter()
        .flat_map(|(member, _, expr)| {
            [
                (expr_column_name(&expr, &None), member.clone()),
                (expr_column_name_with_relation(&expr, &mut relation), member),
            ]
        })
        .collect::<Vec<_>>()
}

impl LogicalPlanData {
    fn find_member(
        &self,
        f: impl Fn(&MemberNameToExpr, &str) -> bool,
    ) -> Option<(&MemberNameToExpr, String)> {
        let mut relation = WithColumnRelation(None);
        for tuple @ (_, _member, expr) in self.member_name_to_expr.as_ref()?.iter() {
            let column_name = expr_column_name(&expr, &None);
            if f(tuple, &column_name) {
                return Some((tuple, column_name));
            }
            let column_name = expr_column_name_with_relation(&expr, &mut relation);
            if f(tuple, &column_name) {
                return Some((tuple, column_name));
            }
        }
        None
    }
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
    egraph
        .index(id)
        .data
        .find_member(|_, a| a == alias)
        .and_then(|(m, _a)| m.0.clone())
}

fn referenced_columns(referenced_expr: &[Expr]) -> Vec<String> {
    referenced_expr
        .iter()
        .map(|expr| expr_column_name(expr, &None))
        .collect::<Vec<_>>()
}

fn expr_column_name_with_relation(expr: &Expr, relation: &mut WithColumnRelation) -> String {
    expr.clone() // TODO(mwillsey) remove clone somehow
        .rewrite(relation)
        .unwrap()
        .name(&DFSchema::empty())
        .unwrap()
}

fn expr_column_name(expr: &Expr, cube: &Option<String>) -> String {
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

pub fn transforming_chain_rewrite_with_root<T>(
    name: &str,
    main_searcher: String,
    chain: Vec<(&str, String)>,
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
        ChainSearcher {
            main: main_searcher.parse().unwrap(),
            chain: chain
                .into_iter()
                .map(|(var, pattern)| (var.parse().unwrap(), pattern.parse().unwrap()))
                .collect(),
        },
        TransformingPattern::new(applier.as_str(), transform_fn),
    )
    .unwrap()
}

type ListMatches = Vec<Subst>;

#[derive(Clone, PartialEq)]
pub enum ListType {
    ProjectionExpr,
    WindowWindowExpr,
    AggregateGroupExpr,
    AggregateAggrExpr,
    ScalarFunctionExprArgs,
    GroupingSetExprMembers,
    WrappedSelectProjectionExpr,
    WrappedSelectGroupExpr,
    WrappedSelectAggrExpr,
    WrappedSelectWindowExpr,
    CubeScanMembers,
}

impl ListType {
    fn empty_list(&self) -> String {
        match self {
            Self::ProjectionExpr => projection_expr_empty_tail(),
            Self::WindowWindowExpr => window_window_expr_empty_tail(),
            Self::AggregateGroupExpr => aggr_group_expr_empty_tail(),
            Self::AggregateAggrExpr => aggr_aggr_expr_empty_tail(),
            Self::GroupingSetExprMembers => grouping_set_expr_members_empty_tail(),
            Self::ScalarFunctionExprArgs => scalar_fun_expr_args_empty_tail(),
            Self::WrappedSelectProjectionExpr => wrapped_select_projection_expr_empty_tail(),
            Self::WrappedSelectGroupExpr => wrapped_select_group_expr_empty_tail(),
            Self::WrappedSelectAggrExpr => wrapped_select_aggr_expr_empty_tail(),
            Self::WrappedSelectWindowExpr => wrapped_select_window_expr_empty_tail(),
            Self::CubeScanMembers => cube_scan_members_empty_tail(),
        }
    }
}

impl Display for ListType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.empty_list())
    }
}

struct ListNodeSearcher {
    list_type: ListType,
    list_var: Var,
    list_pattern: Pattern<LogicalPlanLanguage>,
    elem_pattern: Pattern<LogicalPlanLanguage>,
    top_level_elem_vars: Vec<Var>,
}

impl ListNodeSearcher {
    fn new(list_type: ListType, list_var: &str, list_pattern: &str, elem_pattern: &str) -> Self {
        Self {
            list_type,
            list_var: list_var.parse().unwrap(),
            list_pattern: list_pattern.parse().unwrap(),
            elem_pattern: elem_pattern.parse().unwrap(),
            top_level_elem_vars: vec![],
        }
    }

    fn with_top_level_elem_vars(mut self, vars: &[&str]) -> Self {
        self.top_level_elem_vars = vars.iter().map(|s| s.parse().unwrap()).collect();
        self
    }

    pub fn match_node(&self, node: &LogicalPlanLanguage) -> bool {
        self.match_node_by_list_type(node, &self.list_type)
    }

    pub fn match_node_by_list_type(
        &self,
        node: &LogicalPlanLanguage,
        list_type: &ListType,
    ) -> bool {
        match list_type {
            ListType::ProjectionExpr => {
                matches!(node, LogicalPlanLanguage::ProjectionExpr(_))
            }
            ListType::WindowWindowExpr => {
                matches!(node, LogicalPlanLanguage::WindowWindowExpr(_))
            }
            ListType::AggregateGroupExpr => {
                matches!(node, LogicalPlanLanguage::AggregateGroupExpr(_))
            }
            ListType::AggregateAggrExpr => {
                matches!(node, LogicalPlanLanguage::AggregateAggrExpr(_))
            }
            ListType::ScalarFunctionExprArgs => {
                matches!(node, LogicalPlanLanguage::ScalarFunctionExprArgs(_))
            }
            ListType::WrappedSelectProjectionExpr => {
                matches!(node, LogicalPlanLanguage::WrappedSelectProjectionExpr(_))
            }
            ListType::GroupingSetExprMembers => {
                matches!(node, LogicalPlanLanguage::GroupingSetExprMembers(_))
            }
            ListType::WrappedSelectGroupExpr => {
                matches!(node, LogicalPlanLanguage::WrappedSelectGroupExpr(_))
            }
            ListType::WrappedSelectAggrExpr => {
                matches!(node, LogicalPlanLanguage::WrappedSelectAggrExpr(_))
            }
            ListType::WrappedSelectWindowExpr => {
                matches!(node, LogicalPlanLanguage::WrappedSelectWindowExpr(_))
            }
            ListType::CubeScanMembers => {
                matches!(node, LogicalPlanLanguage::CubeScanMembers(_))
            }
        }
    }
}

impl Searcher<LogicalPlanLanguage, LogicalPlanAnalysis> for ListNodeSearcher {
    fn search_eclass_with_limit(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
        limit: usize,
    ) -> Option<SearchMatches<LogicalPlanLanguage>> {
        let mut matches = self
            .list_pattern
            .search_eclass_with_limit(egraph, eclass, limit)?;

        let mut new_substs: Vec<Subst> = vec![];
        for subst in matches.substs {
            let list_id = subst[self.list_var];
            for node in egraph[list_id].iter() {
                let list_children = node.children();
                if !self.match_node(node) || list_children.is_empty() {
                    continue;
                }

                let matches_product = list_children
                    .iter()
                    .map(|child| {
                        self.elem_pattern
                            .search_eclass_with_limit(egraph, *child, limit)
                            .map_or(vec![], |ms| ms.substs)
                    })
                    .multi_cartesian_product();

                // TODO(mwillsey) this could be optimized more by filtering the
                // matches as you go
                for list_matches in matches_product {
                    let subst0 = &list_matches[0];
                    let agree_with_top_level = list_matches.iter().all(|m| {
                        self.top_level_elem_vars
                            .iter()
                            .all(|&v| m.get(v) == subst0.get(v))
                    });

                    if agree_with_top_level {
                        let mut subst = subst.clone();
                        assert_eq!(list_matches.len(), list_children.len());
                        for &var in &self.top_level_elem_vars {
                            if let Some(id) = list_matches[0].get(var) {
                                subst.insert(var, *id);
                            }
                        }
                        subst.data = Some(Arc::new(list_matches));
                        new_substs.push(subst);
                    }
                }
            }
        }

        matches.substs = new_substs;
        (!matches.substs.is_empty()).then(|| matches)
    }

    fn vars(&self) -> Vec<Var> {
        let mut vars = self.list_pattern.vars();
        vars.extend(self.elem_pattern.vars());
        vars.push(self.list_var);
        vars
    }
}

struct ListNodeApplierList {
    list_type: ListType,
    new_list_var: Var,
    elem_pattern: PatternAst<LogicalPlanLanguage>,
}

impl ListNodeApplierList {
    pub fn make_node(&self, list: Vec<Id>) -> LogicalPlanLanguage {
        self.make_node_by_list_type(list, &self.list_type)
    }

    pub fn make_node_by_list_type(
        &self,
        list: Vec<Id>,
        list_type: &ListType,
    ) -> LogicalPlanLanguage {
        match list_type {
            ListType::ProjectionExpr => LogicalPlanLanguage::ProjectionExpr(list),
            ListType::WindowWindowExpr => LogicalPlanLanguage::WindowWindowExpr(list),
            ListType::AggregateGroupExpr => LogicalPlanLanguage::AggregateGroupExpr(list),
            ListType::AggregateAggrExpr => LogicalPlanLanguage::AggregateAggrExpr(list),
            ListType::ScalarFunctionExprArgs => LogicalPlanLanguage::ScalarFunctionExprArgs(list),
            ListType::WrappedSelectProjectionExpr => {
                LogicalPlanLanguage::WrappedSelectProjectionExpr(list)
            }
            ListType::GroupingSetExprMembers => LogicalPlanLanguage::GroupingSetExprMembers(list),
            ListType::WrappedSelectGroupExpr => LogicalPlanLanguage::WrappedSelectGroupExpr(list),
            ListType::WrappedSelectAggrExpr => LogicalPlanLanguage::WrappedSelectAggrExpr(list),
            ListType::WrappedSelectWindowExpr => LogicalPlanLanguage::WrappedSelectWindowExpr(list),
            ListType::CubeScanMembers => LogicalPlanLanguage::CubeScanMembers(list),
        }
    }
}

pub struct ListApplierListPattern {
    list_type: ListType,
    new_list_var: String,
    elem_pattern: String,
}

struct ListNodeApplier {
    list_pattern: PatternAst<LogicalPlanLanguage>,
    lists: Vec<ListNodeApplierList>,
}

impl ListNodeApplier {
    pub fn new(
        list_type: ListType,
        new_list_var: &str,
        list_pattern: &str,
        elem_pattern: &str,
    ) -> Self {
        Self::from_lists(
            list_pattern,
            [ListApplierListPattern {
                list_type,
                new_list_var: new_list_var.to_string(),
                elem_pattern: elem_pattern.to_string(),
            }],
        )
    }

    pub fn from_lists(
        list_pattern: &str,
        lists: impl IntoIterator<Item = ListApplierListPattern>,
    ) -> Self {
        Self {
            list_pattern: list_pattern.parse().unwrap(),
            lists: lists
                .into_iter()
                .map(|list| ListNodeApplierList {
                    list_type: list.list_type,
                    new_list_var: list.new_list_var.parse().unwrap(),
                    elem_pattern: list.elem_pattern.parse().unwrap(),
                })
                .collect(),
        }
    }
}

impl Applier<LogicalPlanLanguage, LogicalPlanAnalysis> for ListNodeApplier {
    fn apply_one(
        &self,
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&PatternAst<LogicalPlanLanguage>>,
        _rule_name: Symbol,
    ) -> Vec<Id> {
        let mut subst = subst.clone();

        let data = subst
            .data
            .as_ref()
            .expect("no data, did you use ListNodeSearcher?");
        let list_matches = data
            .downcast_ref::<ListMatches>()
            .expect("wrong data type")
            .clone();

        for list in &self.lists {
            let new_list = list_matches
                .iter()
                .map(|list_subst| {
                    let mut subst = subst.clone();
                    subst.extend(list_subst.iter());
                    egraph.add_instantiation(&list.elem_pattern, &subst)
                })
                .collect();

            subst.insert(list.new_list_var, egraph.add(list.make_node(new_list)));
        }

        let result_id = egraph.add_instantiation(&self.list_pattern, &subst);

        if egraph.union(eclass, result_id) {
            vec![result_id]
        } else {
            vec![]
        }
    }

    fn vars(&self) -> Vec<Var> {
        let mut vars = self.list_pattern.vars();
        for list in &self.lists {
            vars.extend(list.elem_pattern.vars());
            vars.retain(|v| *v != list.new_list_var); // this is bound by the applier itself
        }
        vars
    }
}

pub struct ListPattern {
    pattern: String,
    list_var: String,
    elem: String,
}

pub fn list_rewrite(
    name: &str,
    list_type: ListType,
    searcher: ListPattern,
    applier: ListPattern,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    let searcher = ListNodeSearcher::new(
        list_type.clone(),
        &searcher.list_var,
        &searcher.pattern,
        &searcher.elem,
    );
    let applier = ListNodeApplier::new(
        list_type,
        &applier.list_var,
        &applier.pattern,
        &applier.elem,
    );
    Rewrite::new(name.to_string(), searcher, applier).unwrap()
}

pub fn list_rewrite_with_lists(
    name: &str,
    list_type: ListType,
    searcher: ListPattern,
    applier_pattern: &str,
    lists: impl IntoIterator<Item = ListApplierListPattern>,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    let searcher = ListNodeSearcher::new(
        list_type.clone(),
        &searcher.list_var,
        &searcher.pattern,
        &searcher.elem,
    );
    let applier = ListNodeApplier::from_lists(applier_pattern, lists);
    Rewrite::new(name.to_string(), searcher, applier).unwrap()
}

pub fn list_rewrite_with_vars(
    name: &str,
    list_type: ListType,
    searcher: ListPattern,
    applier: ListPattern,
    top_level_elem_vars: &[&str],
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    let searcher = ListNodeSearcher::new(
        list_type.clone(),
        &searcher.list_var,
        &searcher.pattern,
        &searcher.elem,
    )
    .with_top_level_elem_vars(top_level_elem_vars);
    let applier = ListNodeApplier::new(
        list_type,
        &applier.list_var,
        &applier.pattern,
        &applier.elem,
    );
    Rewrite::new(name.to_string(), searcher, applier).unwrap()
}

pub fn list_rewrite_with_lists_and_vars(
    name: &str,
    list_type: ListType,
    searcher: ListPattern,
    applier_pattern: &str,
    lists: impl IntoIterator<Item = ListApplierListPattern>,
    top_level_elem_vars: &[&str],
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    let searcher = ListNodeSearcher::new(
        list_type.clone(),
        &searcher.list_var,
        &searcher.pattern,
        &searcher.elem,
    )
    .with_top_level_elem_vars(top_level_elem_vars);
    let applier = ListNodeApplier::from_lists(applier_pattern, lists);
    Rewrite::new(name.to_string(), searcher, applier).unwrap()
}

fn list_expr(list_type: impl Display, list: Vec<impl Display>) -> String {
    let mut current = list_type.to_string();
    for i in list.into_iter().rev() {
        current = format!("({} {} {})", list_type, i, current);
    }
    current
}

fn flat_list_expr(list_type: impl Display, list: Vec<impl Display>, is_flat: bool) -> String {
    if list.len() < 1 {
        return list_type.to_string();
    }
    if !is_flat {
        return list_expr(list_type, list);
    }
    let args_iter = list.iter().map(|arg| arg.to_string());
    let args_list: String = Itertools::intersperse(args_iter, " ".to_string()).collect();
    format!("({} {})", list_type, args_list)
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

fn fun_expr(fun_name: impl Display, args: Vec<impl Display>, is_flat: bool) -> String {
    let arg_list = fun_expr_args(args, is_flat);
    fun_expr_var_arg(fun_name, arg_list)
}

fn fun_expr_var_arg(fun_name: impl Display, arg_list: impl Display) -> String {
    let prefix = if fun_name.to_string().starts_with("?") {
        ""
    } else {
        "ScalarFunctionExprFun:"
    };
    format!("(ScalarFunctionExpr {}{} {})", prefix, fun_name, arg_list)
}

fn fun_expr_args(args: Vec<impl Display>, is_flat: bool) -> String {
    flat_list_expr("ScalarFunctionExprArgs", args, is_flat)
}

fn fun_expr_args_legacy(left: impl Display, right: impl Display) -> String {
    format!("(ScalarFunctionExprArgs {} {})", left, right)
}

fn fun_expr_args_empty_tail() -> String {
    fun_expr_args(Vec::<String>::new(), true)
}

fn scalar_fun_expr_args_legacy(left: impl Display, right: impl Display) -> String {
    format!("(ScalarFunctionExprArgs {} {})", left, right)
}

fn scalar_fun_expr_args_empty_tail() -> String {
    fun_expr_args_empty_tail()
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

fn window_fun_expr_var_arg(
    fun_name: impl Display,
    arg_list: impl Display,
    partition_by: impl Display,
    order_by: impl Display,
    window_frame: impl Display,
) -> String {
    format!(
        "(WindowFunctionExpr {} {} {} {} {})",
        fun_name, arg_list, partition_by, order_by, window_frame
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

fn window(input: impl Display, window_expr: impl Display) -> String {
    format!("(Window {} {})", input, window_expr)
}

fn window_window_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("WindowWindowExpr", exprs, true)
}

fn window_window_expr_empty_tail() -> String {
    window_window_expr(Vec::<String>::new())
}

fn empty_relation(
    produce_one_row: impl Display,
    derived_source_table_name: impl Display,
    is_wrappable: impl Display,
) -> String {
    format!(
        "(EmptyRelation {} {} {})",
        produce_one_row, derived_source_table_name, is_wrappable,
    )
}

fn wrapped_select(
    select_type: impl Display,
    projection_expr: impl Display,
    subqueries: impl Display,
    group_expr: impl Display,
    aggr_expr: impl Display,
    window_expr: impl Display,
    from: impl Display,
    joins: impl Display,
    filter_expr: impl Display,
    having_expr: impl Display,
    limit: impl Display,
    offset: impl Display,
    order_expr: impl Display,
    alias: impl Display,
    distinct: impl Display,
    ungrouped: impl Display,
    ungrouped_scan: impl Display,
) -> String {
    format!(
        "(WrappedSelect {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {})",
        select_type,
        projection_expr,
        subqueries,
        group_expr,
        aggr_expr,
        window_expr,
        from,
        joins,
        filter_expr,
        having_expr,
        limit,
        offset,
        order_expr,
        alias,
        distinct,
        ungrouped,
        ungrouped_scan
    )
}

fn wrapped_select_projection_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("WrappedSelectProjectionExpr", exprs, true)
}

fn wrapped_select_projection_expr_empty_tail() -> String {
    wrapped_select_projection_expr(Vec::<String>::new())
}

fn wrapped_select_subqueries_empty_tail() -> String {
    "WrappedSelectSubqueries".to_string()
}

fn wrapped_select_group_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("WrappedSelectGroupExpr", exprs, true)
}

fn wrapped_select_group_expr_empty_tail() -> String {
    wrapped_select_group_expr(Vec::<String>::new())
}

fn wrapped_select_aggr_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("WrappedSelectAggrExpr", exprs, true)
}

fn wrapped_select_aggr_expr_empty_tail() -> String {
    wrapped_select_aggr_expr(Vec::<String>::new())
}

fn wrapped_select_window_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("WrappedSelectWindowExpr", exprs, true)
}

fn wrapped_select_window_expr_empty_tail() -> String {
    wrapped_select_window_expr(Vec::<String>::new())
}

#[allow(dead_code)]
fn wrapped_select_joins(left: impl Display, right: impl Display) -> String {
    format!("(WrappedSelectJoins {} {})", left, right)
}

#[allow(dead_code)]
fn wrapped_select_joins_empty_tail() -> String {
    "WrappedSelectJoins".to_string()
}

fn wrapped_select_filter_expr(left: impl Display, right: impl Display) -> String {
    format!("(WrappedSelectFilterExpr {} {})", left, right)
}

#[allow(dead_code)]
fn wrapped_select_filter_expr_empty_tail() -> String {
    "WrappedSelectFilterExpr".to_string()
}

#[allow(dead_code)]
fn wrapped_select_having_expr(left: impl Display, right: impl Display) -> String {
    format!("(WrappedSelectHavingExpr {} {})", left, right)
}

fn wrapped_select_having_expr_empty_tail() -> String {
    "WrappedSelectHavingExpr".to_string()
}

#[allow(dead_code)]
fn wrapped_select_order_expr(left: impl Display, right: impl Display) -> String {
    format!("(WrappedSelectOrderExpr {} {})", left, right)
}

fn wrapped_select_order_expr_empty_tail() -> String {
    "WrappedSelectOrderExpr".to_string()
}

fn aggregate(
    input: impl Display,
    group: impl Display,
    aggr: impl Display,
    split: impl Display,
) -> String {
    format!("(Aggregate {} {} {} {})", input, group, aggr, split)
}

fn aggr_group_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("AggregateGroupExpr", exprs, true)
}

fn aggr_group_expr_empty_tail() -> String {
    aggr_group_expr(Vec::<String>::new())
}

fn aggr_group_expr_legacy(left: impl Display, right: impl Display) -> String {
    format!("(AggregateGroupExpr {} {})", left, right)
}

fn aggr_aggr_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("AggregateAggrExpr", exprs, true)
}

fn aggr_aggr_expr_empty_tail() -> String {
    aggr_aggr_expr(Vec::<String>::new())
}

fn grouping_set_expr(members: impl Display, expr_type: impl Display) -> String {
    format!("(GroupingSetExpr {} {})", members, expr_type)
}

fn grouping_set_expr_members_empty_tail() -> String {
    format!("GroupingSetExprMembers")
}

fn aggr_aggr_expr_legacy(left: impl Display, right: impl Display) -> String {
    format!("(AggregateAggrExpr {} {})", left, right)
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

fn to_day_interval_expr<D: Display>(period: D, unit: D, is_flat: bool) -> String {
    fun_expr("ToDayInterval", vec![period, unit], is_flat)
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

fn insubquery_expr(expr: impl Display, subquery: impl Display, negated: impl Display) -> String {
    format!("(InSubqueryExpr {} {} {})", expr, subquery, negated)
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

fn projection_expr(exprs: Vec<impl Display>) -> String {
    flat_list_expr("ProjectionExpr", exprs, true)
}

fn projection_expr_empty_tail() -> String {
    projection_expr(Vec::<String>::new())
}

fn projection_expr_legacy(left: impl Display, right: impl Display) -> String {
    format!("(ProjectionExpr {} {})", left, right)
}

fn sort(expr: impl Display, input: impl Display) -> String {
    format!("(Sort {} {})", expr, input)
}

fn filter(expr: impl Display, input: impl Display) -> String {
    format!("(Filter {} {})", expr, input)
}

fn subquery(input: impl Display, subqueries: impl Display, types: impl Display) -> String {
    format!("(Subquery {} {} {})", input, subqueries, types)
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

fn filter_simplify_replacer(members: impl Display) -> String {
    format!("(FilterSimplifyReplacer {})", members)
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

fn aggregate_split_pushdown_replacer(
    expr: impl Display,
    list_node: impl Display,
    alias_to_cube: impl Display,
) -> String {
    format!(
        "(AggregateSplitPushDownReplacer {} {} {})",
        expr, list_node, alias_to_cube
    )
}

fn aggregate_split_pullup_replacer(
    inner_expr: impl Display,
    outer_expr: impl Display,
    list_node: impl Display,
    alias_to_cube: impl Display,
) -> String {
    format!(
        "(AggregateSplitPullUpReplacer {} {} {} {})",
        inner_expr, outer_expr, list_node, alias_to_cube
    )
}

fn projection_split_pushdown_replacer(
    expr: impl Display,
    list_node: impl Display,
    alias_to_cube: impl Display,
) -> String {
    format!(
        "(ProjectionSplitPushDownReplacer {} {} {})",
        expr, list_node, alias_to_cube
    )
}

fn projection_split_pullup_replacer(
    inner_expr: impl Display,
    outer_expr: impl Display,
    list_node: impl Display,
    alias_to_cube: impl Display,
) -> String {
    format!(
        "(ProjectionSplitPullUpReplacer {} {} {} {})",
        inner_expr, outer_expr, list_node, alias_to_cube
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

fn wrapper_pushdown_replacer(
    members: impl Display,
    alias_to_cube: impl Display,
    ungrouped: impl Display,
    in_projection: impl Display,
    cube_members: impl Display,
) -> String {
    format!(
        "(WrapperPushdownReplacer {} {} {} {} {})",
        members, alias_to_cube, ungrouped, in_projection, cube_members
    )
}

fn wrapper_pullup_replacer(
    members: impl Display,
    alias_to_cube: impl Display,
    ungrouped: impl Display,
    in_projection: impl Display,
    cube_members: impl Display,
) -> String {
    format!(
        "(WrapperPullupReplacer {} {} {} {} {})",
        members, alias_to_cube, ungrouped, in_projection, cube_members
    )
}

fn flatten_pushdown_replacer(
    expr: impl Display,
    inner_expr: impl Display,
    inner_alias: impl Display,
    top_level: impl Display,
) -> String {
    format!(
        "(FlattenPushdownReplacer {} {} {} {})",
        expr, inner_expr, inner_alias, top_level,
    )
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
    split: impl Display,
    can_pushdown_join: impl Display,
    wrapped: impl Display,
    ungrouped: impl Display,
) -> String {
    format!(
        "(Extension (CubeScan {} {} {} {} {} {} {} {} {} {}))",
        alias_to_cube,
        members,
        filters,
        orders,
        limit,
        offset,
        split,
        can_pushdown_join,
        wrapped,
        ungrouped
    )
}

fn cube_scan_wrapper(input: impl Display, finalized: impl Display) -> String {
    format!("(CubeScanWrapper {} {})", input, finalized)
}

fn distinct(input: impl Display) -> String {
    format!("(Distinct {})", input)
}

pub fn original_expr_name(
    egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    id: Id,
) -> Option<String> {
    egraph[id]
        .data
        .original_expr
        .as_ref()
        .and_then(|e| match e {
            OriginalExpr::Expr(e) => Some(e),
            _ => None,
        })
        .map(|e| match e {
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

    fn search_eclass_with_limit(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
        limit: usize,
    ) -> Option<SearchMatches<LogicalPlanLanguage>> {
        if let Some(m) = self.main.search_eclass_with_limit(egraph, eclass, limit) {
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

pub fn transform_original_expr_to_alias(
    alias_expr_var: &'static str,
) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool {
    let alias_expr_var = var!(alias_expr_var);
    move |egraph, root, subst| add_root_original_expr_alias(egraph, root, subst, alias_expr_var)
}

pub fn add_root_original_expr_alias(
    egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    root: Id,
    subst: &mut Subst,
    alias_expr_var: Var,
) -> bool {
    if let Some(original_expr) = original_expr_name(egraph, root) {
        let alias = egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
            original_expr,
        )));
        subst.insert(alias_expr_var, alias);
        true
    } else {
        false
    }
}

pub fn extract_exprlist_from_groupping_set(exprs: &Vec<Expr>) -> Vec<Expr> {
    let mut result = Vec::new();
    for expr in exprs {
        match expr {
            Expr::GroupingSet(groupping_set) => match groupping_set {
                GroupingSet::Rollup(exprs) => result.extend(exprs.iter().cloned()),
                GroupingSet::Cube(exprs) => result.extend(exprs.iter().cloned()),
                GroupingSet::GroupingSets(sets) => {
                    result.extend(sets.iter().flat_map(|s| s.iter().cloned()))
                }
            },
            _ => result.push(expr.clone()),
        }
    }
    result
}
