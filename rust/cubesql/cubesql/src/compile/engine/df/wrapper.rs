use crate::{
    compile::{
        engine::{
            df::scan::{CubeScanNode, DataType, MemberField},
            udf::{MEASURE_UDAF_NAME, PATCH_MEASURE_UDAF_NAME},
        },
        rewrite::{
            extract_exprlist_from_groupping_set,
            rules::{
                filters::Decimal,
                utils::{granularity_str_to_int_order, DecomposedDayTime, DecomposedMonthDayNano},
            },
            LikeType, WrappedSelectType,
        },
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{
        AliasedColumn, DataSource, LoadRequestMeta, MetaContext, SpanId, SqlGenerator,
        SqlTemplates, TransportLoadRequestQuery, TransportService,
    },
    CubeError,
};
use chrono::{Days, NaiveDate, SecondsFormat, TimeZone, Utc};
use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryJoinSubquery};
use datafusion::logical_plan::{ExprVisitable, ExpressionVisitor, Recursion};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::Extension, replace_col, Column, DFSchema, DFSchemaRef, Expr, GroupingSet, JoinType,
        LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::{aggregates::AggregateFunction, functions::BuiltinScalarFunction},
    scalar::ScalarValue,
};
use futures::FutureExt;
use itertools::Itertools;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    cmp::min,
    collections::{hash_map::Entry, HashMap, HashSet},
    convert::TryInto,
    fmt,
    future::Future,
    iter,
    pin::Pin,
    result,
    sync::{Arc, LazyLock},
};

pub struct JoinSubquery {
    alias: String,
    sql: String,
    condition: Expr,
    join_type: JoinType,
}

pub struct PushToCubeContext<'l> {
    ungrouped_scan_node: &'l CubeScanNode,
    // Known join subquery qualifiers, to generate proper column expressions
    known_join_subqueries: HashSet<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqlQuery {
    pub sql: String,
    pub values: Vec<Option<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct SqlFunctionExpr {
    #[serde(rename = "cubeParams")]
    cube_params: Vec<String>,
    sql: String,
}

#[derive(Debug, Clone, Serialize)]
struct PatchMeasureDef {
    #[serde(rename = "sourceMeasure")]
    source_measure: String,
    #[serde(rename = "replaceAggregationType")]
    replace_aggregation_type: Option<String>,
    #[serde(rename = "addFilters")]
    add_filters: Vec<SqlFunctionExpr>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum UngroupedMemberExpr {
    SqlFunction(SqlFunctionExpr),
    PatchMeasure(PatchMeasureDef),
}

#[derive(Debug, Clone, Serialize)]
struct UngroupedMemberDef {
    #[serde(rename = "cubeName")]
    cube_name: String,
    alias: String,
    expr: UngroupedMemberExpr,
    #[serde(rename = "groupingSet")]
    grouping_set: Option<GroupingSetDesc>,
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub enum GroupingSetType {
    Rollup,
    Cube,
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct GroupingSetDesc {
    #[serde(rename = "groupType")]
    pub group_type: GroupingSetType,
    pub id: u64,
    #[serde(rename = "subId")]
    pub sub_id: Option<u64>,
}

impl GroupingSetDesc {
    pub fn new(group_type: GroupingSetType, id: u64) -> Self {
        Self {
            group_type,
            id,
            sub_id: None,
        }
    }
}

fn extract_group_type_from_groupping_set(
    exprs: &Vec<Expr>,
) -> Result<Vec<Option<GroupingSetDesc>>> {
    let mut result = Vec::new();
    let mut id = 0;
    for expr in exprs {
        match expr {
            Expr::GroupingSet(groupping_set) => match groupping_set {
                GroupingSet::Rollup(exprs) => {
                    result.extend(
                        iter::repeat(Some(GroupingSetDesc::new(GroupingSetType::Rollup, id)))
                            .take(exprs.len()),
                    );
                    id += 1;
                }
                GroupingSet::Cube(exprs) => {
                    result.extend(
                        iter::repeat(Some(GroupingSetDesc::new(GroupingSetType::Cube, id)))
                            .take(exprs.len()),
                    );
                    id += 1;
                }
                GroupingSet::GroupingSets(_) => {
                    return Err(DataFusionError::Internal(format!(
                        "SQL generation for GroupingSet is not supported"
                    )))
                }
            },
            _ => result.push(None),
        }
    }
    Ok(result)
}

impl SqlQuery {
    pub fn new(sql: String, values: Vec<Option<String>>) -> Self {
        Self { sql, values }
    }

    pub fn add_value(&mut self, value: Option<String>) -> usize {
        if let Some(index) = self.values.iter().position(|v| v == &value) {
            return index;
        }
        let index = self.values.len();
        self.values.push(value);
        index
    }

    pub fn extend_values(&mut self, values: impl IntoIterator<Item = Option<String>>) {
        self.values.extend(values);
    }

    pub fn replace_sql(&mut self, sql: String) {
        self.sql = sql;
    }

    pub fn unpack(self) -> (String, Vec<Option<String>>) {
        (self.sql, self.values)
    }

    fn render_param(
        &self,
        sql_templates: Arc<SqlTemplates>,
        param_index: Option<&str>,
        rendered_params: &HashMap<usize, String>,
        new_param_index: usize,
    ) -> Result<(usize, String, bool)> {
        let param = param_index
            .ok_or_else(|| DataFusionError::Execution("Missing param match".to_string()))?
            .parse::<usize>()
            .map_err(|e| DataFusionError::Execution(format!("Can't parse param index: {}", e)))?;
        if sql_templates.reuse_params {
            if let Some(rendered_param) = rendered_params.get(&param) {
                return Ok((param, rendered_param.clone(), false));
            }
        }
        Ok((
            param,
            sql_templates
                .param(new_param_index)
                .map_err(|e| DataFusionError::Execution(format!("Can't render param: {}", e)))?,
            true,
        ))
    }

    pub fn finalize_query(&mut self, sql_templates: Arc<SqlTemplates>) -> Result<()> {
        static REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\$(\d+)\$").unwrap());

        let mut params = Vec::new();
        let mut rendered_params = HashMap::new();
        let mut res = Ok(());
        let replaced_sql = REGEX.replace_all(self.sql.as_str(), |c: &Captures<'_>| {
            let param = c.get(1).map(|x| x.as_str());
            match self.render_param(sql_templates.clone(), param, &rendered_params, params.len()) {
                Ok((param_index, param, push_param)) => {
                    if push_param {
                        params.push(self.values[param_index].clone());
                        rendered_params.insert(param_index, param.clone());
                    }
                    param
                }
                Err(e) => {
                    res = Err(e);
                    "".to_string()
                }
            }
        });

        match res {
            Ok(()) => {
                self.sql = replaced_sql.to_string();
                self.values = params;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CubeScanWrappedSqlNode {
    // TODO maybe replace wrapped plan with schema + scan_node
    pub wrapped_plan: Arc<LogicalPlan>,
    pub wrapped_sql: SqlQuery,
    pub request: TransportLoadRequestQuery,
    pub member_fields: Vec<MemberField>,
}

impl CubeScanWrappedSqlNode {
    pub fn new(
        wrapped_plan: Arc<LogicalPlan>,
        wrapped_sql: SqlQuery,
        request: TransportLoadRequestQuery,
        member_fields: Vec<MemberField>,
    ) -> Self {
        Self {
            wrapped_plan,
            wrapped_sql,
            request,
            member_fields,
        }
    }
}

impl UserDefinedLogicalNode for CubeScanWrappedSqlNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.wrapped_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO figure out nice plan for wrapped plan
        write!(f, "CubeScanWrappedSql")
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_plan::Expr],
        inputs: &[datafusion::logical_plan::LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");

        Arc::new(CubeScanWrappedSqlNode {
            wrapped_plan: self.wrapped_plan.clone(),
            wrapped_sql: self.wrapped_sql.clone(),
            request: self.request.clone(),
            member_fields: self.member_fields.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CubeScanWrapperNode {
    pub wrapped_plan: Arc<LogicalPlan>,
    pub meta: Arc<MetaContext>,
    pub auth_context: AuthContextRef,
    pub span_id: Option<Arc<SpanId>>,
    pub config_obj: Arc<dyn ConfigObj>,
}

impl CubeScanWrapperNode {
    pub fn new(
        wrapped_plan: Arc<LogicalPlan>,
        meta: Arc<MetaContext>,
        auth_context: AuthContextRef,
        span_id: Option<Arc<SpanId>>,
        config_obj: Arc<dyn ConfigObj>,
    ) -> Self {
        Self {
            wrapped_plan,
            meta,
            auth_context,
            span_id,
            config_obj,
        }
    }
}

fn expr_name(e: &Expr, schema: &DFSchema) -> Result<String> {
    match e {
        Expr::Column(col) => Ok(col.name.clone()),
        Expr::Sort { expr, .. } => expr_name(expr, schema),
        _ => e.name(schema),
    }
}

/// Holds column remapping for generated SQL
/// Can be used to remap expression in logical plans on top,
/// and to generate mapping between schema and Cube load query in wrapper
#[derive(Debug)]
pub struct ColumnRemapping {
    column_remapping: HashMap<Column, Column>,
}

impl ColumnRemapping {
    /// Generate member_fields for CubeScanExecutionPlan, which contains SQL with this remapping.
    /// Cube will respond with aliases after remapping, which we must use to read response.
    /// Schema in DF will stay the same as before remapping.
    /// So result would have all aliases after remapping in order derived from `schema`.
    pub fn member_fields(&self, schema: &DFSchema) -> Vec<MemberField> {
        schema
            .fields()
            .iter()
            .map(|f| {
                MemberField::regular(
                    self.column_remapping
                        .get(&Column::from_name(f.name().to_string()))
                        .map(|x| x.name.to_string())
                        .unwrap_or(f.name().to_string()),
                )
            })
            .collect()
    }

    /// Replace every column expression in `expr` according to this remapping. Column expressions
    /// not present in `self` will stay the same.
    pub fn remap(&self, expr: &Expr) -> result::Result<Expr, CubeError> {
        replace_col(expr.clone(), &self.column_remapping.iter().collect())
            .map_err(|_| CubeError::internal(format!("Can't rename columns for expr: {expr:?}",)))
    }

    pub fn extend(&mut self, other: ColumnRemapping) {
        self.column_remapping.extend(other.column_remapping);
    }
}

/// Builds new column mapping
/// One remapper for one context: all unqualified columns with same name are assumed the same column
struct Remapper {
    from_alias: Option<String>,
    can_rename_columns: bool,
    remapping: HashMap<Column, Column>,
    used_targets: HashSet<String>,
}

impl Remapper {
    /// Constructs new Remapper
    /// `from_alias` would be used as qualifier after remapping
    /// When `can_rename_columns` is enabled, column names will be generated.
    /// When it's disabled, column names must stay the same.
    /// Column qualifiers can change in both cases.
    pub fn new(from_alias: Option<String>, can_rename_columns: bool) -> Self {
        Remapper {
            from_alias,
            can_rename_columns,

            remapping: HashMap::new(),
            used_targets: HashSet::new(),
        }
    }

    fn generate_new_alias(&self, start_from: String) -> String {
        static NON_ID_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"[^a-zA-Z0-9_]").unwrap());

        let alias_lower = start_from.clone().to_lowercase();
        let mut truncated_alias = if alias_lower != "__user" && alias_lower != "__cubejoinfield" {
            NON_ID_REGEX
                .replace_all(&alias_lower, "_")
                .trim_start_matches("_")
                .to_string()
        } else {
            alias_lower
        };

        truncated_alias.truncate(16);
        let mut alias = truncated_alias.clone();
        for i in 1..10000 {
            if !self.used_targets.contains(&alias) {
                break;
            }
            alias = format!("{}_{}", truncated_alias, i);
        }
        alias
    }

    fn new_alias(
        &self,
        original_alias: &String,
        start_from: Option<String>,
    ) -> result::Result<String, CubeError> {
        let alias = if self.can_rename_columns {
            self.generate_new_alias(start_from.unwrap_or_else(|| original_alias.clone()))
        } else {
            original_alias.clone()
        };

        if self.used_targets.contains(&alias) {
            return Err(CubeError::internal(format!(
                "Can't generate SQL for column expr: duplicate alias {alias}"
            )));
        }

        Ok(alias)
    }

    fn insert_new_alias(&mut self, original_column: &Column, new_alias: &String) {
        let target_column = Column {
            name: new_alias.clone(),
            relation: self.from_alias.clone(),
        };

        self.used_targets.insert(new_alias.clone());
        self.remapping.insert(
            Column::from_name(&original_column.name),
            target_column.clone(),
        );
        if let Some(from_alias) = &self.from_alias {
            self.remapping.insert(
                Column {
                    name: original_column.name.clone(),
                    relation: Some(from_alias.clone()),
                },
                target_column.clone(),
            );
            if let Some(original_relation) = &original_column.relation {
                if original_relation != from_alias {
                    self.remapping
                        .insert(original_column.clone(), target_column);
                }
            }
        }
    }

    pub fn add_column(&mut self, column: &Column) -> result::Result<String, CubeError> {
        if let Some(alias_column) = self.remapping.get(column) {
            return Ok(alias_column.name.clone());
        }

        let new_alias = self.new_alias(&column.name, None)?;
        self.insert_new_alias(column, &new_alias);

        Ok(new_alias)
    }

    /// Generate new alias for expression
    /// `original_expr` is the one we are generating alias for
    /// `expr` can be same or modified, i.e. when previous column remapping is applied.
    /// `expr` would be used to generate new alias when `can_rename_columns` is enabled.
    /// When `original_expr` is column it would remap both unqualified and qualified colunms to new alias
    pub fn add_expr(
        &mut self,
        schema: &DFSchema,
        original_expr: &Expr,
        expr: &Expr,
    ) -> result::Result<String, CubeError> {
        let original_alias = expr_name(original_expr, schema)?;
        let original_alias_key = Column::from_name(&original_alias);
        if let Some(alias_column) = self.remapping.get(&original_alias_key) {
            return Ok(alias_column.name.clone());
        }

        let start_from = expr_name(&expr, &schema)?;
        let alias = self.new_alias(&original_alias, Some(start_from))?;

        let original_column = if let Expr::Column(column) = &original_expr {
            column
        } else {
            &Column::from_name(original_alias)
        };
        self.insert_new_alias(original_column, &alias);

        Ok(alias)
    }

    pub fn into_remapping(self) -> Option<ColumnRemapping> {
        if self.remapping.len() > 0 {
            Some(ColumnRemapping {
                column_remapping: self.remapping,
            })
        } else {
            None
        }
    }
}

pub struct SqlGenerationResult {
    pub data_source: Option<String>,
    pub from_alias: Option<String>,
    pub column_remapping: Option<ColumnRemapping>,
    pub sql: SqlQuery,
    pub request: TransportLoadRequestQuery,
}

static DATE_PART_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new("^[A-Za-z_ ]+$").unwrap());

macro_rules! generate_sql_for_timestamp {
    (@generic $literal:ident, $value:ident, $value_block:expr, $sql_generator:expr, $sql_query:expr) => {
        if let Some($value) = $value {
            let value = $value_block.to_rfc3339_opts(SecondsFormat::Millis, true);
            (
                $sql_generator
                    .get_sql_templates()
                    .timestamp_literal_expr(value)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for timestamp: {}",
                            e
                        ))
                    })?,
                $sql_query,
            )
        } else {
            (Self::generate_null_for_literal($sql_generator, &$literal)?, $sql_query)
        }
    };
    ($literal:ident, $value:ident, timestamp, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $literal, $value, { Utc.timestamp_opt($value as i64, 0).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($literal:ident, $value:ident, timestamp_millis_opt, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $literal, $value, { Utc.timestamp_millis_opt($value as i64).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($literal:ident, $value:ident, timestamp_micros, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $literal, $value, { Utc.timestamp_micros($value as i64).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($literal:ident, $value:ident, $method:ident, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $literal, $value, { Utc.$method($value as i64) }, $sql_generator, $sql_query
        )
    };
}

struct GeneratedColumns {
    projection: Vec<(AliasedColumn, HashSet<String>)>,
    group_by: Vec<(AliasedColumn, HashSet<String>)>,
    group_descs: Vec<Option<GroupingSetDesc>>,
    flat_group_expr: Vec<Expr>,
    aggregate: Vec<(AliasedColumn, HashSet<String>)>,
    patch_measures: Vec<(PatchMeasureDef, String, String)>,
    filter: Vec<(AliasedColumn, HashSet<String>)>,
    window: Vec<(AliasedColumn, HashSet<String>)>,
    order: Vec<(AliasedColumn, HashSet<String>)>,
}

impl CubeScanWrapperNode {
    pub fn has_ungrouped_scan(&self) -> bool {
        Self::has_ungrouped_wrapped_node(self.wrapped_plan.as_ref())
    }

    fn has_ungrouped_wrapped_node(node: &LogicalPlan) -> bool {
        match node {
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(cube_scan) = node.as_any().downcast_ref::<CubeScanNode>() {
                    cube_scan.request.ungrouped == Some(true)
                } else if let Some(wrapped_select) =
                    node.as_any().downcast_ref::<WrappedSelectNode>()
                {
                    // Don't really care if push-to-Cube or not, any aggregation should be ok here from execution perspective
                    if wrapped_select.select_type == WrappedSelectType::Aggregate {
                        false
                    } else {
                        Self::has_ungrouped_wrapped_node(wrapped_select.from.as_ref())
                            || wrapped_select
                                .joins
                                .iter()
                                .map(|(join, _, _)| join.as_ref())
                                .any(Self::has_ungrouped_wrapped_node)
                            || wrapped_select
                                .subqueries
                                .iter()
                                .map(|subq| subq.as_ref())
                                .any(Self::has_ungrouped_wrapped_node)
                    }
                } else {
                    false
                }
            }
            LogicalPlan::EmptyRelation(_) => false,
            // Everything else is unexpected actually
            _ => false,
        }
    }

    pub async fn generate_sql(
        &self,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
    ) -> result::Result<CubeScanWrappedSqlNode, CubeError> {
        let schema = self.schema();
        let wrapped_plan = self.wrapped_plan.clone();
        let (sql, request, member_fields) = Self::generate_sql_for_node(
            &self.meta,
            transport,
            load_request_meta,
            self.clone().set_max_limit_for_node(wrapped_plan),
            true,
            Vec::new(),
            None,
        )
        .await
        .and_then(|SqlGenerationResult { data_source, mut sql, request, column_remapping, .. }| -> result::Result<_, CubeError> {
            let member_fields = if let Some(column_remapping) = column_remapping {
                column_remapping.member_fields(schema)
            } else {
                schema
                    .fields()
                    .iter()
                    .map(|f| MemberField::regular(f.name().to_string()))
                    .collect()
            };
            let data_source = data_source.ok_or_else(|| CubeError::internal(format!(
                "Can't generate SQL for wrapped select: no data source returned"
            )))?;
            let sql_templates = self
                .meta
                .data_source_to_sql_generator
                .get(&data_source)
                .ok_or_else(|| {
                    CubeError::internal(format!(
                        "Can't generate SQL for wrapped select: no sql generator for '{:?}' data source",
                        data_source
                    ))
                })?
                .get_sql_templates();
            sql.finalize_query(sql_templates).map_err(|e| CubeError::internal(e.to_string()))?;
            Ok((sql, request, member_fields))
        })?;
        Ok(CubeScanWrappedSqlNode::new(
            self.wrapped_plan.clone(),
            sql,
            request,
            member_fields,
        ))
    }

    pub fn set_max_limit_for_node(self, node: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        let stream_mode = self.config_obj.stream_mode();
        if stream_mode {
            return node;
        }

        let query_limit = self.config_obj.non_streaming_query_max_row_limit();
        match node.as_ref() {
            LogicalPlan::Extension(Extension {
                node: extension_node,
            }) => {
                let cube_scan_node = extension_node
                    .as_any()
                    .downcast_ref::<CubeScanNode>()
                    .cloned();
                let wrapped_select_node = extension_node
                    .as_any()
                    .downcast_ref::<WrappedSelectNode>()
                    .cloned();
                if let Some(node) = cube_scan_node {
                    let mut new_node = node.clone();
                    new_node.request.limit = Some(
                        new_node
                            .request
                            .limit
                            .map_or(query_limit, |limit| min(limit, query_limit)),
                    );
                    Arc::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(new_node),
                    }))
                } else if let Some(node) = wrapped_select_node {
                    let mut new_node = node.clone();
                    new_node.limit = Some(new_node.limit.map_or(query_limit as usize, |limit| {
                        min(limit, query_limit as usize)
                    }));
                    Arc::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(new_node),
                    }))
                } else {
                    node.clone()
                }
            }
            _ => node.clone(),
        }
    }

    fn data_source_for_cube_scan<'ctx>(
        meta: &'ctx MetaContext,
        node: &CubeScanNode,
    ) -> result::Result<DataSource<'ctx>, CubeError> {
        meta.data_source_for_member_names(
            node.member_fields
                .iter()
                .filter_map(|mem| match mem {
                    MemberField::Member(mem) => Some(mem),
                    MemberField::Literal(_) => None,
                })
                .map(|mem| mem.member.as_str()),
        )
        .map_err(|err| {
            CubeError::internal(format!(
                "Can't generate SQL for node; error: {err}; node: {node:?}"
            ))
        })
    }

    async fn generate_sql_for_cube_scan(
        meta: &MetaContext,
        node: &CubeScanNode,
        transport: &dyn TransportService,
        load_request_meta: &LoadRequestMeta,
    ) -> result::Result<SqlGenerationResult, CubeError> {
        let data_source =
            Self::data_source_for_cube_scan(meta, node)?.specific_or(CubeError::internal(
                format!("Can't generate SQL for CubeScan without specific data source: {node:?}"),
            ))?;

        let mut meta_with_user = load_request_meta.clone();
        meta_with_user.set_change_user(node.options.change_user.clone());

        // Single CubeScan can represent join of multiple table scans
        // Multiple table scans can have multiple different aliases
        // It means that column expressions on top of this node can have multiple different qualifiers
        // CubeScan can have only one alias, so we remap every column to use that alias

        // Columns in node.schema can have arbitrary names, assigned by DF
        // Stuff like `datetrunc(Utf8("month"), col)`
        // They can be very long, and contain unwanted character
        // So we rename them

        let from_alias = node
            .schema
            .fields()
            .iter()
            .next()
            .and_then(|f| f.qualifier().cloned());
        let mut remapper = Remapper::new(from_alias.clone(), true);
        let mut member_to_alias = HashMap::new();
        // Probably it should just use member expression for all MemberField::Literal
        // But turning literals to dimensions could mess up with NULL in grouping key and joins on Cube.js side (like in fullKeyQuery)
        // And tuning literals to measures would require ugly wrapping with noop aggregation function
        // TODO investigate Cube.js joins, try to implement dimension member expression
        let mut has_literal_members = false;
        let mut has_duplicated_members = false;
        let mut wrapper_exprs = vec![];

        for (member, field) in node.member_fields.iter().zip(node.schema.fields().iter()) {
            let alias = remapper.add_column(&field.qualified_column())?;
            let expr = match member {
                MemberField::Member(f) => {
                    let f = f.field_name.clone();
                    match member_to_alias.entry(f) {
                        Entry::Vacant(entry) => {
                            entry.insert(alias.clone());
                            // `alias` is column name that would be generated by Cube.js, just reference that
                            Expr::Column(Column::from_name(alias.clone()))
                        }
                        Entry::Occupied(entry) => {
                            // This member already has an alias, generate wrapper that would use it
                            has_duplicated_members = true;
                            Expr::Column(Column::from_name(entry.get().clone()))
                        }
                    }
                }
                MemberField::Literal(value) => {
                    has_literal_members = true;
                    // Don't care for `member_to_alias`, Cube.js does not handle literals
                    // Generate literal expression, and put alias into remapper to use higher up
                    Expr::Literal(value.clone())
                }
            };
            wrapper_exprs.push((expr, alias));
        }

        // This is SQL for CubeScan from Cube.js
        // It does have all the members with aliases from `member_to_alias`
        // But it does not have any literal members
        let sql = transport
            .sql(
                node.span_id.clone(),
                node.request.clone(),
                node.auth_context.clone(),
                meta_with_user,
                Some(member_to_alias),
                None,
            )
            .await?;

        // TODO is this check necessary?
        let sql = if has_literal_members || has_duplicated_members {
            // Need to generate wrapper SELECT with literal columns
            // Generated columns need to have same aliases as targets in `remapper`
            // Because that's what plans higher up would use in generated SQL
            let generator = meta
                .data_source_to_sql_generator
                .get(data_source)
                .ok_or_else(|| {
                    CubeError::internal(format!(
                        "Can't generate SQL for CubeScan: no SQL generator for data source {data_source:?}"
                    ))
                })?
                .clone();

            let mut columns = vec![];
            let mut new_sql = sql.sql;

            for (expr, alias) in wrapper_exprs {
                // Don't use `generate_column_expr` here
                // 1. `generate_column_expr` has different idea of literal members
                // When generating column expression that points to literal member it would render literal and generate alias
                // Here it should just generate the literal
                // 2. It would not allow to provide aliases for expressions, instead it usually generates them
                let (expr, sql) = WrappedSelectNode::generate_sql_for_expr(
                    new_sql,
                    generator.clone(),
                    expr,
                    None,
                    &HashMap::new(),
                )
                .await?;
                columns.push(AliasedColumn { expr, alias });
                new_sql = sql;
            }

            // Use SQL from Cube.js as FROM, and prepared expressions as projection
            let resulting_sql = generator
                .get_sql_templates()
                .select(
                    new_sql.sql.to_string(),
                    columns,
                    vec![],
                    vec![],
                    vec![],
                    // TODO
                    from_alias.clone().unwrap_or("".to_string()),
                    None,
                    None,
                    vec![],
                    None,
                    None,
                    false,
                )
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Can't generate SQL for CubeScan in wrapped select: {}",
                        e
                    ))
                })?;
            new_sql.replace_sql(resulting_sql);

            new_sql
        } else {
            sql.sql
        };

        let column_remapping = remapper.into_remapping();

        return Ok(SqlGenerationResult {
            data_source: Some(data_source.to_string()),
            from_alias,
            sql,
            column_remapping,
            request: node.request.clone(),
        });
    }

    pub async fn generate_sql_for_node(
        meta: &MetaContext,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: Arc<LogicalPlan>,
        can_rename_columns: bool,
        values: Vec<Option<String>>,
        parent_data_source: Option<&str>,
    ) -> result::Result<SqlGenerationResult, CubeError> {
        match node.as_ref() {
            // LogicalPlan::Projection(_) => {}
            // LogicalPlan::Filter(_) => {}
            // LogicalPlan::Window(_) => {}
            // LogicalPlan::Sort(_) => {}
            // LogicalPlan::Join(_) => {}
            // LogicalPlan::CrossJoin(_) => {}
            // LogicalPlan::Repartition(_) => {}
            // LogicalPlan::Union(_) => {}
            // LogicalPlan::TableScan(_) => {}
            // LogicalPlan::EmptyRelation(_) => {}
            // LogicalPlan::Limit(_) => {}
            // LogicalPlan::Subquery(_) => {}
            // LogicalPlan::CreateExternalTable(_) => {}
            // LogicalPlan::CreateMemoryTable(_) => {}
            // LogicalPlan::CreateCatalogSchema(_) => {}
            // LogicalPlan::DropTable(_) => {}
            // LogicalPlan::Values(_) => {}
            // LogicalPlan::Explain(_) => {}
            // LogicalPlan::Analyze(_) => {}
            // LogicalPlan::TableUDFs(_) => {}
            LogicalPlan::Extension(Extension { node }) => {
                let node_any = node.as_any();
                if let Some(node) = node_any.downcast_ref::<CubeScanNode>() {
                    Self::generate_sql_for_cube_scan(
                        meta,
                        node,
                        transport.as_ref(),
                        &load_request_meta,
                    )
                    .await
                } else if let Some(wrapped_select_node) =
                    node_any.downcast_ref::<WrappedSelectNode>()
                {
                    wrapped_select_node
                        .generate_sql(
                            meta,
                            transport,
                            load_request_meta,
                            node,
                            can_rename_columns,
                            values,
                            parent_data_source,
                        )
                        .await
                } else {
                    return Err(CubeError::internal(format!(
                        "Can't generate SQL for node: {node:?}"
                    )));
                }
            }
            LogicalPlan::EmptyRelation(_) => Ok(SqlGenerationResult {
                data_source: parent_data_source.map(|ds| ds.to_string()),
                from_alias: None,
                sql: SqlQuery::new("".to_string(), values.clone()),
                column_remapping: None,
                request: TransportLoadRequestQuery::new(),
            }),
            // LogicalPlan::Distinct(_) => {}
            x => {
                return Err(CubeError::internal(format!(
                    "Can't generate SQL for node: {:?}",
                    x
                )))
            }
        }
    }

    fn generate_sql_for_node_rec<'ctx>(
        meta: &'ctx MetaContext,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: Arc<LogicalPlan>,
        can_rename_columns: bool,
        values: Vec<Option<String>>,
        parent_data_source: Option<&'ctx str>,
    ) -> Pin<Box<dyn Future<Output = result::Result<SqlGenerationResult, CubeError>> + Send + 'ctx>>
    {
        Self::generate_sql_for_node(
            meta,
            transport,
            load_request_meta,
            node,
            can_rename_columns,
            values,
            parent_data_source,
        )
        .boxed()
    }
}

impl UserDefinedLogicalNode for CubeScanWrapperNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.wrapped_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO figure out nice plan for wrapped plan
        write!(f, "CubeScanWrapper")
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_plan::Expr],
        inputs: &[datafusion::logical_plan::LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");

        Arc::new(CubeScanWrapperNode {
            wrapped_plan: self.wrapped_plan.clone(),
            meta: self.meta.clone(),
            auth_context: self.auth_context.clone(),
            span_id: self.span_id.clone(),
            config_obj: self.config_obj.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WrappedSelectNode {
    pub schema: DFSchemaRef,
    pub select_type: WrappedSelectType,
    pub projection_expr: Vec<Expr>,
    pub subqueries: Vec<Arc<LogicalPlan>>,
    pub group_expr: Vec<Expr>,
    pub aggr_expr: Vec<Expr>,
    pub window_expr: Vec<Expr>,
    pub from: Arc<LogicalPlan>,
    pub joins: Vec<(Arc<LogicalPlan>, Expr, JoinType)>,
    pub filter_expr: Vec<Expr>,
    pub having_expr: Vec<Expr>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order_expr: Vec<Expr>,
    pub alias: Option<String>,
    pub distinct: bool,

    /// States if this node actually a query to Cube or not.
    /// When `false` this node will generate SQL on its own, using its fields and templates.
    /// When `true` this node will generate SQL with load query to JS side of Cube.
    /// It expects to be flattened: `from` is expected to be ungrouped CubeScan.
    /// There's no point in doing this for grouped CubeScan, we can just use load query from that CubeScan and SQL API generation on top.
    /// Load query generated for this case can be grouped when this node is an aggregation.
    /// Most fields will be rendered as a member expressions in generated load query.
    pub push_to_cube: bool,
}

impl WrappedSelectNode {
    pub fn new(
        schema: DFSchemaRef,
        select_type: WrappedSelectType,
        projection_expr: Vec<Expr>,
        subqueries: Vec<Arc<LogicalPlan>>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        window_expr: Vec<Expr>,
        from: Arc<LogicalPlan>,
        joins: Vec<(Arc<LogicalPlan>, Expr, JoinType)>,
        filter_expr: Vec<Expr>,
        having_expr: Vec<Expr>,
        limit: Option<usize>,
        offset: Option<usize>,
        order_expr: Vec<Expr>,
        alias: Option<String>,
        distinct: bool,
        push_to_cube: bool,
    ) -> Self {
        Self {
            schema,
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
            push_to_cube,
        }
    }

    fn subqueries_names(&self) -> result::Result<HashSet<String>, CubeError> {
        let mut subqueries_names = HashSet::new();
        for subquery in self.subqueries.iter() {
            // TODO why only field 0 is a key?
            let field = subquery.schema().field(0);
            subqueries_names.insert(field.qualified_name());
        }
        Ok(subqueries_names)
    }

    async fn prepare_subqueries_sql(
        &self,
        meta: &MetaContext,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        sql: &mut SqlQuery,
        data_source: Option<&str>,
    ) -> result::Result<HashMap<String, String>, CubeError> {
        let mut subqueries_sql = HashMap::new();
        for subquery in self.subqueries.iter() {
            let SqlGenerationResult {
                data_source: _,
                from_alias: _,
                column_remapping: _,
                sql: subquery_sql,
                request: _,
            } = CubeScanWrapperNode::generate_sql_for_node_rec(
                meta,
                transport.clone(),
                load_request_meta.clone(),
                subquery.clone(),
                true,
                sql.values.clone(),
                data_source,
            )
            .await?;

            let (sql_string, new_values) = subquery_sql.unpack();
            sql.extend_values(new_values);
            // TODO why only field 0 is a key?
            let field = subquery.schema().field(0);
            subqueries_sql.insert(field.qualified_name(), sql_string);
        }
        Ok(subqueries_sql)
    }

    async fn get_patch_measure<'l>(
        sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: &'l Expr,
        push_to_cube_context: Option<&'l PushToCubeContext<'_>>,
        subqueries: &'l HashMap<String, String>,
    ) -> result::Result<(Option<(PatchMeasureDef, String)>, SqlQuery), CubeError> {
        match expr {
            Expr::Alias(inner, _alias) => {
                Self::get_patch_measure_rec(
                    sql_query,
                    sql_generator,
                    inner,
                    push_to_cube_context,
                    subqueries,
                )
                .await
            }
            Expr::AggregateUDF { fun, args } => {
                if fun.name != PATCH_MEASURE_UDAF_NAME {
                    return Ok((None, sql_query));
                }

                let Some(push_to_cube_context) = push_to_cube_context else {
                    return Err(CubeError::internal(format!(
                        "Unexpected UDAF expression without push-to-Cube context: {}",
                        fun.name
                    )));
                };

                let PushToCubeContext {
                    ungrouped_scan_node,
                    ..
                } = push_to_cube_context;

                let (measure, aggregation, filter) = match args.as_slice() {
                    [measure, aggregation, filter] => (measure, aggregation, filter),
                    _ => {
                        return Err(CubeError::internal(format!(
                            "Unexpected number arguments for UDAF: {}, {args:?}",
                            fun.name
                        )))
                    }
                };

                let Expr::Column(measure_column) = measure else {
                    return Err(CubeError::internal(format!(
                        "First argument should be column expression: {}",
                        fun.name
                    )));
                };

                let aggregation = match aggregation {
                    Expr::Literal(ScalarValue::Utf8(Some(aggregation))) => Some(aggregation),
                    Expr::Literal(ScalarValue::Null) => None,
                    _ => {
                        return Err(CubeError::internal(format!(
                            "Second argument should be Utf8 literal expression: {}",
                            fun.name
                        )));
                    }
                };

                let (filters, sql_query) = match filter {
                    Expr::Literal(ScalarValue::Null) => (vec![], sql_query),
                    _ => {
                        let used_members = collect_used_members(
                            filter,
                            push_to_cube_context,
                            // TODO avoid this alloc
                            &subqueries.keys().cloned().collect(),
                        )?;
                        let (filter, sql_query) = Self::generate_sql_for_expr(
                            sql_query,
                            sql_generator.clone(),
                            filter.clone(),
                            Some(push_to_cube_context),
                            subqueries,
                        )
                        .await?;

                        let used_cubes = Self::prepare_used_cubes(&used_members);

                        (
                            vec![SqlFunctionExpr {
                                cube_params: used_cubes,
                                sql: filter,
                            }],
                            sql_query,
                        )
                    }
                };

                let member =
                    Self::find_member_in_ungrouped_scan(ungrouped_scan_node, measure_column)?;

                let MemberField::Member(member) = member else {
                    return Err(CubeError::internal(format!(
                        "First argument should reference regular member, not literal: {}",
                        fun.name
                    )));
                };
                let member = &member.member;

                let (cube, _member) = member.split_once('.').ok_or_else(|| {
                    CubeError::internal(format!("Can't parse cube name from member {member}",))
                })?;

                Ok((
                    Some((
                        PatchMeasureDef {
                            source_measure: member.to_string(),
                            replace_aggregation_type: aggregation.cloned(),
                            add_filters: filters,
                        },
                        cube.to_string(),
                    )),
                    sql_query,
                ))
            }
            _ => Ok((None, sql_query)),
        }
    }

    fn get_patch_measure_rec<'l>(
        sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: &'l Expr,
        push_to_cube_context: Option<&'l PushToCubeContext<'_>>,
        subqueries: &'l HashMap<String, String>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = result::Result<
                        (Option<(PatchMeasureDef, String)>, SqlQuery),
                        CubeError,
                    >,
                > + Send
                + 'l,
        >,
    > {
        Self::get_patch_measure(
            sql_query,
            sql_generator,
            expr,
            push_to_cube_context,
            subqueries,
        )
        .boxed()
    }

    async fn extract_patch_measures(
        schema: &DFSchema,
        exprs: impl IntoIterator<Item = Expr>,
        mut sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        column_remapping: Option<&ColumnRemapping>,
        next_remapper: &mut Remapper,
        can_rename_columns: bool,
        push_to_cube_context: Option<&PushToCubeContext<'_>>,
        subqueries: &HashMap<String, String>,
    ) -> result::Result<(Vec<(PatchMeasureDef, String, String)>, Vec<Expr>, SqlQuery), CubeError>
    {
        let mut patches = vec![];
        let mut other = vec![];

        for original_expr in exprs {
            let (patch_def, sql_query_next) = Self::get_patch_measure(
                sql_query,
                sql_generator.clone(),
                &original_expr,
                push_to_cube_context,
                subqueries,
            )
            .await?;
            sql_query = sql_query_next;
            if let Some((patch_def, cube)) = patch_def {
                let (_expr, alias) = Self::remap_column_expression(
                    schema,
                    &original_expr,
                    column_remapping,
                    next_remapper,
                    can_rename_columns,
                )?;

                patches.push((patch_def, cube, alias));
            } else {
                other.push(original_expr);
            }
        }

        Ok((patches, other, sql_query))
    }

    async fn generate_columns(
        &self,
        meta: &MetaContext,
        node: &Arc<dyn UserDefinedLogicalNode + Send + Sync>,
        can_rename_columns: bool,
        sql: SqlQuery,
        data_source: Option<&str>,
        push_to_cube_context: Option<&PushToCubeContext<'_>>,
        subqueries_sql: &HashMap<String, String>,
        column_remapping: Option<&ColumnRemapping>,
        alias: Option<String>,
    ) -> result::Result<
        (
            Arc<dyn SqlGenerator + Send + Sync>,
            GeneratedColumns,
            SqlQuery,
            Remapper,
        ),
        CubeError,
    > {
        let mut next_remapper = Remapper::new(alias, can_rename_columns);

        let schema = &self.schema;

        let Some(data_source) = data_source else {
            return Err(CubeError::internal(format!(
                "Can't generate SQL for wrapped select: no data source for {:?}",
                node
            )));
        };

        let generator = meta
            .data_source_to_sql_generator
            .get(data_source)
            .ok_or_else(|| {
                CubeError::internal(format!(
                    "Can't generate SQL for wrapped select: no sql generator for {:?}",
                    node
                ))
            })?
            .clone();
        let (projection, sql) = Self::generate_column_expr(
            schema.clone(),
            self.projection_expr.iter().cloned(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;
        let flat_group_expr = extract_exprlist_from_groupping_set(&self.group_expr);
        let (group_by, sql) = Self::generate_column_expr(
            schema.clone(),
            flat_group_expr.clone(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;
        let group_descs = extract_group_type_from_groupping_set(&self.group_expr)?;

        let (patch_measures, aggr_expr, sql) = Self::extract_patch_measures(
            schema.as_ref(),
            self.aggr_expr.iter().cloned(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;

        let (aggregate, sql) = Self::generate_column_expr(
            schema.clone(),
            aggr_expr.clone(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;

        let (filter, sql) = Self::generate_column_expr(
            schema.clone(),
            self.filter_expr.iter().cloned(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;

        let (window, sql) = Self::generate_column_expr(
            schema.clone(),
            self.window_expr.iter().cloned(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;

        let (order, sql) = Self::generate_column_expr(
            schema.clone(),
            self.order_expr.iter().cloned(),
            sql,
            generator.clone(),
            column_remapping,
            &mut next_remapper,
            can_rename_columns,
            push_to_cube_context,
            subqueries_sql,
        )
        .await?;

        Ok((
            generator,
            GeneratedColumns {
                projection,
                group_by,
                group_descs,
                flat_group_expr,
                aggregate,
                patch_measures,
                filter,
                window,
                order,
            },
            sql,
            next_remapper,
        ))
    }

    fn prepare_used_cubes<'m>(used_members: impl IntoIterator<Item = &'m String>) -> Vec<String> {
        used_members
            .into_iter()
            .flat_map(|member| member.split_once('.'))
            .map(|(cube, _rest)| cube)
            .unique()
            .map(|cube| cube.to_string())
            .collect::<Vec<_>>()
    }

    fn make_member_def<'m>(
        column: &AliasedColumn,
        used_members: impl IntoIterator<Item = &'m String>,
        ungrouped_scan_cubes: &Vec<String>,
    ) -> Result<UngroupedMemberDef> {
        let used_cubes = Self::prepare_used_cubes(used_members);
        let cube_name = used_cubes
            .first()
            .or_else(|| ungrouped_scan_cubes.first())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Can't generate SQL for column without cubes: {:?}",
                    column
                ))
            })?
            .clone();

        let res = UngroupedMemberDef {
            cube_name,
            alias: column.alias.clone(),
            expr: UngroupedMemberExpr::SqlFunction(SqlFunctionExpr {
                cube_params: used_cubes,
                sql: column.expr.clone(),
            }),
            grouping_set: None,
        };
        Ok(res)
    }

    fn ungrouped_member_def<'m>(
        column: &AliasedColumn,
        used_members: impl IntoIterator<Item = &'m String>,
        ungrouped_scan_cubes: &Vec<String>,
    ) -> Result<String> {
        let res = Self::make_member_def(column, used_members, ungrouped_scan_cubes)?;
        Ok(serde_json::json!(res).to_string())
    }

    fn dimension_member_def<'m>(
        column: &AliasedColumn,
        used_members: impl IntoIterator<Item = &'m String>,
        ungrouped_scan_cubes: &Vec<String>,
        grouping_type: &Option<GroupingSetDesc>,
    ) -> Result<String> {
        let mut res = Self::make_member_def(column, used_members, ungrouped_scan_cubes)?;
        res.grouping_set = grouping_type.clone();
        Ok(serde_json::json!(res).to_string())
    }

    fn patch_measure_expr(
        def: PatchMeasureDef,
        cube_name: String,
        alias: String,
    ) -> Result<String> {
        let res = UngroupedMemberDef {
            cube_name,
            alias,
            expr: UngroupedMemberExpr::PatchMeasure(def),
            grouping_set: None,
        };

        Ok(serde_json::json!(res).to_string())
    }

    fn remap_column_expression(
        schema: &DFSchema,
        original_expr: &Expr,
        column_remapping: Option<&ColumnRemapping>,
        next_remapper: &mut Remapper,
        can_rename_columns: bool,
    ) -> result::Result<(Expr, String), CubeError> {
        let expr = if let Some(column_remapping) = column_remapping {
            let mut expr = column_remapping.remap(original_expr)?;
            if !can_rename_columns {
                let original_alias = expr_name(original_expr, &schema)?;
                if original_alias != expr_name(&expr, &schema)? {
                    expr = Expr::Alias(Box::new(expr), original_alias.clone());
                }
            }
            expr
        } else {
            original_expr.clone()
        };
        let alias = next_remapper.add_expr(&schema, original_expr, &expr)?;

        Ok((expr, alias))
    }

    async fn generate_column_expr(
        schema: DFSchemaRef,
        exprs: impl IntoIterator<Item = Expr>,
        mut sql: SqlQuery,
        generator: Arc<dyn SqlGenerator>,
        column_remapping: Option<&ColumnRemapping>,
        next_remapper: &mut Remapper,
        can_rename_columns: bool,
        push_to_cube_context: Option<&PushToCubeContext<'_>>,
        subqueries: &HashMap<String, String>,
    ) -> result::Result<(Vec<(AliasedColumn, HashSet<String>)>, SqlQuery), CubeError> {
        let mut aliased_columns = Vec::new();
        for original_expr in exprs {
            let (expr, alias) = Self::remap_column_expression(
                schema.as_ref(),
                &original_expr,
                column_remapping,
                next_remapper,
                can_rename_columns,
            )?;

            let used_members = match push_to_cube_context {
                Some(push_to_cube_context) => collect_used_members(
                    &expr,
                    push_to_cube_context,
                    // TODO avoid this alloc
                    &subqueries.keys().cloned().collect(),
                )?,
                None => HashSet::new(),
            };
            let (expr_sql, new_sql_query) = Self::generate_sql_for_expr(
                sql,
                generator.clone(),
                expr.clone(),
                push_to_cube_context,
                subqueries,
            )
            .await?;
            let expr_sql =
                Self::escape_interpolation_quotes(expr_sql, push_to_cube_context.is_some());
            sql = new_sql_query;

            aliased_columns.push((
                AliasedColumn {
                    expr: expr_sql,
                    alias,
                },
                used_members,
            ));
        }
        Ok((aliased_columns, sql))
    }

    fn generate_sql_cast_expr(
        sql_generator: Arc<dyn SqlGenerator>,
        inner_expr: String,
        data_type: String,
    ) -> result::Result<String, DataFusionError> {
        sql_generator
            .get_sql_templates()
            .cast_expr(inner_expr, data_type)
            .map_err(|e| DataFusionError::Internal(format!("Can't generate SQL for cast: {}", e)))
    }

    fn generate_sql_type(
        sql_generator: Arc<dyn SqlGenerator>,
        data_type: DataType,
    ) -> result::Result<String, DataFusionError> {
        sql_generator
            .get_sql_templates()
            .sql_type(data_type)
            .map_err(|e| DataFusionError::Internal(format!("Can't generate SQL for type: {}", e)))
    }

    fn generate_typed_null(
        sql_generator: Arc<dyn SqlGenerator>,
        data_type: Option<DataType>,
    ) -> result::Result<String, DataFusionError> {
        let Some(data_type) = data_type else {
            return Ok("NULL".to_string());
        };

        let sql_type = Self::generate_sql_type(sql_generator.clone(), data_type)?;
        let result = Self::generate_sql_cast_expr(sql_generator, "NULL".to_string(), sql_type)?;
        Ok(result)
    }

    fn generate_null_for_literal(
        sql_generator: Arc<dyn SqlGenerator>,
        value: &ScalarValue,
    ) -> result::Result<String, DataFusionError> {
        let data_type = value.get_datatype();
        Self::generate_typed_null(sql_generator, Some(data_type))
    }

    /// This function is async to be able to call to JS land,
    /// in case some SQL generation could not be done through Jinja
    pub async fn generate_sql_for_expr<'ctx>(
        mut sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: Expr,
        push_to_cube_context: Option<&'ctx PushToCubeContext<'ctx>>,
        subqueries: &HashMap<String, String>,
    ) -> Result<(String, SqlQuery)> {
        match expr {
            Expr::Alias(expr, _) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                Ok((expr, sql_query))
            }
            // Expr::OuterColumn(_, _) => {}
            Expr::Column(ref c) => {
                if let Some(subquery) = subqueries.get(&c.flat_name()) {
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .subquery_expr(subquery.clone())
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for subquery expr: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                } else if let Some(PushToCubeContext {
                    ungrouped_scan_node,
                    known_join_subqueries,
                }) = push_to_cube_context
                {
                    if let Some(relation) = c.relation.as_ref() {
                        if known_join_subqueries.contains(relation) {
                            // SQL API passes fixed aliases to Cube.js for join subqueries
                            // It means we don't need to use member expressions here, and can just use that fixed alias
                            // So we can generate that as if it were regular column expression

                            return Self::generate_sql_for_expr_rec(
                                sql_query,
                                sql_generator.clone(),
                                expr,
                                None,
                                subqueries,
                            )
                            .await;
                        }
                    }

                    let member = Self::find_member_in_ungrouped_scan(ungrouped_scan_node, c)?;

                    match member {
                        MemberField::Member(member) => {
                            Ok((format!("${{{}}}", member.field_name), sql_query))
                        }
                        MemberField::Literal(value) => {
                            Self::generate_sql_for_expr_rec(
                                sql_query,
                                sql_generator.clone(),
                                Expr::Literal(value.clone()),
                                push_to_cube_context,
                                subqueries,
                            )
                            .await
                        }
                    }
                } else {
                    Ok((
                        match c.relation.as_ref() {
                            Some(r) => format!(
                                "{}.{}",
                                sql_generator
                                    .get_sql_templates()
                                    .quote_identifier(&r)
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for column: {}",
                                            e
                                        ))
                                    })?,
                                sql_generator
                                    .get_sql_templates()
                                    .quote_identifier(&c.name)
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for column: {}",
                                            e
                                        ))
                                    })?
                            ),
                            None => sql_generator
                                .get_sql_templates()
                                .quote_identifier(&c.name)
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Can't generate SQL for column: {}",
                                        e
                                    ))
                                })?,
                        },
                        sql_query,
                    ))
                }
            }
            // Expr::ScalarVariable(_, _) => {}
            Expr::BinaryExpr { left, op, right } => {
                let (left, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *left,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (right, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *right,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .binary_expr(left, op.to_string(), right)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for binary expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            // Expr::AnyExpr { .. } => {}
            Expr::Like(like) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *like.expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (pattern, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *like.pattern,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (escape_char, sql_query) = match like.escape_char {
                    Some(escape_char) => {
                        let (escape_char, sql_query) = Self::generate_sql_for_expr_rec(
                            sql_query,
                            sql_generator.clone(),
                            Expr::Literal(ScalarValue::Utf8(Some(escape_char.to_string()))),
                            push_to_cube_context,
                            subqueries,
                        )
                        .await?;
                        (Some(escape_char), sql_query)
                    }
                    None => (None, sql_query),
                };
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .like_expr(LikeType::Like, expr, like.negated, pattern, escape_char)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for like expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::ILike(ilike) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *ilike.expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (pattern, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *ilike.pattern,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (escape_char, sql_query) = match ilike.escape_char {
                    Some(escape_char) => {
                        let (escape_char, sql_query) = Self::generate_sql_for_expr_rec(
                            sql_query,
                            sql_generator.clone(),
                            Expr::Literal(ScalarValue::Utf8(Some(escape_char.to_string()))),
                            push_to_cube_context,
                            subqueries,
                        )
                        .await?;
                        (Some(escape_char), sql_query)
                    }
                    None => (None, sql_query),
                };
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .like_expr(LikeType::ILike, expr, ilike.negated, pattern, escape_char)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for ilike expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            // Expr::SimilarTo(_) => {}
            Expr::Not(expr) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql =
                    sql_generator
                        .get_sql_templates()
                        .not_expr(expr)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for not expr: {}",
                                e
                            ))
                        })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::IsNotNull(expr) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .is_null_expr(expr, true)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for is not null expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::IsNull(expr) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .is_null_expr(expr, false)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for is null expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::Negative(expr) => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .negative_expr(expr)
                    .map_err(|e| {
                        DataFusionError::Internal(format!("Can't generate SQL for not expr: {}", e))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            // Expr::GetIndexedField { .. } => {}
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (low, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *low,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let (high, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *high,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .between_expr(expr, negated, low, high)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for between expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let expr = if let Some(expr) = expr {
                    let (expr, sql_query_next) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = sql_query_next;
                    Some(expr)
                } else {
                    None
                };
                let mut when_then_expr_sql = Vec::new();
                for (when, then) in when_then_expr {
                    let (when, sql_query_next) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        *when,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    let (then, sql_query_next) = Self::generate_sql_for_expr_rec(
                        sql_query_next,
                        sql_generator.clone(),
                        *then,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = sql_query_next;
                    when_then_expr_sql.push((when, then));
                }
                let else_expr = if let Some(else_expr) = else_expr {
                    let (else_expr, sql_query_next) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        *else_expr,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = sql_query_next;
                    Some(else_expr)
                } else {
                    None
                };
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .case(expr, when_then_expr_sql, else_expr)
                    .map_err(|e| {
                        DataFusionError::Internal(format!("Can't generate SQL for case: {}", e))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::Cast { expr, data_type } => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let data_type = Self::generate_sql_type(sql_generator.clone(), data_type)?;
                let resulting_sql = Self::generate_sql_cast_expr(sql_generator, expr, data_type)?;
                Ok((resulting_sql, sql_query))
            }
            // Expr::TryCast { .. } => {}
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let (expr, sql_query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .sort_expr(expr, asc, nulls_first)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for sort expr: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }

            // Expr::TableUDF { .. } => {}
            Expr::Literal(literal) => {
                Ok(match literal {
                    ScalarValue::Boolean(b) => (
                        b.map(|b| {
                            sql_generator
                                .get_sql_templates()
                                .literal_bool_expr(b)
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Can't generate SQL for literal bool: {}",
                                        e
                                    ))
                                })
                        })
                        .transpose()?
                        .map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Float32(f) => (
                        f.map(|f| format!("{f}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Float64(f) => (
                        f.map(|f| format!("{f}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Decimal128(x, precision, scale) => {
                        // In Postgres, NUMERIC or DECIMAL scale can be negative.  But it's unsigned, here.
                        let scale: usize = scale;

                        (
                            if let Some(x) = x {
                                let number = Decimal::format_string(x, scale);
                                let data_type = Self::generate_sql_type(
                                    sql_generator.clone(),
                                    DataType::Decimal(precision, scale),
                                )?;
                                Self::generate_sql_cast_expr(
                                    sql_generator,
                                    format!("'{}'", number),
                                    data_type,
                                )?
                            } else {
                                Self::generate_null_for_literal(sql_generator, &literal)?
                            },
                            sql_query,
                        )
                    }
                    ScalarValue::Int8(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Int16(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Int32(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Int64(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::UInt8(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::UInt16(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::UInt32(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::UInt64(x) => (
                        x.map(|x| format!("{x}")).map_or_else(
                            || Self::generate_null_for_literal(sql_generator, &literal),
                            Ok,
                        )?,
                        sql_query,
                    ),
                    ScalarValue::Utf8(x) => {
                        if x.is_some() {
                            let param_index = sql_query.add_value(x);
                            (format!("${}$", param_index), sql_query)
                        } else {
                            (
                                Self::generate_typed_null(sql_generator, Some(DataType::Utf8))?,
                                sql_query,
                            )
                        }
                    }
                    // ScalarValue::LargeUtf8(_) => {}
                    // ScalarValue::Binary(_) => {}
                    // ScalarValue::LargeBinary(_) => {}
                    // ScalarValue::List(_, _) => {}
                    ScalarValue::Date32(x) => {
                        if let Some(x) = x {
                            let days = Days::new(x.abs().try_into().unwrap());
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let new_date = if x < 0 {
                                epoch.checked_sub_days(days)
                            } else {
                                epoch.checked_add_days(days)
                            };
                            let Some(new_date) = new_date else {
                                return Err(DataFusionError::Internal(format!(
                                    "Can't generate SQL for date: day out of bounds ({})",
                                    x
                                )));
                            };
                            let formatted_date = new_date.format("%Y-%m-%d").to_string();
                            (
                                sql_generator
                                    .get_sql_templates()
                                    .scalar_function(
                                        "DATE".to_string(),
                                        vec![format!("'{}'", formatted_date)],
                                        None,
                                        None,
                                    )
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for date: {}",
                                            e
                                        ))
                                    })?,
                                sql_query,
                            )
                        } else {
                            (
                                Self::generate_null_for_literal(sql_generator, &literal)?,
                                sql_query,
                            )
                        }
                    }
                    // ScalarValue::Date64(_) => {}

                    // generate_sql_for_timestamp will call Utc constructors, so only support UTC zone for now
                    // DataFusion can return "UTC" for stuff like `NOW()` during constant folding
                    ScalarValue::TimestampSecond(s, ref tz)
                        if matches!(tz.as_deref(), None | Some("UTC")) =>
                    {
                        generate_sql_for_timestamp!(literal, s, timestamp, sql_generator, sql_query)
                    }
                    ScalarValue::TimestampMillisecond(ms, ref tz)
                        if matches!(tz.as_deref(), None | Some("UTC")) =>
                    {
                        generate_sql_for_timestamp!(
                            literal,
                            ms,
                            timestamp_millis_opt,
                            sql_generator,
                            sql_query
                        )
                    }
                    ScalarValue::TimestampMicrosecond(ms, ref tz)
                        if matches!(tz.as_deref(), None | Some("UTC")) =>
                    {
                        generate_sql_for_timestamp!(
                            literal,
                            ms,
                            timestamp_micros,
                            sql_generator,
                            sql_query
                        )
                    }
                    ScalarValue::TimestampNanosecond(nanoseconds, ref tz)
                        if matches!(tz.as_deref(), None | Some("UTC")) =>
                    {
                        generate_sql_for_timestamp!(
                            literal,
                            nanoseconds,
                            timestamp_nanos,
                            sql_generator,
                            sql_query
                        )
                    }
                    ScalarValue::IntervalYearMonth(x) => {
                        if let Some(x) = x {
                            let (num, date_part) = (x, "MONTH");
                            let interval = format!("{} {}", num, date_part);
                            (
                                sql_generator
                                    .get_sql_templates()
                                    .interval_any_expr(interval, num.into(), date_part)
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for interval: {}",
                                            e
                                        ))
                                    })?,
                                sql_query,
                            )
                        } else {
                            (
                                Self::generate_null_for_literal(sql_generator, &literal)?,
                                sql_query,
                            )
                        }
                    }
                    ScalarValue::IntervalDayTime(x) => {
                        if let Some(x) = x {
                            let templates = sql_generator.get_sql_templates();
                            let decomposed = DecomposedDayTime::from_raw_interval_value(x);
                            let generated_sql = decomposed.generate_interval_sql(&templates)?;
                            (generated_sql, sql_query)
                        } else {
                            (
                                Self::generate_null_for_literal(sql_generator, &literal)?,
                                sql_query,
                            )
                        }
                    }
                    ScalarValue::IntervalMonthDayNano(x) => {
                        if let Some(x) = x {
                            let templates = sql_generator.get_sql_templates();
                            let decomposed = DecomposedMonthDayNano::from_raw_interval_value(x);
                            let generated_sql = decomposed.generate_interval_sql(&templates)?;
                            (generated_sql, sql_query)
                        } else {
                            (
                                Self::generate_null_for_literal(sql_generator, &literal)?,
                                sql_query,
                            )
                        }
                    }
                    // ScalarValue::Struct(_, _) => {}
                    ScalarValue::Null => {
                        (Self::generate_typed_null(sql_generator, None)?, sql_query)
                    }
                    x => {
                        return Err(DataFusionError::Internal(format!(
                            "Can't generate SQL for literal: {:?}",
                            x
                        )));
                    }
                })
            }
            Expr::ScalarUDF { fun, args } => {
                let date_part_err = |dp| {
                    DataFusionError::Internal(format!(
                        "Can't generate SQL for scalar function: date part '{}' is not supported",
                        dp
                    ))
                };
                let date_part = match fun.name.as_str() {
                    "datediff" | "dateadd" => match &args[0] {
                        Expr::Literal(ScalarValue::Utf8(Some(date_part))) => {
                            // Security check to prevent SQL injection
                            if DATE_PART_REGEX.is_match(date_part) {
                                Ok(Some(date_part.to_string()))
                            } else {
                                Err(date_part_err(date_part.to_string()))
                            }
                        }
                        _ => Err(date_part_err(args[0].to_string())),
                    },
                    "date_add" => match &args[1] {
                        Expr::Literal(ScalarValue::IntervalDayTime(Some(interval))) => {
                            let days = (*interval >> 32) as i32;
                            let ms = (*interval & 0xFFFF_FFFF) as i32;

                            if days != 0 && ms == 0 {
                                Ok(Some("DAY".to_string()))
                            } else if ms != 0 && days == 0 {
                                Ok(Some("MILLISECOND".to_string()))
                            } else {
                                Err(DataFusionError::Internal(format!(
                                    "Unsupported mixed IntervalDayTime: days = {days}, ms = {ms}"
                                )))
                            }
                        }
                        Expr::Literal(ScalarValue::IntervalYearMonth(Some(_months))) => {
                            Ok(Some("MONTH".to_string()))
                        }
                        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(interval))) => {
                            let months = (interval >> 96) as i32;
                            let days = ((interval >> 64) & 0xFFFF_FFFF) as i32;
                            let nanos = *interval as i64;

                            if months != 0 && days == 0 && nanos == 0 {
                                Ok(Some("MONTH".to_string()))
                            } else if days != 0 && months == 0 && nanos == 0 {
                                Ok(Some("DAY".to_string()))
                            } else if nanos != 0 && months == 0 && days == 0 {
                                Ok(Some("NANOSECOND".to_string()))
                            } else {
                                Err(DataFusionError::Internal(format!(
                                    "Unsupported mixed IntervalMonthDayNano: months = {months}, days = {days}, nanos = {nanos}"
                                )))
                            }
                        }
                        _ => Err(date_part_err(args[1].to_string())),
                    },
                    _ => Ok(None),
                }?;
                let interval = match fun.name.as_str() {
                    "dateadd" => match &args[1] {
                        Expr::Literal(ScalarValue::Int64(Some(interval))) => {
                            Ok(Some(interval.to_string()))
                        }
                        _ => Err(DataFusionError::Internal(format!(
                            "Can't generate SQL for scalar function: interval must be Int64"
                        ))),
                    },
                    "date_add" => match &args[1] {
                        Expr::Literal(ScalarValue::IntervalDayTime(Some(interval))) => {
                            let days = (*interval >> 32) as i32;
                            let ms = (*interval & 0xFFFF_FFFF) as i32;

                            if days != 0 && ms == 0 {
                                Ok(Some(days.to_string()))
                            } else if ms != 0 && days == 0 {
                                Ok(Some(ms.to_string()))
                            } else {
                                Err(DataFusionError::Internal(format!(
                                    "Unsupported mixed IntervalDayTime: days = {days}, ms = {ms}"
                                )))
                            }
                        }
                        Expr::Literal(ScalarValue::IntervalYearMonth(Some(months))) => {
                            Ok(Some(months.to_string()))
                        }
                        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(interval))) => {
                            let months = (interval >> 96) as i32;
                            let days = ((interval >> 64) & 0xFFFF_FFFF) as i32;
                            let nanos = *interval as i64;

                            if months != 0 && days == 0 && nanos == 0 {
                                Ok(Some(months.to_string()))
                            } else if days != 0 && months == 0 && nanos == 0 {
                                Ok(Some(days.to_string()))
                            } else if nanos != 0 && months == 0 && days == 0 {
                                Ok(Some(nanos.to_string()))
                            } else {
                                Err(DataFusionError::Internal(format!(
                                    "Unsupported mixed IntervalMonthDayNano: months = {months}, days = {days}, nanos = {nanos}"
                                )))
                            }
                        }
                        _ => Err(date_part_err(args[1].to_string())),
                    },
                    _ => Ok(None),
                }?;
                let mut sql_args = Vec::new();
                for arg in args {
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        arg,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_args.push(sql);
                }
                Ok((
                    sql_generator
                        .get_sql_templates()
                        .scalar_function(fun.name.to_string(), sql_args, date_part, interval)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for scalar function: {}",
                                e
                            ))
                        })?,
                    sql_query,
                ))
            }
            Expr::ScalarFunction { fun, args } => {
                if args.len() == 2 {
                    if let (
                        BuiltinScalarFunction::DateTrunc,
                        Expr::Literal(ScalarValue::Utf8(Some(granularity))),
                        Expr::Column(column),
                        Some(PushToCubeContext {
                            ungrouped_scan_node,
                            known_join_subqueries,
                        }),
                    ) = (&fun, &args[0], &args[1], push_to_cube_context)
                    {
                        let granularity = granularity.to_ascii_lowercase();
                        // Security check to prevent SQL injection
                        if granularity_str_to_int_order(&granularity, Some(false)).is_some()
                            && subqueries.get(&column.flat_name()).is_none()
                            && !column
                                .relation
                                .as_ref()
                                .map(|relation| known_join_subqueries.contains(relation))
                                .unwrap_or(false)
                        {
                            if let Ok(MemberField::Member(regular_member)) =
                                Self::find_member_in_ungrouped_scan(ungrouped_scan_node, column)
                            {
                                // TODO: check if member is a time dimension
                                if let MemberField::Member(time_dimension_member) =
                                    MemberField::time_dimension(
                                        regular_member.member.clone(),
                                        granularity,
                                    )
                                {
                                    return Ok((
                                        format!("${{{}}}", time_dimension_member.field_name),
                                        sql_query,
                                    ));
                                }
                            }
                        }
                    }
                }
                if let BuiltinScalarFunction::DatePart = &fun {
                    if args.len() >= 2 {
                        match &args[0] {
                            Expr::Literal(ScalarValue::Utf8(Some(date_part))) => {
                                // Security check to prevent SQL injection
                                if !DATE_PART_REGEX.is_match(date_part) {
                                    return Err(DataFusionError::Internal(format!(
                                        "Can't generate SQL for scalar function: date part '{}' is not supported",
                                        date_part
                                    )));
                                }
                                let (arg_sql, query) = Self::generate_sql_for_expr_rec(
                                    sql_query,
                                    sql_generator.clone(),
                                    args[1].clone(),
                                    push_to_cube_context,
                                    subqueries,
                                )
                                .await?;
                                return Ok((
                                    sql_generator
                                        .get_sql_templates()
                                        .extract_expr(date_part.to_string(), arg_sql)
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Can't generate SQL for scalar function: {}",
                                                e
                                            ))
                                        })?,
                                    query,
                                ));
                            }
                            _ => {}
                        }
                    }
                }
                let date_part = if let BuiltinScalarFunction::DateTrunc = &fun {
                    match &args[0] {
                        Expr::Literal(ScalarValue::Utf8(Some(date_part))) => {
                            // Security check to prevent SQL injection
                            if DATE_PART_REGEX.is_match(date_part) {
                                Some(date_part.to_string())
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                let mut sql_args = Vec::new();
                for arg in args {
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        arg,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_args.push(sql);
                }
                Ok((
                    sql_generator
                        .get_sql_templates()
                        .scalar_function(fun.to_string(), sql_args, date_part, None)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for scalar function: {}",
                                e
                            ))
                        })?,
                    sql_query,
                ))
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
                within_group,
            } => {
                let mut sql_args = Vec::new();
                let mut sql_within_group = Vec::new();
                for arg in args {
                    if let AggregateFunction::Count = fun {
                        if !distinct {
                            if let Expr::Literal(_) = arg {
                                sql_args.push("*".to_string());
                                break;
                            }
                        }
                    }
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        arg,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_args.push(sql);
                }
                if let Some(within_group) = within_group {
                    for expr in within_group {
                        let (sql, query) = Self::generate_sql_for_expr_rec(
                            sql_query,
                            sql_generator.clone(),
                            expr,
                            push_to_cube_context,
                            subqueries,
                        )
                        .await?;
                        sql_query = query;
                        sql_within_group.push(sql);
                    }
                }
                Ok((
                    sql_generator
                        .get_sql_templates()
                        .aggregate_function(fun, sql_args, distinct, sql_within_group)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for aggregate function: {}",
                                e
                            ))
                        })?,
                    sql_query,
                ))
            }
            Expr::GroupingSet(grouping_set) => match grouping_set {
                datafusion::logical_plan::GroupingSet::Rollup(exprs) => {
                    let mut sql_exprs = Vec::new();
                    for expr in exprs {
                        let (sql, query) = Self::generate_sql_for_expr_rec(
                            sql_query,
                            sql_generator.clone(),
                            expr,
                            push_to_cube_context,
                            subqueries,
                        )
                        .await?;
                        sql_query = query;
                        sql_exprs.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .rollup_expr(sql_exprs)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for rollup expression: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                datafusion::logical_plan::GroupingSet::Cube(exprs) => {
                    let mut sql_exprs = Vec::new();
                    for expr in exprs {
                        let (sql, query) = Self::generate_sql_for_expr_rec(
                            sql_query,
                            sql_generator.clone(),
                            expr,
                            push_to_cube_context,
                            subqueries,
                        )
                        .await?;
                        sql_query = query;
                        sql_exprs.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .cube_expr(sql_exprs)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Can't generate SQL for rollup expression: {}",
                                    e
                                ))
                            })?,
                        sql_query,
                    ))
                }
                datafusion::logical_plan::GroupingSet::GroupingSets(_) => {
                    Err(DataFusionError::Internal(format!(
                        "SQL generation for GroupingSet is not supported"
                    )))
                }
            },

            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let mut sql_args = Vec::new();
                for arg in args {
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        arg,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_args.push(sql);
                }
                let mut sql_partition_by = Vec::new();
                for arg in partition_by {
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        arg,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_partition_by.push(sql);
                }
                let mut sql_order_by = Vec::new();
                for arg in order_by {
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        arg,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_order_by.push(sql);
                }
                let resulting_sql = sql_generator
                    .get_sql_templates()
                    .window_function_expr(
                        fun,
                        sql_args,
                        sql_partition_by,
                        sql_order_by,
                        window_frame,
                    )
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Can't generate SQL for window function: {}",
                            e
                        ))
                    })?;
                Ok((resulting_sql, sql_query))
            }
            Expr::AggregateUDF { ref fun, ref args } => {
                match fun.name.as_str() {
                    // TODO allow this only in agg expr
                    MEASURE_UDAF_NAME => {
                        let Some(PushToCubeContext {
                            ungrouped_scan_node,
                            ..
                        }) = push_to_cube_context
                        else {
                            return Err(DataFusionError::Internal(format!(
                                "Unexpected {} UDAF expression without push-to-Cube context: {expr}",
                                fun.name,
                            )));
                        };

                        let measure_column = match args.as_slice() {
                            [Expr::Column(measure_column)] => measure_column,
                            _ => {
                                return Err(DataFusionError::Internal(format!(
                                    "Unexpected arguments for {} UDAF: {expr}",
                                    fun.name,
                                )))
                            }
                        };

                        let member = Self::find_member_in_ungrouped_scan(
                            ungrouped_scan_node,
                            measure_column,
                        )?;

                        let MemberField::Member(member) = member else {
                            return Err(DataFusionError::Internal(format!(
                                "First argument for {} UDAF should reference regular member, not literal: {expr}",
                                fun.name,
                            )));
                        };

                        Ok((format!("${{{}}}", member.field_name), sql_query))
                    }
                    // There's no branch for PatchMeasure, because it should generate via different path
                    _ => Err(DataFusionError::Internal(format!(
                        "Can't generate SQL for UDAF: {}",
                        fun.name
                    ))),
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut sql_query = sql_query;
                let (sql_expr, query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                sql_query = query;
                let mut sql_in_exprs = Vec::new();
                for expr in list {
                    let (sql, query) = Self::generate_sql_for_expr_rec(
                        sql_query,
                        sql_generator.clone(),
                        expr,
                        push_to_cube_context,
                        subqueries,
                    )
                    .await?;
                    sql_query = query;
                    sql_in_exprs.push(sql);
                }
                Ok((
                    sql_generator
                        .get_sql_templates()
                        .in_list_expr(sql_expr, sql_in_exprs, negated)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for in list expr: {}",
                                e
                            ))
                        })?,
                    sql_query,
                ))
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let mut sql_query = sql_query;
                let (sql_expr, query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *expr,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                sql_query = query;
                let (subquery_sql, query) = Self::generate_sql_for_expr_rec(
                    sql_query,
                    sql_generator.clone(),
                    *subquery,
                    push_to_cube_context,
                    subqueries,
                )
                .await?;
                sql_query = query;

                Ok((
                    sql_generator
                        .get_sql_templates()
                        .in_subquery_expr(sql_expr, subquery_sql, negated)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for in subquery expr: {}",
                                e
                            ))
                        })?,
                    sql_query,
                ))
            }
            // Expr::Wildcard => {}
            // Expr::QualifiedWildcard { .. } => {}
            x => {
                return Err(DataFusionError::Internal(format!(
                    "SQL generation for expression is not supported: {:?}",
                    x
                )))
            }
        }
    }

    /// This function is async to be able to call to JS land,
    /// in case some SQL generation could not be done through Jinja
    fn generate_sql_for_expr_rec<'ctx>(
        sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: Expr,
        push_to_cube_context: Option<&'ctx PushToCubeContext>,
        subqueries: &'ctx HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = Result<(String, SqlQuery)>> + Send + 'ctx>> {
        Self::generate_sql_for_expr(
            sql_query,
            sql_generator,
            expr,
            push_to_cube_context,
            subqueries,
        )
        .boxed()
    }

    fn find_member_in_ungrouped_scan<'scan, 'col>(
        ungrouped_scan_node: &'scan CubeScanNode,
        column: &'col Column,
    ) -> Result<&'scan MemberField> {
        let (_field, member) = ungrouped_scan_node
            .schema
            .fields()
            .iter()
            .zip(ungrouped_scan_node.member_fields.iter())
            .find(|(f, _mf)| {
                f.name() == &column.name
                    && match column.relation.as_ref() {
                        Some(r) => Some(r) == f.qualifier(),
                        None => true,
                    }
            })
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Can't find member for column {column} in ungrouped scan node"
                ))
            })?;

        Ok(member)
    }

    fn escape_interpolation_quotes(s: String, ungrouped: bool) -> String {
        if ungrouped {
            s.replace("\\", "\\\\").replace("`", "\\`")
        } else {
            s
        }
    }

    async fn generate_sql_for_push_to_cube(
        &self,
        meta: &MetaContext,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: &Arc<dyn UserDefinedLogicalNode + Send + Sync>,
        can_rename_columns: bool,
        values: Vec<Option<String>>,
    ) -> result::Result<SqlGenerationResult, CubeError> {
        // TODO support ungrouped joins
        let ungrouped_scan_node = {
            let from = self.from.as_ref();
            let LogicalPlan::Extension(Extension { node }) = from else {
                return Err(CubeError::internal(format!(
                    "Expected CubeScan node in from for Push-to-cube but found: {from:?}"
                )));
            };
            let Some(cube_scan_node) = node.as_any().downcast_ref::<CubeScanNode>() else {
                return Err(CubeError::internal(format!(
                    "Expected CubeScan node in from for Push-to-cube but found: {from:?}"
                )));
            };
            if cube_scan_node.request.ungrouped != Some(true) {
                return Err(CubeError::internal(format!(
                    "Expected ungrouped CubeScan node in from for Push-to-cube but found: {cube_scan_node:?}"
                )));
            }
            cube_scan_node
        };

        let from_alias = ungrouped_scan_node
            .schema
            .fields()
            .iter()
            .next()
            .and_then(|f| f.qualifier().cloned());
        let mut column_remapping = None;
        let mut sql = SqlQuery::new("".to_string(), values.clone());

        let subqueries_names = self.subqueries_names()?;

        fn alias_for_join_subq(plan: &LogicalPlan) -> result::Result<&String, CubeError> {
            // TODO avoid using direct alias from schema, implement remapping for qualifiers instead
            plan.schema()
                .fields()
                .iter()
                .filter_map(|f| f.qualifier())
                .next()
                .ok_or_else(|| {
                    CubeError::internal(format!("Alias not found for join subquery {plan:?}"))
                })
        }

        let push_to_cube_context = {
            let mut known_join_subqueries = HashSet::new();
            for (lp, _cond, _join_type) in &self.joins {
                // TODO avoid using direct alias from schema, implement remapping for qualifiers instead
                known_join_subqueries.insert(alias_for_join_subq(lp)?.clone());
            }
            PushToCubeContext {
                ungrouped_scan_node,
                known_join_subqueries,
            }
        };

        // Turn to ref
        let push_to_cube_context = &push_to_cube_context;

        let data_source = {
            let mut every_used_member = HashSet::new();
            let every_expression = self
                .projection_expr
                .iter()
                .chain(self.group_expr.iter())
                .chain(self.aggr_expr.iter())
                .chain(self.filter_expr.iter())
                .chain(self.window_expr.iter())
                .chain(self.order_expr.iter())
                .chain(self.joins.iter().map(|(_plan, cond, _join_type)| cond));
            for expr in every_expression {
                collect_used_members_to_set(
                    expr,
                    push_to_cube_context,
                    &subqueries_names,
                    &mut every_used_member,
                )?;
            }

            meta.data_source_for_member_names(every_used_member.iter().map(|m| m.as_str()))
                .map_err(|err| {
                    CubeError::internal(format!("Could not determine data source: {err}"))
                })?
        };

        let data_source = data_source.specific_or(CubeError::internal(format!(
            "Can't generate SQL for push-to-Cube CubeScan without specific data source: {node:?}"
        )))?;

        let subqueries_sql = self
            .prepare_subqueries_sql(
                meta,
                transport.clone(),
                load_request_meta.clone(),
                &mut sql,
                Some(data_source),
            )
            .await?;
        let subqueries_sql = &subqueries_sql;
        let alias = self.alias.clone().or(from_alias.clone());

        let join_subqueries = {
            let mut join_subqueries = vec![];
            for (lp, cond, join_type) in &self.joins {
                match lp.as_ref() {
                    LogicalPlan::Extension(Extension { node }) => {
                        if let Some(join_cube_scan) = node.as_any().downcast_ref::<CubeScanNode>() {
                            if join_cube_scan.request.ungrouped == Some(true) {
                                return Err(CubeError::internal(format!(
                                    "Unsupported ungrouped CubeScan as join subquery: {join_cube_scan:?}"
                                )));
                            }
                        } else if let Some(wrapped_select) =
                            node.as_any().downcast_ref::<WrappedSelectNode>()
                        {
                            if wrapped_select.push_to_cube {
                                return Err(CubeError::internal(format!(
                                    "Unsupported push_to_cube WrappedSelect as join subquery: {wrapped_select:?}"
                                )));
                            }
                        } else {
                            // TODO support more grouped cases here
                            return Err(CubeError::internal(format!(
                                "Unsupported unknown extension as join subquery: {node:?}"
                            )));
                        }
                    }
                    _ => {
                        // TODO support more grouped cases here
                        return Err(CubeError::internal(format!(
                            "Unsupported logical plan node as join subquery: {lp:?}"
                        )));
                    }
                }

                match join_type {
                    JoinType::Inner | JoinType::Left => {
                        // Do nothing
                    }
                    _ => {
                        return Err(CubeError::internal(format!(
                            "Unsupported join type for join subquery: {join_type:?}"
                        )));
                    }
                }

                // TODO avoid using direct alias from schema, implement remapping for qualifiers instead
                let alias = alias_for_join_subq(lp)?;

                let subq_sql = CubeScanWrapperNode::generate_sql_for_node_rec(
                    meta,
                    transport.clone(),
                    load_request_meta.clone(),
                    lp.clone(),
                    true,
                    sql.values.clone(),
                    Some(data_source),
                )
                .await?;
                let (subq_sql_string, new_values) = subq_sql.sql.unpack();
                sql.extend_values(new_values);
                let subq_alias = subq_sql.from_alias;
                // Expect that subq_sql.column_remapping already incorporates subq_alias/
                // TODO does it?

                // TODO expect returned from_alias to be fine, but still need to remap it from original alias somewhere in generate_sql_for_node

                // grouped join subquery can have its columns remapped, and expressions current node can reference original columns
                column_remapping = {
                    match (column_remapping, subq_sql.column_remapping) {
                        (None, None) => None,
                        (None, Some(remapping)) | (Some(remapping), None) => Some(remapping),
                        (Some(mut left), Some(right)) => {
                            left.extend(right);
                            Some(left)
                        }
                    }
                };

                let mut alias = subq_alias.unwrap_or_else(|| alias.clone());
                if let Some(generator) = meta.data_source_to_sql_generator.get(data_source) {
                    alias = generator.get_sql_templates().quote_identifier(&alias)?;
                };
                join_subqueries.push(JoinSubquery {
                    // TODO what alias to actually use here? two more-or-less valid options: returned from generate_sql_for_node ot realiased from `alias`. Plain `alias` is incorrect here
                    alias,
                    sql: subq_sql_string,
                    condition: cond.clone(),
                    join_type: *join_type,
                });
            }

            join_subqueries
        };

        // Drop mut, turn to ref
        let column_remapping = column_remapping.as_ref();

        let (generator, columns, mut sql, mut next_remapper) = self
            .generate_columns(
                meta,
                node,
                can_rename_columns,
                sql,
                Some(data_source),
                Some(push_to_cube_context),
                subqueries_sql,
                column_remapping,
                alias.clone(),
            )
            .await?;

        let GeneratedColumns {
            projection,
            group_by,
            group_descs,
            flat_group_expr,
            aggregate,
            patch_measures,
            filter,
            window,
            order: _,
        } = columns;

        let PushToCubeContext {
            ungrouped_scan_node,
            known_join_subqueries: _,
        } = push_to_cube_context;
        let mut prepared_join_subqueries = vec![];
        for JoinSubquery {
            alias: subq_alias,
            sql: subq_sql,
            condition,
            join_type,
        } in join_subqueries
        {
            // Need to call generate_column_expr to apply column_remapping
            let (join_condition, new_sql) = Self::generate_column_expr(
                self.schema.clone(),
                [condition.clone()],
                sql,
                generator.clone(),
                column_remapping,
                &mut next_remapper,
                true,
                Some(push_to_cube_context),
                subqueries_sql,
            )
            .await?;

            let join_condition_members = &join_condition[0].1;
            let join_condition = join_condition[0].0.expr.clone();
            sql = new_sql;

            let join_sql_expression = {
                // TODO this is NOT a proper way to generate member expr here
                // TODO Do we even want a full-blown member expression here? or arguments + expr will be enough?
                let res = Self::make_member_def(
                    &AliasedColumn {
                        expr: join_condition,
                        alias: "__join__alias__unused".to_string(),
                    },
                    join_condition_members,
                    &ungrouped_scan_node.used_cubes,
                )?;
                serde_json::json!(res).to_string()
            };

            let join_type = match join_type {
                JoinType::Left => generator.get_sql_templates().left_join()?,
                JoinType::Inner => generator.get_sql_templates().inner_join()?,
                _ => {
                    return Err(CubeError::internal(format!(
                        "Unsupported join type for join subquery: {join_type:?}"
                    )))
                }
            };

            // for simple ungrouped-grouped joins everything should already be present in from
            // so we can just attach this join to the end, no need to look for a proper spot
            prepared_join_subqueries.push(V1LoadRequestQueryJoinSubquery {
                sql: subq_sql.clone(),
                on: join_sql_expression,
                join_type,
                alias: subq_alias.clone(),
            });
        }

        let load_request = &ungrouped_scan_node.request;

        let (dimensions_only_projection, projection_with_measures) = projection
            .iter()
            .partition::<Vec<_>, _>(|(_column, used_members)| {
                used_members
                    .iter()
                    .all(|member| meta.find_dimension_with_name(member).is_some())
            });

        let load_request = V1LoadRequestQuery {
            measures: Some(
                aggregate
                    .iter()
                    .map(|(m, used_members)| {
                        Self::ungrouped_member_def(m, used_members, &ungrouped_scan_node.used_cubes)
                    })
                    .chain(projection_with_measures.iter().map(|(m, used_members)| {
                        Self::ungrouped_member_def(m, used_members, &ungrouped_scan_node.used_cubes)
                    }))
                    .chain(window.iter().map(|(m, used_members)| {
                        Self::ungrouped_member_def(m, used_members, &ungrouped_scan_node.used_cubes)
                    }))
                    .chain(
                        patch_measures
                            .into_iter()
                            .map(|(def, cube, alias)| Self::patch_measure_expr(def, cube, alias)),
                    )
                    .collect::<Result<_>>()?,
            ),
            dimensions: Some(
                group_by
                    .iter()
                    .zip(group_descs.iter())
                    .map(|((m, used_members), t)| {
                        Self::dimension_member_def(
                            m,
                            used_members,
                            &ungrouped_scan_node.used_cubes,
                            t,
                        )
                    })
                    .chain(dimensions_only_projection.iter().map(|(m, used_members)| {
                        Self::ungrouped_member_def(m, used_members, &ungrouped_scan_node.used_cubes)
                    }))
                    .collect::<Result<_>>()?,
            ),
            segments: Some(
                filter
                    .iter()
                    .map(|(m, used_members)| {
                        Self::ungrouped_member_def(m, used_members, &ungrouped_scan_node.used_cubes)
                    })
                    .collect::<Result<_>>()?,
            ),
            order: if !self.order_expr.is_empty() {
                Some(
                    self.order_expr
                        .iter()
                        .map(|o| -> Result<_> { match o {
                            Expr::Sort {
                                expr,
                                asc,
                                ..
                            } => {
                                let col_name = expr_name(&expr, &self.schema)?;

                                let find_column = |exprs: &[Expr], columns: &[(AliasedColumn, HashSet<String>)]| -> Option<AliasedColumn> {
                                    exprs.iter().zip(columns.iter())
                                        .find(|(e, _c)| expr_name(e, &self.schema).map(|n| n == col_name).unwrap_or(false))
                                        .map(|(_e, c)| c.0.clone())
                                };

                                // TODO handle patch measures collection here
                                let aliased_column = find_column(&self.aggr_expr, &aggregate)
                                    .or_else(|| find_column(&self.projection_expr, &projection))
                                    .or_else(|| find_column(&flat_group_expr, &group_by))
                                    .ok_or_else(|| {
                                        DataFusionError::Execution(format!(
                                            "Can't find column {} in projection {:?} or aggregate {:?} or group {:?}",
                                            col_name,
                                            self.projection_expr,
                                            self.aggr_expr,
                                            flat_group_expr
                                        ))
                                    })?;
                                Ok(vec![
                                    aliased_column.alias,
                                    if *asc { "asc".to_string() } else { "desc".to_string() },
                                ])
                            }
                            _ => Err(DataFusionError::Execution(format!(
                                "Expected sort expression, found {:?}",
                                o
                            ))),
                        }})
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                load_request.order.clone()
            },
            ungrouped: if let WrappedSelectType::Projection = self.select_type {
                load_request.ungrouped
            } else {
                None
            },
            // TODO is it okay to just override limit?
            limit: if let Some(limit) = self.limit {
                Some(limit as i32)
            } else {
                load_request.limit
            },
            // TODO is it okay to just override offset?
            offset: if let Some(offset) = self.offset {
                Some(offset as i32)
            } else {
                load_request.offset
            },

            // Original scan node can already have consumed filters from Logical plan
            // It's incorrect to just throw them away
            filters: ungrouped_scan_node.request.filters.clone(),

            time_dimensions: load_request.time_dimensions.clone(),
            subquery_joins: (!prepared_join_subqueries.is_empty())
                .then_some(prepared_join_subqueries),

            join_hints: load_request.join_hints.clone(),
        };

        // TODO time dimensions, filters, segments

        let mut meta_with_user = load_request_meta.as_ref().clone();
        meta_with_user.set_change_user(ungrouped_scan_node.options.change_user.clone());
        let sql_response = transport
            .sql(
                ungrouped_scan_node.span_id.clone(),
                load_request.clone(),
                ungrouped_scan_node.auth_context.clone(),
                meta_with_user,
                // TODO use aliases or push everything through names?
                None,
                Some(sql.values.clone()),
            )
            .await?;

        Ok(SqlGenerationResult {
            data_source: Some(data_source.to_string()),
            from_alias: alias,
            sql: sql_response.sql,
            column_remapping: next_remapper.into_remapping(),
            request: load_request.clone(),
        })
    }

    async fn generate_sql(
        &self,
        meta: &MetaContext,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: &Arc<dyn UserDefinedLogicalNode + Send + Sync>,
        can_rename_columns: bool,
        values: Vec<Option<String>>,
        parent_data_source: Option<&str>,
    ) -> result::Result<SqlGenerationResult, CubeError> {
        if self.push_to_cube {
            return self
                .generate_sql_for_push_to_cube(
                    meta,
                    transport,
                    load_request_meta,
                    node,
                    can_rename_columns,
                    values,
                )
                .await;
        }

        let SqlGenerationResult {
            data_source,
            from_alias,
            column_remapping,
            mut sql,
            request,
        } = CubeScanWrapperNode::generate_sql_for_node_rec(
            meta,
            transport.clone(),
            load_request_meta.clone(),
            self.from.clone(),
            true,
            values.clone(),
            parent_data_source,
        )
        .await?;

        let subqueries_sql = self
            .prepare_subqueries_sql(
                meta,
                transport.clone(),
                load_request_meta.clone(),
                &mut sql,
                data_source.as_deref(),
            )
            .await?;
        let subqueries_sql = &subqueries_sql;
        let alias = self.alias.clone().or(from_alias.clone());

        // Drop mut, turn to ref
        let column_remapping = column_remapping.as_ref();

        let (generator, columns, mut sql, next_remapper) = self
            .generate_columns(
                meta,
                node,
                can_rename_columns,
                sql,
                data_source.as_deref(),
                None,
                subqueries_sql,
                column_remapping,
                alias.clone(),
            )
            .await?;

        let GeneratedColumns {
            projection,
            group_by,
            group_descs,
            flat_group_expr: _,
            aggregate,
            patch_measures,
            filter,
            window: _,
            order,
        } = columns;

        if !patch_measures.is_empty() {
            return Err(CubeError::internal(format!(
                "Unexpected patch measures for non-push-to-Cube wrapped select: {patch_measures:?}",
            )));
        }

        let resulting_sql = generator
            .get_sql_templates()
            .select(
                sql.sql.to_string(),
                projection.into_iter().map(|(m, _)| m).collect(),
                group_by.into_iter().map(|(m, _)| m).collect(),
                group_descs,
                aggregate.into_iter().map(|(m, _)| m).collect(),
                // TODO
                from_alias.unwrap_or("".to_string()),
                if !filter.is_empty() {
                    Some(filter.iter().map(|(f, _)| f.expr.to_string()).join(" AND "))
                } else {
                    None
                },
                None,
                order.into_iter().map(|(m, _)| m).collect(),
                self.limit,
                self.offset,
                self.distinct,
            )
            .map_err(|e| {
                DataFusionError::Internal(format!("Can't generate SQL for wrapped select: {}", e))
            })?;
        sql.replace_sql(resulting_sql.clone());
        Ok(SqlGenerationResult {
            data_source,
            from_alias: alias,
            sql,
            column_remapping: next_remapper.into_remapping(),
            request,
        })
    }
}

impl UserDefinedLogicalNode for WrappedSelectNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        let mut inputs = vec![self.from.as_ref()];
        inputs.extend(self.joins.iter().map(|(j, _, _)| j.as_ref()));
        inputs
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![];
        exprs.extend(self.projection_expr.clone());
        exprs.extend(self.group_expr.clone());
        exprs.extend(self.aggr_expr.clone());
        exprs.extend(self.window_expr.clone());
        exprs.extend(self.joins.iter().map(|(_, expr, _)| expr.clone()));
        exprs.extend(self.filter_expr.clone());
        exprs.extend(self.having_expr.clone());
        exprs.extend(self.order_expr.clone());
        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WrappedSelect: select_type={:?}, projection_expr={:?}, group_expr={:?}, aggregate_expr={:?}, window_expr={:?}, from={:?}, joins={:?}, filter_expr={:?}, having_expr={:?}, limit={:?}, offset={:?}, order_expr={:?}, alias={:?}, distinct={:?}",
            self.select_type,
            self.projection_expr,
            self.group_expr,
            self.aggr_expr,
            self.window_expr,
            self.from,
            self.joins,
            self.filter_expr,
            self.having_expr,
            self.limit,
            self.offset,
            self.order_expr,
            self.alias,
            self.distinct,
        )
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_plan::Expr],
        inputs: &[datafusion::logical_plan::LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), self.inputs().len(), "input size inconsistent");
        assert_eq!(
            exprs.len(),
            self.expressions().len(),
            "expression size inconsistent"
        );

        let from = Arc::new(inputs[0].clone());
        let joins = (1..self.joins.len() + 1)
            .map(|i| Arc::new(inputs[i].clone()))
            .collect::<Vec<_>>();
        let mut joins_expr = vec![];
        let join_types = self.joins.iter().map(|(_, _, t)| *t).collect::<Vec<_>>();
        let mut filter_expr = vec![];
        let mut having_expr = vec![];
        let mut order_expr = vec![];
        let mut projection_expr = vec![];
        let mut group_expr = vec![];
        let mut aggregate_expr = vec![];
        let mut window_expr = vec![];
        let limit = None;
        let offset = None;
        let alias = None;

        let mut exprs_iter = exprs.iter();
        for _ in self.projection_expr.iter() {
            projection_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.group_expr.iter() {
            group_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.aggr_expr.iter() {
            aggregate_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.window_expr.iter() {
            window_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.joins.iter() {
            joins_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.filter_expr.iter() {
            filter_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.having_expr.iter() {
            having_expr.push(exprs_iter.next().unwrap().clone());
        }

        for _ in self.order_expr.iter() {
            order_expr.push(exprs_iter.next().unwrap().clone());
        }

        Arc::new(WrappedSelectNode::new(
            self.schema.clone(),
            self.select_type,
            projection_expr,
            self.subqueries.clone(),
            group_expr,
            aggregate_expr,
            window_expr,
            from,
            joins
                .into_iter()
                .zip(joins_expr)
                .zip(join_types)
                .map(|((plan, expr), join_type)| (plan, expr, join_type))
                .collect(),
            filter_expr,
            having_expr,
            limit,
            offset,
            order_expr,
            alias,
            self.distinct,
            self.push_to_cube,
        ))
    }
}

struct CollectMembersVisitor<'ctx, 'mem> {
    push_to_cube_context: &'ctx PushToCubeContext<'ctx>,
    subqueries: &'ctx HashSet<String>,
    used_members: &'mem mut HashSet<String>,
}

fn collect_used_members<'ctx>(
    expr: &Expr,
    push_to_cube_context: &'ctx PushToCubeContext<'_>,
    subqueries: &'ctx HashSet<String>,
) -> result::Result<HashSet<String>, CubeError> {
    let mut used_members = HashSet::new();
    collect_used_members_to_set(expr, push_to_cube_context, subqueries, &mut used_members)?;
    Ok(used_members)
}

fn collect_used_members_to_set<'ctx, 'mem>(
    expr: &Expr,
    push_to_cube_context: &'ctx PushToCubeContext<'_>,
    subqueries: &'ctx HashSet<String>,
    used_members: &'mem mut HashSet<String>,
) -> result::Result<(), CubeError> {
    let v = CollectMembersVisitor {
        push_to_cube_context,
        subqueries,
        used_members,
    };
    expr.accept(v)?;

    Ok(())
}

impl<'ctx, 'mem> CollectMembersVisitor<'ctx, 'mem> {
    fn handle_column(&mut self, c: &Column) -> Result<()> {
        if self.subqueries.contains(&c.flat_name()) {
            // Do nothing
        } else {
            let PushToCubeContext {
                ungrouped_scan_node,
                known_join_subqueries,
            } = self.push_to_cube_context;

            if let Some(relation) = c.relation.as_ref() {
                if known_join_subqueries.contains(relation) {
                    return Ok(());
                }
            }

            let member = WrappedSelectNode::find_member_in_ungrouped_scan(ungrouped_scan_node, c)?;

            match member {
                MemberField::Member(member) => {
                    self.used_members.insert(member.member.clone());
                }
                MemberField::Literal(_) => {
                    // Do nothing
                }
            }
        }

        Ok(())
    }

    fn handle_count_rows(&mut self) -> Result<()> {
        // COUNT(*) references all members in the ungrouped scan node
        for member in &self.push_to_cube_context.ungrouped_scan_node.member_fields {
            match member {
                MemberField::Member(member) => {
                    self.used_members.insert(member.member.clone());
                }
                MemberField::Literal(_) => {
                    // Do nothing
                }
            }
        }
        Ok(())
    }
}

impl<'ctx, 'mem> ExpressionVisitor for CollectMembersVisitor<'ctx, 'mem> {
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(ref c) => {
                self.handle_column(c)?;
            }
            Expr::AggregateFunction {
                fun: AggregateFunction::Count,
                args,
                ..
            } if args.len() == 1 && matches!(args[0], Expr::Literal(_)) => {
                self.handle_count_rows()?;
            }
            _ => {}
        }

        Ok(Recursion::Continue(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_member_expression_sql() {
        insta::assert_json_snapshot!(UngroupedMemberDef {
            cube_name: "cube".to_string(),
            alias: "alias".to_string(),
            expr: UngroupedMemberExpr::SqlFunction(SqlFunctionExpr {
                cube_params: vec!["cube".to_string(), "other".to_string()],
                sql: "1 + 2".to_string(),
            }),
            grouping_set: None,
        });
    }

    #[test]
    fn test_member_expression_patch_measure() {
        insta::assert_json_snapshot!(UngroupedMemberDef {
            cube_name: "cube".to_string(),
            alias: "alias".to_string(),
            expr: UngroupedMemberExpr::PatchMeasure(PatchMeasureDef {
                source_measure: "cube.measure".to_string(),
                replace_aggregation_type: None,
                add_filters: vec![SqlFunctionExpr {
                    cube_params: vec!["cube".to_string()],
                    sql: "1 + 2 = 3".to_string(),
                }],
            }),
            grouping_set: None,
        });
    }
}
