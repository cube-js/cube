use crate::{
    compile::{
        engine::df::scan::{CubeScanNode, DataType, MemberField, WrappedSelectNode},
        rewrite::{
            extract_exprlist_from_groupping_set,
            rules::{
                filters::Decimal,
                utils::{DecomposedDayTime, DecomposedMonthDayNano},
            },
            LikeType, WrappedSelectType,
        },
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{
        AliasedColumn, LoadRequestMeta, MetaContext, SpanId, SqlGenerator, SqlTemplates,
        TransportLoadRequestQuery, TransportService,
    },
    CubeError,
};
use chrono::{Days, NaiveDate, SecondsFormat, TimeZone, Utc};
use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryJoinSubquery};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::Extension, replace_col, Column, DFSchema, DFSchemaRef, Expr, GroupingSet, JoinType,
        LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::{aggregates::AggregateFunction, functions::BuiltinScalarFunction},
    scalar::ScalarValue,
};
use itertools::Itertools;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    cmp::min,
    collections::{HashMap, HashSet},
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
    join_subqueries: Vec<JoinSubquery>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqlQuery {
    pub sql: String,
    pub values: Vec<Option<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct UngrouppedMemberDef {
    cube_name: String,
    alias: String,
    cube_params: Vec<String>,
    expr: String,
    grouping_set: Option<GroupingSetDesc>,
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub enum GroupingSetType {
    Rollup,
    Cube,
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct GroupingSetDesc {
    pub group_type: GroupingSetType,
    pub id: u64,
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
        self.values.extend(values.into_iter());
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
                MemberField::Member(
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
        replace_col(
            expr.clone(),
            &self.column_remapping.iter().map(|(k, v)| (k, v)).collect(),
        )
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

        let alias = start_from;
        let mut truncated_alias = NON_ID_REGEX
            .replace_all(&alias, "_")
            .trim_start_matches("_")
            .to_lowercase();
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
    (@generic $value:ident, $value_block:expr, $sql_generator:expr, $sql_query:expr) => {
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
            ("NULL".to_string(), $sql_query)
        }
    };
    ($value:ident, timestamp, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.timestamp_opt($value as i64, 0).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($value:ident, timestamp_millis_opt, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.timestamp_millis_opt($value as i64).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($value:ident, timestamp_micros, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.timestamp_micros($value as i64).unwrap() }, $sql_generator, $sql_query
        )
    };
    ($value:ident, $method:ident, $sql_generator:expr, $sql_query:expr) => {
        generate_sql_for_timestamp!(
            @generic $value, { Utc.$method($value as i64) }, $sql_generator, $sql_query
        )
    };
}

impl CubeScanWrapperNode {
    pub async fn generate_sql(
        &self,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
    ) -> result::Result<CubeScanWrappedSqlNode, CubeError> {
        let schema = self.schema();
        let wrapped_plan = self.wrapped_plan.clone();
        let (sql, request, member_fields) = Self::generate_sql_for_node(
            Arc::new(self.clone()),
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
                    .map(|f| MemberField::Member(f.name().to_string()))
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

    pub fn generate_sql_for_node(
        plan: Arc<Self>,
        transport: Arc<dyn TransportService>,
        load_request_meta: Arc<LoadRequestMeta>,
        node: Arc<LogicalPlan>,
        can_rename_columns: bool,
        values: Vec<Option<String>>,
        parent_data_source: Option<String>,
    ) -> Pin<Box<dyn Future<Output = result::Result<SqlGenerationResult, CubeError>> + Send>> {
        Box::pin(async move {
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
                    // .cloned() to avoid borrowing Any to comply with Send + Sync
                    let cube_scan_node = node.as_any().downcast_ref::<CubeScanNode>().cloned();
                    let wrapped_select_node =
                        node.as_any().downcast_ref::<WrappedSelectNode>().cloned();
                    if let Some(node) = cube_scan_node {
                        let data_sources = node
                            .used_cubes
                            .iter()
                            .map(|c| plan.meta.cube_to_data_source.get(c).map(|c| c.to_string()))
                            .unique()
                            .collect::<Option<Vec<_>>>().ok_or_else(|| {
                            CubeError::internal(format!(
                                "Can't generate SQL for node due to sql generator can't be found: {:?}",
                                node
                            ))
                        })?;
                        if data_sources.len() != 1 {
                            return Err(CubeError::internal(format!(
                                "Can't generate SQL for node due to multiple data sources {}: {:?}",
                                data_sources.join(","),
                                node
                            )));
                        }
                        let data_source = &data_sources[0];
                        let mut meta_with_user = load_request_meta.as_ref().clone();
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
                        let mut wrapper_exprs = vec![];

                        for (member, field) in
                            node.member_fields.iter().zip(node.schema.fields().iter())
                        {
                            let alias = remapper.add_column(&field.qualified_column())?;
                            let expr = match member {
                                MemberField::Member(f) => {
                                    member_to_alias.insert(f.to_string(), alias.clone());
                                    // `alias` is column name that would be generated by Cube.js, just reference that
                                    Expr::Column(Column::from_name(alias.clone()))
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
                                node.auth_context,
                                meta_with_user,
                                Some(member_to_alias),
                                None,
                            )
                            .await?;

                        // TODO is this check necessary?
                        let sql = if has_literal_members {
                            // Need to generate wrapper SELECT with literal columns
                            // Generated columns need to have same aliases as targets in `remapper`
                            // Because that's what plans higher up would use in generated SQL
                            let generator = plan
                                .meta
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
                                let (expr, sql) = Self::generate_sql_for_expr(
                                    plan.clone(),
                                    new_sql,
                                    generator.clone(),
                                    expr,
                                    None,
                                    Arc::new(HashMap::new()),
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
                            data_source: Some(data_source.clone()),
                            from_alias,
                            sql,
                            column_remapping,
                            request: node.request.clone(),
                        });
                    } else if let Some(WrappedSelectNode {
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
                        having_expr: _having_expr,
                        limit,
                        offset,
                        order_expr,
                        alias,
                        distinct,
                        push_to_cube,
                    }) = wrapped_select_node
                    {
                        // TODO support ungrouped joins
                        let ungrouped_scan_node = if push_to_cube {
                            if let LogicalPlan::Extension(Extension { node }) = from.as_ref() {
                                if let Some(cube_scan_node) =
                                    node.as_any().downcast_ref::<CubeScanNode>()
                                {
                                    if cube_scan_node.request.ungrouped != Some(true) {
                                        return Err(CubeError::internal(format!(
                                            "Expected ungrouped CubeScan node but found: {cube_scan_node:?}"
                                        )));
                                    }
                                    Some(cube_scan_node)
                                } else {
                                    return Err(CubeError::internal(format!(
                                        "Expected CubeScan node but found: {:?}",
                                        plan
                                    )));
                                }
                            } else {
                                return Err(CubeError::internal(format!(
                                    "Expected CubeScan node but found: {:?}",
                                    plan
                                )));
                            }
                        } else {
                            None
                        };

                        let SqlGenerationResult {
                            data_source,
                            from_alias,
                            mut column_remapping,
                            mut sql,
                            request,
                        } = if let Some(ungrouped_scan_node) = &ungrouped_scan_node {
                            let data_sources = ungrouped_scan_node
                                .used_cubes
                                .iter()
                                .map(|c| plan.meta.cube_to_data_source.get(c).map(|c| c.to_string()))
                                .unique()
                                .collect::<Option<Vec<_>>>().ok_or_else(|| {
                                CubeError::internal(format!(
                                    "Can't generate SQL for node due to sql generator can't be found: {:?}",
                                    ungrouped_scan_node
                                ))
                            })?;
                            if data_sources.len() != 1 {
                                return Err(CubeError::internal(format!(
                                    "Can't generate SQL for node due to multiple data sources {}: {:?}",
                                    data_sources.join(","),
                                    ungrouped_scan_node
                                )));
                            }
                            let sql = SqlQuery::new("".to_string(), values.clone());
                            SqlGenerationResult {
                                data_source: Some(data_sources[0].clone()),
                                from_alias: ungrouped_scan_node
                                    .schema
                                    .fields()
                                    .iter()
                                    .next()
                                    .and_then(|f| f.qualifier().cloned()),
                                column_remapping: None,
                                sql,
                                request: ungrouped_scan_node.request.clone(),
                            }
                        } else {
                            Self::generate_sql_for_node(
                                plan.clone(),
                                transport.clone(),
                                load_request_meta.clone(),
                                from.clone(),
                                true,
                                values.clone(),
                                parent_data_source.clone(),
                            )
                            .await?
                        };

                        let mut subqueries_sql = HashMap::new();
                        for subquery in subqueries.iter() {
                            let SqlGenerationResult {
                                data_source: _,
                                from_alias: _,
                                column_remapping: _,
                                sql: subquery_sql,
                                request: _,
                            } = Self::generate_sql_for_node(
                                plan.clone(),
                                transport.clone(),
                                load_request_meta.clone(),
                                subquery.clone(),
                                true,
                                sql.values.clone(),
                                data_source.clone(),
                            )
                            .await?;

                            let (sql_string, new_values) = subquery_sql.unpack();
                            sql.extend_values(new_values);
                            // TODO why only field 0 is a key?
                            let field = subquery.schema().field(0);
                            subqueries_sql.insert(field.qualified_name(), sql_string);
                        }
                        let subqueries_sql = Arc::new(subqueries_sql);
                        let alias = alias.or(from_alias.clone());
                        let mut next_remapper = Remapper::new(alias.clone(), can_rename_columns);

                        let push_to_cube_context = if let Some(ungrouped_scan_node) =
                            ungrouped_scan_node
                        {
                            let mut join_subqueries = vec![];
                            let mut known_join_subqueries = HashSet::new();
                            for (lp, cond, join_type) in joins {
                                match lp.as_ref() {
                                    LogicalPlan::Extension(Extension { node }) => {
                                        if let Some(join_cube_scan) =
                                            node.as_any().downcast_ref::<CubeScanNode>()
                                        {
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
                                let alias = lp
                                    .schema()
                                    .fields()
                                    .iter()
                                    .filter_map(|f| f.qualifier())
                                    .next()
                                    .ok_or_else(|| {
                                        CubeError::internal(format!(
                                            "Alias not found for join subquery {lp:?}"
                                        ))
                                    })?;

                                let subq_sql = Self::generate_sql_for_node(
                                    plan.clone(),
                                    transport.clone(),
                                    load_request_meta.clone(),
                                    lp.clone(),
                                    true,
                                    sql.values.clone(),
                                    data_source.clone(),
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
                                        (None, Some(remapping)) | (Some(remapping), None) => {
                                            Some(remapping)
                                        }
                                        (Some(mut left), Some(right)) => {
                                            left.extend(right);
                                            Some(left)
                                        }
                                    }
                                };

                                join_subqueries.push(JoinSubquery {
                                    // TODO what alias to actually use here? two more-or-less valid options: returned from generate_sql_for_node ot realiased from `alias`. Plain `alias` is incorrect here
                                    alias: subq_alias.unwrap_or_else(|| alias.clone()),
                                    sql: subq_sql_string,
                                    condition: cond.clone(),
                                    join_type: join_type.clone(),
                                });
                                known_join_subqueries.insert(alias.clone());
                            }

                            Some(PushToCubeContext {
                                ungrouped_scan_node,
                                join_subqueries,
                                known_join_subqueries,
                            })
                        } else {
                            None
                        };
                        // Drop mut, turn to ref
                        let column_remapping = column_remapping.as_ref();
                        // Turn to ref
                        let push_to_cube_context = push_to_cube_context.as_ref();
                        if let Some(data_source) = data_source {
                            let generator = plan
                                .meta
                                .data_source_to_sql_generator
                                .get(&data_source)
                                .ok_or_else(|| {
                                    CubeError::internal(format!(
                                        "Can't generate SQL for wrapped select: no sql generator for {:?}",
                                        node
                                    ))
                                })?
                                .clone();
                            let (projection, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                projection_expr.clone(),
                                sql,
                                generator.clone(),
                                column_remapping,
                                &mut next_remapper,
                                can_rename_columns,
                                push_to_cube_context,
                                subqueries_sql.clone(),
                            )
                            .await?;
                            let flat_group_expr = extract_exprlist_from_groupping_set(&group_expr);
                            let (group_by, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                flat_group_expr.clone(),
                                sql,
                                generator.clone(),
                                column_remapping,
                                &mut next_remapper,
                                can_rename_columns,
                                push_to_cube_context,
                                subqueries_sql.clone(),
                            )
                            .await?;
                            let group_descs = extract_group_type_from_groupping_set(&group_expr)?;
                            let (aggregate, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                aggr_expr.clone(),
                                sql,
                                generator.clone(),
                                column_remapping,
                                &mut next_remapper,
                                can_rename_columns,
                                push_to_cube_context,
                                subqueries_sql.clone(),
                            )
                            .await?;

                            let (filter, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                filter_expr.clone(),
                                sql,
                                generator.clone(),
                                column_remapping,
                                &mut next_remapper,
                                can_rename_columns,
                                push_to_cube_context,
                                subqueries_sql.clone(),
                            )
                            .await?;

                            let (window, sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                window_expr.clone(),
                                sql,
                                generator.clone(),
                                column_remapping,
                                &mut next_remapper,
                                can_rename_columns,
                                push_to_cube_context,
                                subqueries_sql.clone(),
                            )
                            .await?;

                            let (order, mut sql) = Self::generate_column_expr(
                                plan.clone(),
                                schema.clone(),
                                order_expr.clone(),
                                sql,
                                generator.clone(),
                                column_remapping,
                                &mut next_remapper,
                                can_rename_columns,
                                push_to_cube_context,
                                subqueries_sql.clone(),
                            )
                            .await?;
                            if let Some(PushToCubeContext {
                                ungrouped_scan_node,
                                join_subqueries,
                                known_join_subqueries: _,
                            }) = push_to_cube_context
                            {
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
                                        plan.clone(),
                                        schema.clone(),
                                        [condition.clone()],
                                        sql,
                                        generator.clone(),
                                        column_remapping,
                                        &mut next_remapper,
                                        true,
                                        push_to_cube_context,
                                        subqueries_sql.clone(),
                                    )
                                    .await?;
                                    let join_condition = join_condition[0].expr.clone();
                                    sql = new_sql;

                                    let join_sql_expression = {
                                        // TODO this is NOT a proper way to generate member expr here
                                        // TODO Do we even want a full-blown member expression here? or arguments + expr will be enough?
                                        let res = Self::make_member_def(
                                            &AliasedColumn {
                                                expr: join_condition,
                                                alias: "__join__alias__unused".to_string(),
                                            },
                                            &ungrouped_scan_node.used_cubes,
                                        )?;
                                        serde_json::json!(res).to_string()
                                    };

                                    let join_type = match join_type {
                                        JoinType::Left => generator
                                            .get_sql_templates()
                                            .left_join()?,
                                        JoinType::Inner => generator
                                            .get_sql_templates()
                                            .inner_join()?,
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

                                let load_request = V1LoadRequestQuery {
                                    measures: Some(
                                        aggregate
                                            .iter()
                                            .map(|m| {
                                                Self::ungrouped_member_def(
                                                    m,
                                                    &ungrouped_scan_node.used_cubes,
                                                )
                                            })
                                            .chain(
                                                // TODO understand type of projections
                                                projection.iter().map(|m| {
                                                    Self::ungrouped_member_def(
                                                        m,
                                                        &ungrouped_scan_node.used_cubes,
                                                    )
                                                }),
                                            )
                                            .chain(window.iter().map(|m| {
                                                Self::ungrouped_member_def(
                                                    m,
                                                    &ungrouped_scan_node.used_cubes,
                                                )
                                            }))
                                            .collect::<Result<_>>()?,
                                    ),
                                    dimensions: Some(
                                        group_by
                                            .iter()
                                            .zip(group_descs.iter())
                                            .map(|(m, t)| {
                                                Self::dimension_member_def(
                                                    m,
                                                    &ungrouped_scan_node.used_cubes,
                                                    t,
                                                )
                                            })
                                            .collect::<Result<_>>()?,
                                    ),
                                    segments: Some(
                                        filter
                                            .iter()
                                            .map(|m| {
                                                Self::ungrouped_member_def(
                                                    m,
                                                    &ungrouped_scan_node.used_cubes,
                                                )
                                            })
                                            .collect::<Result<_>>()?,
                                    ),
                                    order: if !order_expr.is_empty() {
                                        Some(
                                            order_expr
                                                .iter()
                                                .map(|o| -> Result<_> { match o {
                                                    Expr::Sort {
                                                        expr,
                                                        asc,
                                                        ..
                                                    } => {
                                                        let col_name = expr_name(&expr, &schema)?;
                                                        let aliased_column = aggr_expr
                                                            .iter()
                                                            .find_position(|e| {
                                                                expr_name(e, &schema).map(|n| &n == &col_name).unwrap_or(false)
                                                            })
                                                            .map(|(i, _)| aggregate[i].clone()).or_else(|| {
                                                            projection_expr
                                                                .iter()
                                                                .find_position(|e| {
                                                                    expr_name(e, &schema).map(|n| &n == &col_name).unwrap_or(false)
                                                                })
                                                                .map(|(i, _)| {
                                                                    projection[i].clone()
                                                                })
                                                        }).or_else(|| {
                                                            flat_group_expr
                                                                .iter()
                                                                .find_position(|e| {
                                                                    expr_name(e, &schema).map(|n| &n == &col_name).unwrap_or(false)
                                                                })
                                                                .map(|(i, _)| group_by[i].clone())
                                                        }).ok_or_else(|| {
                                                            DataFusionError::Execution(format!(
                                                                "Can't find column {} in projection {:?} or aggregate {:?} or group {:?}",
                                                                col_name,
                                                                projection_expr,
                                                                aggr_expr,
                                                                flat_group_expr
                                                            ))
                                                        })?;
                                                        Ok(vec![
                                                            aliased_column.alias.clone(),
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
                                    ungrouped: if let WrappedSelectType::Projection = select_type {
                                        load_request.ungrouped.clone()
                                    } else {
                                        None
                                    },
                                    // TODO is it okay to just override limit?
                                    limit: if let Some(limit) = limit {
                                        Some(limit as i32)
                                    } else {
                                        load_request.limit.clone()
                                    },
                                    // TODO is it okay to just override offset?
                                    offset: if let Some(offset) = offset {
                                        Some(offset as i32)
                                    } else {
                                        load_request.offset.clone()
                                    },

                                    // Original scan node can already have consumed filters from Logical plan
                                    // It's incorrect to just throw them away
                                    filters: ungrouped_scan_node.request.filters.clone(),

                                    time_dimensions: load_request.time_dimensions.clone(),
                                    subquery_joins: (!prepared_join_subqueries.is_empty())
                                        .then_some(prepared_join_subqueries),
                                };

                                // TODO time dimensions, filters, segments

                                let mut meta_with_user = load_request_meta.as_ref().clone();
                                meta_with_user.set_change_user(
                                    ungrouped_scan_node.options.change_user.clone(),
                                );
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
                                    data_source: Some(data_source),
                                    from_alias: alias,
                                    sql: sql_response.sql,
                                    column_remapping: next_remapper.into_remapping(),
                                    request: load_request.clone(),
                                })
                            } else {
                                let resulting_sql = generator
                                    .get_sql_templates()
                                    .select(
                                        sql.sql.to_string(),
                                        projection,
                                        group_by,
                                        group_descs,
                                        aggregate,
                                        // TODO
                                        from_alias.unwrap_or("".to_string()),
                                        if !filter.is_empty() {
                                            Some(
                                                filter
                                                    .iter()
                                                    .map(|f| f.expr.to_string())
                                                    .join(" AND "),
                                            )
                                        } else {
                                            None
                                        },
                                        None,
                                        order,
                                        limit,
                                        offset,
                                        distinct,
                                    )
                                    .map_err(|e| {
                                        DataFusionError::Internal(format!(
                                            "Can't generate SQL for wrapped select: {}",
                                            e
                                        ))
                                    })?;
                                sql.replace_sql(resulting_sql.clone());
                                Ok(SqlGenerationResult {
                                    data_source: Some(data_source),
                                    from_alias: alias,
                                    sql,
                                    column_remapping: next_remapper.into_remapping(),
                                    request,
                                })
                            }
                        } else {
                            Err(CubeError::internal(format!(
                                "Can't generate SQL for wrapped select: no data source for {:?}",
                                node
                            )))
                        }
                    } else {
                        return Err(CubeError::internal(format!(
                            "Can't generate SQL for node: {:?}",
                            node
                        )));
                    }
                }
                LogicalPlan::EmptyRelation(_) => Ok(SqlGenerationResult {
                    data_source: parent_data_source,
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
        })
    }

    async fn generate_column_expr(
        plan: Arc<Self>,
        schema: DFSchemaRef,
        exprs: impl IntoIterator<Item = Expr>,
        mut sql: SqlQuery,
        generator: Arc<dyn SqlGenerator>,
        column_remapping: Option<&ColumnRemapping>,
        next_remapper: &mut Remapper,
        can_rename_columns: bool,
        push_to_cube_context: Option<&PushToCubeContext<'_>>,
        subqueries: Arc<HashMap<String, String>>,
    ) -> result::Result<(Vec<AliasedColumn>, SqlQuery), CubeError> {
        let mut aliased_columns = Vec::new();
        for original_expr in exprs {
            let expr = if let Some(column_remapping) = column_remapping {
                let mut expr = column_remapping.remap(&original_expr)?;
                if !can_rename_columns {
                    let original_alias = expr_name(&original_expr, &schema)?;
                    if original_alias != expr_name(&expr, &schema)? {
                        expr = Expr::Alias(Box::new(expr), original_alias.clone());
                    }
                }
                expr
            } else {
                original_expr.clone()
            };
            let (expr_sql, new_sql_query) = Self::generate_sql_for_expr(
                plan.clone(),
                sql,
                generator.clone(),
                expr.clone(),
                push_to_cube_context,
                subqueries.clone(),
            )
            .await?;
            let expr_sql =
                Self::escape_interpolation_quotes(expr_sql, push_to_cube_context.is_some());
            sql = new_sql_query;

            let alias = next_remapper.add_expr(&schema, &original_expr, &expr)?;
            aliased_columns.push(AliasedColumn {
                expr: expr_sql,
                alias,
            });
        }
        Ok((aliased_columns, sql))
    }

    fn make_member_def(
        column: &AliasedColumn,
        used_cubes: &Vec<String>,
    ) -> Result<UngrouppedMemberDef> {
        let res = UngrouppedMemberDef {
            cube_name: used_cubes
                .iter()
                .next()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can't generate SQL for column without cubes: {:?}",
                        column
                    ))
                })?
                .to_string(),
            alias: column.alias.clone(),
            cube_params: used_cubes.clone(),
            expr: column.expr.clone(),
            grouping_set: None,
        };
        Ok(res)
    }

    fn ungrouped_member_def(column: &AliasedColumn, used_cubes: &Vec<String>) -> Result<String> {
        let res = Self::make_member_def(column, used_cubes)?;
        Ok(serde_json::json!(res).to_string())
    }

    fn dimension_member_def(
        column: &AliasedColumn,
        used_cubes: &Vec<String>,
        grouping_type: &Option<GroupingSetDesc>,
    ) -> Result<String> {
        let mut res = Self::make_member_def(column, used_cubes)?;
        res.grouping_set = grouping_type.clone();
        Ok(serde_json::json!(res).to_string())
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

    /// This function is async to be able to call to JS land,
    /// in case some SQL generation could not be done through Jinja
    pub fn generate_sql_for_expr<'ctx>(
        plan: Arc<Self>,
        mut sql_query: SqlQuery,
        sql_generator: Arc<dyn SqlGenerator>,
        expr: Expr,
        push_to_cube_context: Option<&'ctx PushToCubeContext>,
        subqueries: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = Result<(String, SqlQuery)>> + Send + 'ctx>> {
        Box::pin(async move {
            match expr {
                Expr::Alias(expr, _) => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
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
                        join_subqueries: _,
                        known_join_subqueries,
                    }) = push_to_cube_context
                    {
                        if let Some(relation) = c.relation.as_ref() {
                            if known_join_subqueries.contains(relation) {
                                // SQL API passes fixed aliases to Cube.js for join subqueries
                                // It means we don't need to use member expressions here, and can just use that fixed alias
                                // So we can generate that as if it were regular column expression

                                return Self::generate_sql_for_expr(
                                    plan.clone(),
                                    sql_query,
                                    sql_generator.clone(),
                                    expr,
                                    None,
                                    subqueries.clone(),
                                )
                                .await;
                            }
                        }

                        let field_index = ungrouped_scan_node
                            .schema
                            .fields()
                            .iter()
                            .find_position(|f| {
                                f.name() == &c.name
                                    && match c.relation.as_ref() {
                                        Some(r) => Some(r) == f.qualifier(),
                                        None => true,
                                    }
                            })
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Can't find column {} in ungrouped scan node",
                                    c
                                ))
                            })?
                            .0;
                        let member = ungrouped_scan_node
                            .member_fields
                            .get(field_index)
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Can't find member for column {} in ungrouped scan node",
                                    c
                                ))
                            })?;
                        match member {
                            MemberField::Member(member) => {
                                Ok((format!("${{{}}}", member), sql_query))
                            }
                            MemberField::Literal(value) => {
                                Self::generate_sql_for_expr(
                                    plan.clone(),
                                    sql_query,
                                    sql_generator.clone(),
                                    Expr::Literal(value.clone()),
                                    push_to_cube_context,
                                    subqueries.clone(),
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
                    let (left, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *left,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let (right, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *right,
                        push_to_cube_context,
                        subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *like.expr,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let (pattern, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *like.pattern,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let (escape_char, sql_query) = match like.escape_char {
                        Some(escape_char) => {
                            let (escape_char, sql_query) = Self::generate_sql_for_expr(
                                plan.clone(),
                                sql_query,
                                sql_generator.clone(),
                                Expr::Literal(ScalarValue::Utf8(Some(escape_char.to_string()))),
                                push_to_cube_context,
                                subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *ilike.expr,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let (pattern, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *ilike.pattern,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let (escape_char, sql_query) = match ilike.escape_char {
                        Some(escape_char) => {
                            let (escape_char, sql_query) = Self::generate_sql_for_expr(
                                plan.clone(),
                                sql_query,
                                sql_generator.clone(),
                                Expr::Literal(ScalarValue::Utf8(Some(escape_char.to_string()))),
                                push_to_cube_context,
                                subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let resulting_sql = sql_generator
                        .get_sql_templates()
                        .negative_expr(expr)
                        .map_err(|e| {
                            DataFusionError::Internal(format!(
                                "Can't generate SQL for not expr: {}",
                                e
                            ))
                        })?;
                    Ok((resulting_sql, sql_query))
                }
                // Expr::GetIndexedField { .. } => {}
                // Expr::Between { .. } => {}
                Expr::Case {
                    expr,
                    when_then_expr,
                    else_expr,
                } => {
                    let expr = if let Some(expr) = expr {
                        let (expr, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            *expr,
                            push_to_cube_context,
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = sql_query_next;
                        Some(expr)
                    } else {
                        None
                    };
                    let mut when_then_expr_sql = Vec::new();
                    for (when, then) in when_then_expr {
                        let (when, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            *when,
                            push_to_cube_context,
                            subqueries.clone(),
                        )
                        .await?;
                        let (then, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query_next,
                            sql_generator.clone(),
                            *then,
                            push_to_cube_context,
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = sql_query_next;
                        when_then_expr_sql.push((when, then));
                    }
                    let else_expr = if let Some(else_expr) = else_expr {
                        let (else_expr, sql_query_next) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            *else_expr,
                            push_to_cube_context,
                            subqueries.clone(),
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
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    let data_type = Self::generate_sql_type(sql_generator.clone(), data_type)?;
                    let resulting_sql =
                        Self::generate_sql_cast_expr(sql_generator, expr, data_type)?;
                    Ok((resulting_sql, sql_query))
                }
                // Expr::TryCast { .. } => {}
                Expr::Sort {
                    expr,
                    asc,
                    nulls_first,
                } => {
                    let (expr, sql_query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
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
                            .unwrap_or(Ok("NULL".to_string()))?,
                            sql_query,
                        ),
                        ScalarValue::Float32(f) => (
                            f.map(|f| format!("{}", f)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Float64(f) => (
                            f.map(|f| format!("{}", f)).unwrap_or("NULL".to_string()),
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
                                    CubeScanWrapperNode::generate_sql_cast_expr(
                                        sql_generator,
                                        format!("'{}'", number),
                                        data_type,
                                    )?
                                } else {
                                    "NULL".to_string()
                                },
                                sql_query,
                            )
                        }
                        ScalarValue::Int8(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Int16(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Int32(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Int64(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt8(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt16(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt32(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::UInt64(x) => (
                            x.map(|x| format!("{}", x)).unwrap_or("NULL".to_string()),
                            sql_query,
                        ),
                        ScalarValue::Utf8(x) => {
                            if x.is_some() {
                                let param_index = sql_query.add_value(x);
                                (format!("${}$", param_index), sql_query)
                            } else {
                                ("NULL".into(), sql_query)
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
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        // ScalarValue::Date64(_) => {}

                        // generate_sql_for_timestamp will call Utc constructors, so only support UTC zone for now
                        // DataFusion can return "UTC" for stuff like `NOW()` during constant folding
                        ScalarValue::TimestampSecond(s, tz)
                            if matches!(tz.as_deref(), None | Some("UTC")) =>
                        {
                            generate_sql_for_timestamp!(s, timestamp, sql_generator, sql_query)
                        }
                        ScalarValue::TimestampMillisecond(ms, tz)
                            if matches!(tz.as_deref(), None | Some("UTC")) =>
                        {
                            generate_sql_for_timestamp!(
                                ms,
                                timestamp_millis_opt,
                                sql_generator,
                                sql_query
                            )
                        }
                        ScalarValue::TimestampMicrosecond(ms, tz)
                            if matches!(tz.as_deref(), None | Some("UTC")) =>
                        {
                            generate_sql_for_timestamp!(
                                ms,
                                timestamp_micros,
                                sql_generator,
                                sql_query
                            )
                        }
                        ScalarValue::TimestampNanosecond(nanoseconds, tz)
                            if matches!(tz.as_deref(), None | Some("UTC")) =>
                        {
                            generate_sql_for_timestamp!(
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
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        ScalarValue::IntervalDayTime(x) => {
                            if let Some(x) = x {
                                let templates = sql_generator.get_sql_templates();
                                let decomposed = DecomposedDayTime::from_raw_interval_value(x);
                                let generated_sql = decomposed.generate_interval_sql(&templates)?;
                                (generated_sql, sql_query)
                            } else {
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        ScalarValue::IntervalMonthDayNano(x) => {
                            if let Some(x) = x {
                                let templates = sql_generator.get_sql_templates();
                                let decomposed = DecomposedMonthDayNano::from_raw_interval_value(x);
                                let generated_sql = decomposed.generate_interval_sql(&templates)?;
                                (generated_sql, sql_query)
                            } else {
                                ("NULL".to_string(), sql_query)
                            }
                        }
                        // ScalarValue::Struct(_, _) => {}
                        ScalarValue::Null => ("NULL".to_string(), sql_query),
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
                                    Err(date_part_err(date_part))
                                }
                            }
                            _ => Err(date_part_err(&args[0].to_string())),
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
                        _ => Ok(None),
                    }?;
                    let mut sql_args = Vec::new();
                    for arg in args {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            push_to_cube_context,
                            subqueries.clone(),
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
                                    let (arg_sql, query) = Self::generate_sql_for_expr(
                                        plan.clone(),
                                        sql_query,
                                        sql_generator.clone(),
                                        args[1].clone(),
                                        push_to_cube_context,
                                        subqueries.clone(),
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
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            push_to_cube_context,
                            subqueries.clone(),
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
                } => {
                    let mut sql_args = Vec::new();
                    for arg in args {
                        if let AggregateFunction::Count = fun {
                            if !distinct {
                                if let Expr::Literal(_) = arg {
                                    sql_args.push("*".to_string());
                                    break;
                                }
                            }
                        }
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            push_to_cube_context,
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    Ok((
                        sql_generator
                            .get_sql_templates()
                            .aggregate_function(fun, sql_args, distinct)
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
                            let (sql, query) = Self::generate_sql_for_expr(
                                plan.clone(),
                                sql_query,
                                sql_generator.clone(),
                                expr,
                                push_to_cube_context,
                                subqueries.clone(),
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
                            let (sql, query) = Self::generate_sql_for_expr(
                                plan.clone(),
                                sql_query,
                                sql_generator.clone(),
                                expr,
                                push_to_cube_context,
                                subqueries.clone(),
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
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            push_to_cube_context,
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_args.push(sql);
                    }
                    let mut sql_partition_by = Vec::new();
                    for arg in partition_by {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            push_to_cube_context,
                            subqueries.clone(),
                        )
                        .await?;
                        sql_query = query;
                        sql_partition_by.push(sql);
                    }
                    let mut sql_order_by = Vec::new();
                    for arg in order_by {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            arg,
                            push_to_cube_context,
                            subqueries.clone(),
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
                // Expr::AggregateUDF { .. } => {}
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => {
                    let mut sql_query = sql_query;
                    let (sql_expr, query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    sql_query = query;
                    let mut sql_in_exprs = Vec::new();
                    for expr in list {
                        let (sql, query) = Self::generate_sql_for_expr(
                            plan.clone(),
                            sql_query,
                            sql_generator.clone(),
                            expr,
                            push_to_cube_context,
                            subqueries.clone(),
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
                    let (sql_expr, query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *expr,
                        push_to_cube_context,
                        subqueries.clone(),
                    )
                    .await?;
                    sql_query = query;
                    let (subquery_sql, query) = Self::generate_sql_for_expr(
                        plan.clone(),
                        sql_query,
                        sql_generator.clone(),
                        *subquery,
                        push_to_cube_context,
                        subqueries.clone(),
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
        })
    }

    fn escape_interpolation_quotes(s: String, ungrouped: bool) -> String {
        if ungrouped {
            s.replace("\\", "\\\\").replace("`", "\\`")
        } else {
            s
        }
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
