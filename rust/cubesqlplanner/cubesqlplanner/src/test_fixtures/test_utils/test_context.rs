use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::logical_plan::PreAggregation;
#[cfg(feature = "integration-postgres")]
use crate::logical_plan::{PreAggregationSource, PreAggregationTable};
use crate::plan::Filter;
use crate::planner::filter::base_segment::BaseSegment;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor, TimeDimensionSymbol};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::top_level_planner::TopLevelPlanner;
use crate::planner::{GranularityHelper, QueryProperties, VisitorContext};
use crate::test_fixtures::cube_bridge::yaml::YamlBaseQueryOptions;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockBaseTools, MockSchema, MockSecurityContext,
};
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Test context providing query tools and symbol creation helpers
pub struct TestContext {
    schema: MockSchema,
    query_tools: Rc<QueryTools>,
    security_context: Rc<dyn crate::cube_bridge::security_context::SecurityContext>,
}

impl TestContext {
    pub fn new(schema: MockSchema) -> Result<Self, CubeError> {
        Self::new_with_options(schema, Tz::UTC, None, None, false)
    }

    #[allow(dead_code)]
    pub fn new_with_base_tools(
        schema: MockSchema,
        base_tools: MockBaseTools,
    ) -> Result<Self, CubeError> {
        let join_graph = Rc::new(schema.create_join_graph()?);
        let evaluator = schema.clone().create_evaluator();
        let security_context: Rc<dyn crate::cube_bridge::security_context::SecurityContext> =
            Rc::new(MockSecurityContext);

        let query_tools = QueryTools::try_new(
            evaluator,
            security_context.clone(),
            Rc::new(base_tools),
            join_graph,
            Some(Tz::UTC.to_string()),
            false,
            None,
            None,
        )?;

        Ok(Self {
            schema,
            query_tools,
            security_context,
        })
    }

    #[allow(dead_code)]
    pub fn new_with_generated_time_series(schema: MockSchema) -> Result<Self, CubeError> {
        use crate::test_fixtures::cube_bridge::{MockDriverTools, MockSqlTemplatesRender};
        let sql_templates = MockSqlTemplatesRender::default_templates_with_generated_time_series();
        let driver_tools = MockDriverTools::with_sql_templates(sql_templates);
        let base_tools = schema.create_base_tools_with_driver(driver_tools)?;
        Self::new_with_base_tools(schema, base_tools)
    }

    #[allow(dead_code)]
    pub fn new_with_timezone(schema: MockSchema, timezone: Tz) -> Result<Self, CubeError> {
        Self::new_with_options(schema, timezone, None, None, false)
    }

    pub fn new_with_masked_members(
        schema: MockSchema,
        masked_members: Vec<String>,
    ) -> Result<Self, CubeError> {
        Self::new_with_options(schema, Tz::UTC, Some(masked_members), None, false)
    }

    fn for_options(&self, options: &dyn BaseQueryOptions) -> Result<Self, CubeError> {
        let static_data = options.static_data();
        let timezone = static_data
            .timezone
            .as_deref()
            .and_then(|tz| tz.parse::<Tz>().ok())
            .unwrap_or(Tz::UTC);

        Self::new_with_options(
            self.schema.clone(),
            timezone,
            static_data.masked_members.clone(),
            static_data.member_to_alias.clone(),
            static_data.export_annotated_sql,
        )
    }

    fn new_with_options(
        schema: MockSchema,
        timezone: Tz,
        masked_members: Option<Vec<String>>,
        member_to_alias: Option<std::collections::HashMap<String, String>>,
        export_annotated_sql: bool,
    ) -> Result<Self, CubeError> {
        let base_tools = schema.create_base_tools_with_timezone(timezone.to_string())?;
        let join_graph = Rc::new(schema.create_join_graph()?);
        let evaluator = schema.clone().create_evaluator();
        let security_context: Rc<dyn crate::cube_bridge::security_context::SecurityContext> =
            Rc::new(MockSecurityContext);

        let query_tools = QueryTools::try_new(
            evaluator,
            security_context.clone(),
            Rc::new(base_tools),
            join_graph,
            Some(timezone.to_string()),
            export_annotated_sql,
            masked_members,
            member_to_alias,
        )?;

        Ok(Self {
            schema,
            query_tools,
            security_context,
        })
    }

    #[allow(dead_code)]
    pub fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
    }

    #[allow(dead_code)]
    pub fn security_context(
        &self,
    ) -> &Rc<dyn crate::cube_bridge::security_context::SecurityContext> {
        &self.security_context
    }

    #[allow(dead_code)]
    pub fn create_symbol(&self, member_path: &str) -> Result<Rc<MemberSymbol>, CubeError> {
        self.query_tools
            .evaluator_compiler()
            .borrow_mut()
            .add_auto_resolved_member_evaluator(member_path.to_string())
    }

    pub fn create_dimension(&self, path: &str) -> Result<Rc<MemberSymbol>, CubeError> {
        self.query_tools
            .evaluator_compiler()
            .borrow_mut()
            .add_dimension_evaluator(path.to_string())
    }

    pub fn create_measure(&self, path: &str) -> Result<Rc<MemberSymbol>, CubeError> {
        self.query_tools
            .evaluator_compiler()
            .borrow_mut()
            .add_measure_evaluator(path.to_string())
    }

    pub fn create_segment(&self, path: &str) -> Result<Rc<BaseSegment>, CubeError> {
        let mut iter = self
            .query_tools
            .cube_evaluator()
            .parse_path("segments".to_string(), path.to_string())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let definition = self
            .query_tools
            .cube_evaluator()
            .segment_by_path(path.to_string())?;
        let mut compiler = self.query_tools.evaluator_compiler().borrow_mut();
        let expression = compiler.compile_sql_call(&cube_name, definition.sql()?)?;
        let cube_symbol = compiler.add_cube_table_evaluator(cube_name.clone(), vec![])?;
        drop(compiler);
        BaseSegment::try_new(expression, cube_symbol, name, Some(path.to_string()))
    }

    #[allow(dead_code)]
    pub fn create_time_dimension(
        &self,
        path: &str,
        granularity: Option<&str>,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let mut compiler = self.query_tools.evaluator_compiler().borrow_mut();
        let base_symbol = compiler.add_dimension_evaluator(path.to_string())?;
        let granularity = granularity.map(|g| g.to_string());
        let granularity_obj = GranularityHelper::make_granularity_obj(
            self.query_tools.cube_evaluator().clone(),
            &mut compiler,
            &base_symbol.cube_name(),
            &base_symbol.name(),
            granularity.clone(),
        )?;
        Ok(MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
            base_symbol,
            granularity,
            granularity_obj,
            None,
        )))
    }

    pub fn evaluate_symbol(&self, symbol: &Rc<MemberSymbol>) -> Result<String, CubeError> {
        let nodes_factory = SqlNodesFactory::default();
        let cube_ref_evaluator = Rc::new(nodes_factory.cube_ref_evaluator());
        let visitor = SqlEvaluatorVisitor::new(self.query_tools.clone(), cube_ref_evaluator, None);
        let base_tools = self.query_tools.base_tools();
        let driver_tools = base_tools.driver_tools(false)?;
        let templates = PlanSqlTemplates::try_new(driver_tools, false)?;
        let node_processor = nodes_factory.default_node_processor();

        visitor.apply(symbol, node_processor, &templates)
    }

    /// Creates MockBaseQueryOptions from YAML string
    ///
    /// The YAML structure should match the JS query format:
    /// ```yaml
    /// measures:
    ///   - visitors.visitor_count
    /// dimensions:
    ///   - visitors.source
    /// time_dimensions:
    ///   - dimension: visitors.created_at
    ///     granularity: day
    ///     dateRange:
    ///       - "2024-01-01"
    ///       - "2024-12-31"
    /// order:
    ///   - id: visitors.visitor_count
    ///     desc: true
    /// filters:
    ///   - or:
    ///       - dimension: visitors.visitor_count
    ///         operator: gt
    ///         values:
    ///           - "1"
    ///       - dimension: visitors.source
    ///         operator: equals
    ///         values:
    ///           - google
    ///   - dimension: visitors.created_at
    ///     operator: gte
    ///     values:
    ///       - "2024-01-01"
    /// limit: "100"
    /// offset: "20"
    /// ungrouped: true
    /// ```
    ///
    /// Panics if YAML cannot be parsed.
    pub fn create_query_options_from_yaml(&self, yaml: &str) -> Rc<dyn BaseQueryOptions> {
        let yaml_options: YamlBaseQueryOptions = serde_yaml::from_str(yaml)
            .unwrap_or_else(|e| panic!("Failed to parse YAML query options: {}", e));

        let measures = yaml_options
            .measures
            .map(|m| members_from_strings(m))
            .filter(|m| !m.is_empty());

        let dimensions = yaml_options
            .dimensions
            .map(|d| members_from_strings(d))
            .filter(|d| !d.is_empty());

        let segments = yaml_options
            .segments
            .map(|s| members_from_strings(s))
            .filter(|s| !s.is_empty());

        let order = yaml_options
            .order
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| item.into_order_by_item())
                    .collect::<Vec<_>>()
            })
            .filter(|o| !o.is_empty());

        let filters = yaml_options
            .filters
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| item.into_filter_item())
                    .collect::<Vec<_>>()
            })
            .filter(|f| !f.is_empty());

        let time_dimensions = yaml_options
            .time_dimensions
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| item.into_time_dimension())
                    .collect::<Vec<_>>()
            })
            .filter(|td| !td.is_empty());

        let join_hints = yaml_options.join_hints.map(|hints| {
            hints
                .into_iter()
                .map(|path| {
                    if path.len() == 1 {
                        JoinHintItem::Single(path.into_iter().next().unwrap())
                    } else {
                        JoinHintItem::Vector(path)
                    }
                })
                .collect::<Vec<_>>()
        });

        Rc::new(
            MockBaseQueryOptions::builder()
                .cube_evaluator(self.query_tools.cube_evaluator().clone())
                .base_tools(self.query_tools.base_tools().clone())
                .join_graph(self.query_tools.join_graph().clone())
                .security_context(self.security_context.clone())
                .measures(measures)
                .dimensions(dimensions)
                .segments(segments)
                .time_dimensions(time_dimensions)
                .order(order)
                .filters(filters)
                .join_hints(join_hints)
                .limit(yaml_options.row_limit.clone())
                .row_limit(yaml_options.row_limit)
                .offset(yaml_options.offset)
                .ungrouped(yaml_options.ungrouped)
                .export_annotated_sql(yaml_options.export_annotated_sql.unwrap_or(false))
                .pre_aggregation_query(yaml_options.pre_aggregation_query)
                .total_query(yaml_options.total_query)
                .cubestore_support_multistage(yaml_options.cubestore_support_multistage)
                .disable_external_pre_aggregations(
                    yaml_options
                        .disable_external_pre_aggregations
                        .unwrap_or(false),
                )
                .pre_aggregation_id(yaml_options.pre_aggregation_id)
                .member_to_alias(yaml_options.member_to_alias)
                .masked_members(yaml_options.masked_members)
                .timezone(yaml_options.timezone)
                .build(),
        )
    }

    pub fn create_query_properties(&self, yaml: &str) -> Result<Rc<QueryProperties>, CubeError> {
        let options = self.create_query_options_from_yaml(yaml);
        QueryProperties::try_new(self.query_tools.clone(), options)
    }

    #[allow(dead_code)]
    pub fn build_sql(&self, query: &str) -> Result<String, cubenativeutils::CubeError> {
        let (sql, _) = self.build_sql_with_used_pre_aggregations(query)?;
        Ok(sql)
    }

    #[allow(dead_code)]
    pub fn build_sql_from_options(
        &self,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<String, CubeError> {
        let request = QueryProperties::try_new(self.query_tools.clone(), options)?;
        let planner = TopLevelPlanner::new(request, self.query_tools.clone(), false);
        let (sql, _) = planner.plan()?;
        Ok(sql)
    }

    pub fn build_sql_with_used_pre_aggregations(
        &self,
        query: &str,
    ) -> Result<(String, Vec<Rc<PreAggregation>>), cubenativeutils::CubeError> {
        let options = self.create_query_options_from_yaml(query);
        let ctx = self.for_options(options.as_ref())?;
        let request = QueryProperties::try_new(ctx.query_tools.clone(), options)?;
        let planner = TopLevelPlanner::new(request, ctx.query_tools.clone(), false);
        planner.plan()
    }

    #[cfg(feature = "integration-postgres")]
    pub async fn try_execute_pg(&self, query_yaml: &str, seed_file: &str) -> Option<String> {
        let options = self.create_query_options_from_yaml(query_yaml);
        self.try_execute_pg_from_options(options, seed_file).await
    }

    #[cfg(not(feature = "integration-postgres"))]
    pub async fn try_execute_pg(&self, _query_yaml: &str, _seed_file: &str) -> Option<String> {
        None
    }

    #[cfg(feature = "integration-postgres")]
    pub async fn try_execute_pg_from_options(
        &self,
        options: Rc<dyn BaseQueryOptions>,
        seed_file: &str,
    ) -> Option<String> {
        let client = super::pg_service::connect_and_seed(seed_file).await;

        let ctx = self
            .for_options(options.as_ref())
            .expect("Failed to create context");
        let request = QueryProperties::try_new(ctx.query_tools.clone(), options)
            .expect("Failed to create query properties");
        let planner = TopLevelPlanner::new(request, ctx.query_tools.clone(), false);
        let (raw_sql, pre_aggregations) = planner.plan().expect("Failed to plan query");

        if !pre_aggregations.is_empty() {
            self.create_pre_agg_tables(&client, &pre_aggregations).await;
        }

        let templates = ctx
            .query_tools
            .plan_sql_templates(false)
            .expect("Failed to get SQL templates");
        let (sql, params) = ctx
            .query_tools
            .build_sql_and_params(&raw_sql, true, &templates)
            .expect("Failed to build SQL and params");

        let final_sql = Self::inline_params(&sql, &params);

        let messages = client.simple_query(&final_sql).await.unwrap_or_else(|e| {
            panic!(
                "SQL execution failed:\n{}\nParams: {:?}\n\nError: {:?}",
                final_sql, params, e
            )
        });

        Some(super::integration_context::format_simple_query_results(
            &messages,
        ))
    }

    #[cfg(feature = "integration-postgres")]
    async fn create_pre_agg_tables(
        &self,
        client: &tokio_postgres::Client,
        pre_aggregations: &[Rc<PreAggregation>],
    ) {
        for pre_agg in pre_aggregations {
            let tables = Self::collect_pre_agg_source_tables(pre_agg.source());
            let yaml = Self::build_pre_agg_query_yaml(pre_agg);

            let pa_ctx = Self::new_with_options(self.schema.clone(), Tz::UTC, None, None, false)
                .expect("Failed to create pre-agg context");

            let (raw_sql, _) = pa_ctx
                .build_sql_with_used_pre_aggregations(&yaml)
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to build pre-agg SQL.\nQuery YAML:\n{}\nError: {}",
                        yaml, e
                    )
                });

            let templates = pa_ctx
                .query_tools
                .plan_sql_templates(false)
                .expect("Failed to get SQL templates");
            let (sql, params) = pa_ctx
                .query_tools
                .build_sql_and_params(&raw_sql, true, &templates)
                .expect("Failed to build pre-agg SQL and params");
            let inlined_sql = Self::inline_params(&sql, &params);

            for table in &tables {
                let name = table.alias.clone().unwrap_or_else(|| table.name.clone());
                let table_name =
                    PlanSqlTemplates::alias_name(&format!("{}.{}", table.cube_name, name));

                client
                    .batch_execute(&format!(
                        "DROP TABLE IF EXISTS \"{table_name}\";\n\
                         CREATE TABLE \"{table_name}\" AS ({inlined_sql})"
                    ))
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "Failed to create pre-agg table {}: {}\nSQL: {}",
                            table_name, e, inlined_sql
                        )
                    });
            }
        }
    }

    #[cfg(feature = "integration-postgres")]
    fn collect_pre_agg_source_tables(source: &PreAggregationSource) -> Vec<PreAggregationTable> {
        match source {
            PreAggregationSource::Single(table) => vec![table.clone()],
            PreAggregationSource::Join(join) => {
                let mut tables = Self::collect_pre_agg_source_tables(&join.root);
                for item in &join.items {
                    tables.extend(Self::collect_pre_agg_source_tables(&item.to));
                }
                tables
            }
            PreAggregationSource::Union(union) => {
                union.items.iter().map(|t| t.as_ref().clone()).collect()
            }
        }
    }

    #[cfg(feature = "integration-postgres")]
    fn build_pre_agg_query_yaml(pre_agg: &PreAggregation) -> String {
        let mut yaml = String::new();

        let measures: Vec<String> = pre_agg.measures().iter().map(|m| m.full_name()).collect();
        if !measures.is_empty() {
            yaml.push_str("measures:\n");
            for m in &measures {
                yaml.push_str(&format!("  - {}\n", m));
            }
        }

        // Segments go into dimensions
        // Segments in pre-aggregation are stored as MemberExpression with "expr:" prefix
        let dims: Vec<String> = pre_agg
            .dimensions()
            .iter()
            .map(|d| d.full_name())
            .chain(pre_agg.segments().iter().map(|s| {
                s.full_name()
                    .strip_prefix("expr:")
                    .unwrap_or(&s.full_name())
                    .to_string()
            }))
            .collect();
        if !dims.is_empty() {
            yaml.push_str("dimensions:\n");
            for d in &dims {
                yaml.push_str(&format!("  - {}\n", d));
            }
        }

        if !pre_agg.time_dimensions().is_empty() {
            yaml.push_str("time_dimensions:\n");
            for td in pre_agg.time_dimensions() {
                if let Ok(td_sym) = td.as_time_dimension() {
                    yaml.push_str(&format!(
                        "  - dimension: {}\n",
                        td_sym.base_symbol().full_name()
                    ));
                    if let Some(gran) = td_sym.granularity() {
                        yaml.push_str(&format!("    granularity: {}\n", gran));
                    }
                } else {
                    yaml.push_str(&format!("  - dimension: {}\n", td.full_name()));
                }
            }
        }

        yaml.push_str("pre_aggregation_query: true\n");
        yaml
    }

    #[cfg(not(feature = "integration-postgres"))]
    pub async fn try_execute_pg_from_options(
        &self,
        _options: Rc<dyn BaseQueryOptions>,
        _seed_file: &str,
    ) -> Option<String> {
        None
    }

    #[cfg(feature = "integration-postgres")]
    fn inline_params(sql: &str, params: &[String]) -> String {
        let mut result = sql.to_string();
        for (i, param) in params.iter().enumerate().rev() {
            let placeholder = format!("${}", i + 1);
            let escaped = param.replace('\'', "''");
            result = result.replace(&placeholder, &format!("'{}'", escaped));
        }
        result
    }

    pub fn build_filter_sql(&self, yaml: &str) -> Result<(String, Vec<String>), CubeError> {
        let props = self.create_query_properties(yaml)?;

        let filter = Filter {
            items: props
                .dimensions_filters()
                .iter()
                .chain(props.time_dimensions_filters().iter())
                .chain(props.measures_filters().iter())
                .cloned()
                .collect(),
        };

        let nodes_factory = SqlNodesFactory::default();
        let context = Rc::new(VisitorContext::new(
            self.query_tools.clone(),
            &nodes_factory,
            None,
        ));
        let base_tools = self.query_tools.base_tools();
        let driver_tools = base_tools.driver_tools(false)?;
        let templates = PlanSqlTemplates::try_new(driver_tools, false)?;

        let sql = filter.to_sql(&templates, context)?;
        let params = self.query_tools.get_allocated_params();
        Ok((sql, params))
    }

    pub fn build_base_filter_sql(
        &self,
        base_filter: &Rc<crate::planner::filter::base_filter::BaseFilter>,
    ) -> Result<(String, Vec<String>), CubeError> {
        let nodes_factory = SqlNodesFactory::default();
        let context = Rc::new(VisitorContext::new(
            self.query_tools.clone(),
            &nodes_factory,
            None,
        ));
        let base_tools = self.query_tools.base_tools();
        let driver_tools = base_tools.driver_tools(false)?;
        let templates = PlanSqlTemplates::try_new(driver_tools, false)?;

        let sql = base_filter.to_sql(context, &templates)?;
        let params = self.query_tools.get_allocated_params();
        Ok((sql, params))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::MockSchema;

    #[test]
    fn test_yaml_filter_parsing() {
        use indoc::indoc;

        let yaml = indoc! {"
            filters:
              - or:
                  - dimension: visitors.count
                    operator: gt
                    values:
                      - \"1\"
                  - dimension: visitors.source
                    operator: equals
                    values:
                      - google
              - dimension: visitors.created_at
                operator: gte
                values:
                  - \"2024-01-01\"
        "};
        let parsed: YamlBaseQueryOptions = serde_yaml::from_str(yaml).unwrap();
        let filters = parsed.filters.unwrap();

        println!("Filter count: {}", filters.len());
        for (i, filter) in filters.iter().enumerate() {
            println!("Filter {}: {:?}", i, filter);
        }

        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_create_query_options_from_yaml() {
        use indoc::indoc;

        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let yaml = indoc! {"
            measures:
              - visitors.count
            dimensions:
              - visitors.source
            order:
              - id: visitors.count
                desc: true
            filters:
              - or:
                  - dimension: visitors.count
                    operator: gt
                    values:
                      - \"1\"
                  - dimension: visitors.source
                    operator: equals
                    values:
                      - google
              - dimension: visitors.created_at
                operator: gte
                values:
                  - \"2024-01-01\"
            limit: \"100\"
            offset: \"20\"
            ungrouped: true
        "};

        let options = ctx.create_query_options_from_yaml(yaml);

        // Verify measures
        let measures = options.measures().unwrap().unwrap();
        assert_eq!(measures.len(), 1);

        // Verify dimensions
        let dimensions = options.dimensions().unwrap().unwrap();
        assert_eq!(dimensions.len(), 1);

        // Verify order and filters from static_data
        let static_data = options.static_data();

        let order = static_data.order.as_ref().unwrap();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].id, "visitors.count");
        assert!(order[0].is_desc());

        let filters = static_data.filters.as_ref().unwrap();
        assert_eq!(filters.len(), 2, "Should have 2 filters");

        assert!(filters[0].or.is_some(), "First filter should have 'or'");
        assert!(
            filters[0].and.is_none(),
            "First filter should not have 'and'"
        );

        assert!(
            filters[1].or.is_none(),
            "Second filter should not have 'or': {:?}",
            filters[1].or
        );
        assert!(
            filters[1].and.is_none(),
            "Second filter should not have 'and': {:?}",
            filters[1].and
        );
        assert!(
            filters[1].dimension.is_some(),
            "Second filter: member={:?}, dimension={:?}, operator={:?}, values={:?}",
            filters[1].member,
            filters[1].dimension,
            filters[1].operator,
            filters[1].values
        );

        // Verify other fields
        assert_eq!(static_data.limit, Some("100".to_string()));
        assert_eq!(static_data.offset, Some("20".to_string()));
        assert_eq!(static_data.ungrouped, Some(true));
    }

    #[test]
    fn test_create_query_options_minimal() {
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let yaml = r#"
measures:
  - visitors.count
"#;

        let options = ctx.create_query_options_from_yaml(yaml);
        let measures = options.measures().unwrap().unwrap();
        assert_eq!(measures.len(), 1);

        // All other fields should be None/empty
        assert!(options.dimensions().unwrap().is_none());

        let static_data = options.static_data();
        assert!(static_data.order.is_none());
        assert!(static_data.filters.is_none());
    }

    #[test]
    fn test_time_dimensions_parsing_full() {
        use indoc::indoc;

        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let yaml = indoc! {"
            measures:
              - visitors.count
            time_dimensions:
              - dimension: visitors.created_at
                granularity: day
                dateRange:
                  - \"2024-01-01\"
                  - \"2024-12-31\"
        "};

        let options = ctx.create_query_options_from_yaml(yaml);
        let static_data = options.static_data();

        let time_dimensions = static_data.time_dimensions.as_ref().unwrap();
        assert_eq!(time_dimensions.len(), 1);

        let td = &time_dimensions[0];
        assert_eq!(td.dimension, "visitors.created_at");
        assert_eq!(td.granularity, Some("day".to_string()));

        let date_range = td.date_range.as_ref().unwrap();
        assert_eq!(date_range.len(), 2);
        assert_eq!(date_range[0], "2024-01-01");
        assert_eq!(date_range[1], "2024-12-31");
    }

    #[test]
    fn test_time_dimensions_parsing_minimal() {
        use indoc::indoc;

        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let yaml = indoc! {"
            measures:
              - visitors.count
            time_dimensions:
              - dimension: visitors.created_at
        "};

        let options = ctx.create_query_options_from_yaml(yaml);
        let static_data = options.static_data();

        let time_dimensions = static_data.time_dimensions.as_ref().unwrap();
        assert_eq!(time_dimensions.len(), 1);

        let td = &time_dimensions[0];
        assert_eq!(td.dimension, "visitors.created_at");
        assert_eq!(td.granularity, None);
        assert_eq!(td.date_range, None);
    }

    #[test]
    fn test_time_dimensions_parsing_multiple() {
        use indoc::indoc;

        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let yaml = indoc! {"
            measures:
              - visitors.count
            time_dimensions:
              - dimension: visitors.created_at
                granularity: day
                dateRange:
                  - \"2024-01-01\"
                  - \"2024-12-31\"
              - dimension: visitors.updated_at
                granularity: month
        "};

        let options = ctx.create_query_options_from_yaml(yaml);
        let static_data = options.static_data();

        let time_dimensions = static_data.time_dimensions.as_ref().unwrap();
        assert_eq!(time_dimensions.len(), 2);

        // First time dimension
        let td1 = &time_dimensions[0];
        assert_eq!(td1.dimension, "visitors.created_at");
        assert_eq!(td1.granularity, Some("day".to_string()));
        assert!(td1.date_range.is_some());

        // Second time dimension
        let td2 = &time_dimensions[1];
        assert_eq!(td2.dimension, "visitors.updated_at");
        assert_eq!(td2.granularity, Some("month".to_string()));
        assert_eq!(td2.date_range, None);
    }

    #[test]
    fn test_time_dimensions_with_other_fields() {
        use indoc::indoc;

        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema).unwrap();

        let yaml = indoc! {"
            measures:
              - visitors.count
            dimensions:
              - visitors.source
            time_dimensions:
              - dimension: visitors.created_at
                granularity: day
            filters:
              - dimension: visitors.source
                operator: equals
                values:
                  - google
            order:
              - id: visitors.count
                desc: true
            limit: \"100\"
        "};

        let options = ctx.create_query_options_from_yaml(yaml);
        let static_data = options.static_data();

        // Verify time_dimensions
        let time_dimensions = static_data.time_dimensions.as_ref().unwrap();
        assert_eq!(time_dimensions.len(), 1);
        assert_eq!(time_dimensions[0].dimension, "visitors.created_at");

        // Verify other fields still work
        assert!(options.measures().unwrap().is_some());
        assert!(options.dimensions().unwrap().is_some());
        assert!(static_data.filters.is_some());
        assert!(static_data.order.is_some());
        assert_eq!(static_data.limit, Some("100".to_string()));
    }
}
