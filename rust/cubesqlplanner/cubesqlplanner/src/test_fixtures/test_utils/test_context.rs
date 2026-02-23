use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::logical_plan::PreAggregation;
use crate::planner::filter::base_segment::BaseSegment;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor, TimeDimensionSymbol};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::top_level_planner::TopLevelPlanner;
use crate::planner::{GranularityHelper, QueryProperties};
use crate::test_fixtures::cube_bridge::yaml::YamlBaseQueryOptions;
use crate::test_fixtures::cube_bridge::{
    members_from_strings, MockBaseQueryOptions, MockSchema, MockSecurityContext,
};
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Test context providing query tools and symbol creation helpers
pub struct TestContext {
    query_tools: Rc<QueryTools>,
    security_context: Rc<dyn crate::cube_bridge::security_context::SecurityContext>,
}

impl TestContext {
    pub fn new(schema: MockSchema) -> Result<Self, CubeError> {
        Self::new_with_timezone(schema, Tz::UTC)
    }

    pub fn new_with_timezone(schema: MockSchema, timezone: Tz) -> Result<Self, CubeError> {
        let base_tools = schema.create_base_tools()?;
        let join_graph = Rc::new(schema.create_join_graph()?);
        let evaluator = schema.create_evaluator();
        let security_context: Rc<dyn crate::cube_bridge::security_context::SecurityContext> =
            Rc::new(MockSecurityContext);

        let query_tools = QueryTools::try_new(
            evaluator,
            security_context.clone(),
            Rc::new(base_tools),
            join_graph,
            Some(timezone.to_string()),
            false, // export_annotated_sql
        )?;

        Ok(Self {
            query_tools,
            security_context,
        })
    }

    #[allow(dead_code)]
    pub fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
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
        let expression = self
            .query_tools
            .evaluator_compiler()
            .borrow_mut()
            .compile_sql_call(&cube_name, definition.sql()?)?;
        BaseSegment::try_new(
            expression,
            cube_name,
            name,
            Some(path.to_string()),
            self.query_tools.clone(),
        )
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
        let visitor = SqlEvaluatorVisitor::new(self.query_tools.clone(), None);
        let base_tools = self.query_tools.base_tools();
        let driver_tools = base_tools.driver_tools(false)?;
        let templates = PlanSqlTemplates::try_new(driver_tools, false)?;
        let node_processor = SqlNodesFactory::default().default_node_processor();

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
                .limit(yaml_options.limit)
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
    pub fn build_sql_with_used_pre_aggregations(
        &self,
        query: &str,
    ) -> Result<(String, Vec<Rc<PreAggregation>>), cubenativeutils::CubeError> {
        let options = self.create_query_options_from_yaml(query);
        let request = QueryProperties::try_new(self.query_tools.clone(), options.clone())?;
        let planner = TopLevelPlanner::new(request, self.query_tools.clone(), false);
        planner.plan()
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
