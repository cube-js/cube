use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
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

        Rc::new(
            MockBaseQueryOptions::builder()
                .cube_evaluator(self.query_tools.cube_evaluator().clone())
                .base_tools(self.query_tools.base_tools().clone())
                .join_graph(self.query_tools.join_graph().clone())
                .security_context(self.security_context.clone())
                .measures(measures)
                .dimensions(dimensions)
                .segments(segments)
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
                .build(),
        )
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
}
