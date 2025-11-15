use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::test_fixtures::cube_bridge::{MockSchema, MockSecurityContext};
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Test context providing query tools and symbol creation helpers
pub struct TestContext {
    query_tools: Rc<QueryTools>,
}

impl TestContext {
    /// Creates new test context from a mock schema with UTC timezone
    pub fn new(schema: MockSchema) -> Result<Self, CubeError> {
        Self::new_with_timezone(schema, Tz::UTC)
    }

    /// Creates new test context from a mock schema with specific timezone
    pub fn new_with_timezone(schema: MockSchema, timezone: Tz) -> Result<Self, CubeError> {
        let base_tools = schema.create_base_tools()?;
        let join_graph = Rc::new(schema.create_join_graph()?);
        let evaluator = schema.create_evaluator();
        let security_context: Rc<dyn crate::cube_bridge::security_context::SecurityContext> =
            Rc::new(MockSecurityContext);

        let query_tools = QueryTools::try_new(
            evaluator,
            security_context,
            Rc::new(base_tools),
            join_graph,
            Some(timezone.to_string()),
            false, // export_annotated_sql
        )?;

        Ok(Self { query_tools })
    }

    /// Returns reference to query tools
    pub fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
    }

    /// Creates a symbol from cube.member path
    pub fn create_symbol(&self, member_path: &str) -> Result<Rc<MemberSymbol>, CubeError> {
        self.query_tools
            .evaluator_compiler()
            .borrow_mut()
            .add_auto_resolved_member_evaluator(member_path.to_string())
    }
}
