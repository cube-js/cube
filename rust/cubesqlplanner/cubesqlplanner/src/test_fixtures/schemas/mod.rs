pub mod simple_schema;
pub mod visitors_schema;

pub use simple_schema::create_simple_schema;
pub use visitors_schema::create_visitors_schema;

use crate::planner::sql_evaluator::Compiler;
use crate::test_fixtures::cube_bridge::{MockCubeEvaluator, MockSecurityContext, MockSqlUtils};
use chrono_tz::Tz;
use std::rc::Rc;

/// Helper struct that bundles together a Compiler with its dependencies for testing
pub struct TestCompiler {
    pub compiler: Compiler,
}

impl TestCompiler {
    /// Create a new TestCompiler from a MockCubeEvaluator
    pub fn new(evaluator: Rc<MockCubeEvaluator>) -> Self {
        Self::new_with_timezone(evaluator, Tz::UTC)
    }

    /// Create a new TestCompiler with a specific timezone
    pub fn new_with_timezone(evaluator: Rc<MockCubeEvaluator>, timezone: Tz) -> Self {
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        Self { compiler }
    }
}
