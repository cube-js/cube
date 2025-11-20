use crate::planner::sql_evaluator::Compiler;
use crate::test_fixtures::cube_bridge::{MockBaseTools, MockCubeEvaluator, MockSecurityContext};
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
        let base_tools = Rc::new(MockBaseTools::default());
        let security_context = Rc::new(MockSecurityContext);
        let compiler = Compiler::new(evaluator, base_tools, security_context, timezone);

        Self { compiler }
    }
}
