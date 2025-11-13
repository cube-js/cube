use super::collectors::JoinHintsCollector;
use super::symbols::MemberSymbol;
use super::{
    CubeNameSymbolFactory, CubeTableSymbolFactory, DimensionSymbolFactory, MeasureSymbolFactory,
    SqlCall, SymbolFactory, TraversalVisitor,
};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::security_context::SecurityContext;
use crate::cube_bridge::sql_utils::SqlUtils;
use crate::planner::sql_evaluator::sql_call_builder::SqlCallBuilder;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;
pub struct Compiler {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    sql_utils: Rc<dyn SqlUtils>,
    security_context: Rc<dyn SecurityContext>,
    timezone: Tz,
    /* (type, name) */
    members: HashMap<(String, String), Rc<MemberSymbol>>,
}

impl Compiler {
    pub fn new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        sql_utils: Rc<dyn SqlUtils>,
        security_context: Rc<dyn SecurityContext>,
        timezone: Tz,
    ) -> Self {
        Self {
            cube_evaluator,
            security_context,
            sql_utils,
            timezone,
            members: HashMap::new(),
        }
    }

    pub fn add_auto_resolved_member_evaluator(
        &mut self,
        name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = name.split(".").map(|s| s.to_string()).collect::<Vec<_>>();
        if self.cube_evaluator.is_measure(path.clone())? {
            Ok(self.add_measure_evaluator(name)?)
        } else if self.cube_evaluator.is_dimension(path.clone())? {
            Ok(self.add_dimension_evaluator(name)?)
        } else {
            Err(CubeError::internal(format!(
                "Cannot resolve evaluator of member {}. Only dimensions and measures can be autoresolved",
                name
            )))
        }
    }

    pub fn add_measure_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member::<MeasureSymbolFactory>(&measure) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &measure,
                MeasureSymbolFactory::try_new(&measure, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn add_dimension_evaluator(
        &mut self,
        dimension: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member::<DimensionSymbolFactory>(&dimension) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &dimension,
                DimensionSymbolFactory::try_new(&dimension, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn add_cube_name_evaluator(
        &mut self,
        cube_name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member::<CubeNameSymbolFactory>(&cube_name) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &cube_name,
                CubeNameSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn add_cube_table_evaluator(
        &mut self,
        cube_name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member::<CubeTableSymbolFactory>(&cube_name) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &cube_name,
                CubeTableSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn join_hints(&self) -> Result<Vec<JoinHintItem>, CubeError> {
        let mut collector = JoinHintsCollector::new();
        for member in self.members.values() {
            collector.apply(member, &())?;
        }
        Ok(collector.extract_result())
    }

    pub fn timezone(&self) -> Tz {
        self.timezone.clone()
    }

    pub fn compile_sql_call(
        &mut self,
        cube_name: &String,
        member_sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<SqlCall>, CubeError> {
        let call_builder = SqlCallBuilder::new(
            self,
            self.cube_evaluator.clone(),
            self.sql_utils.clone(),
            self.security_context.clone(),
        );
        let sql_call = call_builder.build(&cube_name, member_sql.clone())?;
        Ok(Rc::new(sql_call))
    }

    fn exists_member<T: SymbolFactory>(&self, full_name: &String) -> Option<Rc<MemberSymbol>> {
        if T::is_cachable() {
            let key = (T::symbol_name(), full_name.clone());
            self.members.get(&key).cloned()
        } else {
            None
        }
    }

    fn add_evaluator_impl<T: SymbolFactory + 'static>(
        &mut self,
        full_name: &String,
        factory: T,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let node = factory.build(self)?;
        let key = (T::symbol_name().to_string(), full_name.clone());
        if T::is_cachable() {
            self.members.insert(key, node.clone());
        }
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::{MockCubeEvaluator, MockSecurityContext, MockSqlUtils};
    use crate::test_fixtures::schemas::create_visitors_schema;
    use chrono_tz::Tz;

    #[test]
    fn test_add_dimension_evaluator_number_dimension() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        let symbol = compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();

        // Check symbol type
        assert!(symbol.is_dimension());
        assert!(!symbol.is_measure());

        // Check full name
        assert_eq!(symbol.full_name(), "visitors.id");

        // Check cube name and member name
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "id");

        // Check no dependencies for simple dimension
        let dependencies = symbol.get_dependencies();
        assert_eq!(
            dependencies.len(),
            0,
            "Simple dimension should have no dependencies"
        );

        // Check dimension type
        let dimension = symbol.as_dimension().unwrap();
        assert_eq!(dimension.dimension_type(), "number");
    }

    #[test]
    fn test_add_dimension_evaluator_string_dimension() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        let symbol = compiler
            .add_dimension_evaluator("visitors.source".to_string())
            .unwrap();

        // Check symbol type
        assert!(symbol.is_dimension());
        assert!(!symbol.is_measure());

        // Check full name
        assert_eq!(symbol.full_name(), "visitors.source");

        // Check cube name and member name
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "source");

        // Check no dependencies for simple dimension
        let dependencies = symbol.get_dependencies();
        assert_eq!(
            dependencies.len(),
            0,
            "Simple dimension should have no dependencies"
        );

        // Check dimension type
        let dimension = symbol.as_dimension().unwrap();
        assert_eq!(dimension.dimension_type(), "string");
    }

    #[test]
    fn test_add_dimension_evaluator_caching() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        // Add dimension twice
        let symbol1 = compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();
        let symbol2 = compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();

        // Should return the same cached instance
        assert_eq!(
            symbol1.full_name(),
            symbol2.full_name(),
            "Cached symbols should have the same full name"
        );
    }

    #[test]
    fn test_add_dimension_evaluator_invalid_path() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        // Try to add non-existent dimension
        let result = compiler.add_dimension_evaluator("nonexistent.dimension".to_string());

        assert!(result.is_err(), "Should fail for non-existent dimension");
    }

    #[test]
    fn test_add_dimension_evaluator_multiple_dimensions() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        // Add multiple different dimensions
        let id_symbol = compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();
        let source_symbol = compiler
            .add_dimension_evaluator("visitors.source".to_string())
            .unwrap();
        let created_at_symbol = compiler
            .add_dimension_evaluator("visitors.created_at".to_string())
            .unwrap();

        // Verify each dimension
        assert_eq!(id_symbol.full_name(), "visitors.id");
        assert_eq!(id_symbol.as_dimension().unwrap().dimension_type(), "number");

        assert_eq!(source_symbol.full_name(), "visitors.source");
        assert_eq!(
            source_symbol.as_dimension().unwrap().dimension_type(),
            "string"
        );

        assert_eq!(created_at_symbol.full_name(), "visitors.created_at");
        assert_eq!(
            created_at_symbol.as_dimension().unwrap().dimension_type(),
            "time"
        );

        // All should have no dependencies
        assert_eq!(id_symbol.get_dependencies().len(), 0);
        assert_eq!(source_symbol.get_dependencies().len(), 0);
        assert_eq!(created_at_symbol.get_dependencies().len(), 0);
    }

    #[test]
    fn test_add_measure_evaluator_count_measure() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        let symbol = compiler
            .add_measure_evaluator("visitor_checkins.count".to_string())
            .unwrap();

        // Check symbol type
        assert!(symbol.is_measure());
        assert!(!symbol.is_dimension());

        // Check full name
        assert_eq!(symbol.full_name(), "visitor_checkins.count");

        // Check cube name and member name
        assert_eq!(symbol.cube_name(), "visitor_checkins");
        assert_eq!(symbol.name(), "count");

        // Check no dependencies for simple measure
        let dependencies = symbol.get_dependencies();
        assert_eq!(
            dependencies.len(),
            0,
            "Simple measure should have no dependencies"
        );

        // Check measure type
        let measure = symbol.as_measure().unwrap();
        assert_eq!(measure.measure_type(), "count");
    }

    #[test]
    fn test_add_measure_evaluator_sum_measure() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        let symbol = compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        // Check symbol type
        assert!(symbol.is_measure());
        assert!(!symbol.is_dimension());

        // Check full name
        assert_eq!(symbol.full_name(), "visitors.total_revenue");

        // Check cube name and member name
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "total_revenue");

        // Check no dependencies for simple measure
        let dependencies = symbol.get_dependencies();
        assert_eq!(
            dependencies.len(),
            0,
            "Simple measure should have no dependencies"
        );

        // Check measure type
        let measure = symbol.as_measure().unwrap();
        assert_eq!(measure.measure_type(), "sum");
    }

    #[test]
    fn test_add_measure_evaluator_caching() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        // Add measure twice
        let symbol1 = compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();
        let symbol2 = compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        // Should return the same cached instance
        assert_eq!(
            symbol1.full_name(),
            symbol2.full_name(),
            "Cached symbols should have the same full name"
        );
    }

    #[test]
    fn test_add_measure_evaluator_invalid_path() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        // Try to add non-existent measure
        let result = compiler.add_measure_evaluator("nonexistent.measure".to_string());

        assert!(result.is_err(), "Should fail for non-existent measure");
    }

    #[test]
    fn test_add_measure_evaluator_multiple_measures() {
        let schema = create_visitors_schema();
        let evaluator = Rc::new(MockCubeEvaluator::new(schema));
        let sql_utils = Rc::new(MockSqlUtils);
        let security_context = Rc::new(MockSecurityContext);
        let timezone = Tz::UTC;

        let mut compiler = Compiler::new(evaluator, sql_utils, security_context, timezone);

        // Add multiple different measures
        let count_symbol = compiler
            .add_measure_evaluator("visitor_checkins.count".to_string())
            .unwrap();
        let revenue_symbol = compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        // Verify each measure
        assert_eq!(count_symbol.full_name(), "visitor_checkins.count");
        assert_eq!(count_symbol.as_measure().unwrap().measure_type(), "count");

        assert_eq!(revenue_symbol.full_name(), "visitors.total_revenue");
        assert_eq!(revenue_symbol.as_measure().unwrap().measure_type(), "sum");

        // All should have no dependencies
        assert_eq!(count_symbol.get_dependencies().len(), 0);
        assert_eq!(revenue_symbol.get_dependencies().len(), 0);
    }
}
