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
    use crate::test_fixtures::schemas::{create_visitors_schema, TestCompiler};

    #[test]
    fn test_add_dimension_evaluator_number_dimension() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();

        assert!(symbol.is_dimension());
        assert!(!symbol.is_measure());
        assert_eq!(symbol.full_name(), "visitors.id");
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "id");
        assert_eq!(symbol.get_dependencies().len(), 0);
        assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "number");
    }

    #[test]
    fn test_add_dimension_evaluator_string_dimension() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.source".to_string())
            .unwrap();

        assert!(symbol.is_dimension());
        assert!(!symbol.is_measure());
        assert_eq!(symbol.full_name(), "visitors.source");
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "source");
        assert_eq!(symbol.get_dependencies().len(), 0);
        assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "string");
    }

    #[test]
    fn test_add_dimension_evaluator_caching() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol1 = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();
        let symbol2 = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();

        assert_eq!(symbol1.full_name(), symbol2.full_name());
    }

    #[test]
    fn test_add_dimension_evaluator_invalid_path() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let result = test_compiler
            .compiler
            .add_dimension_evaluator("nonexistent.dimension".to_string());

        assert!(result.is_err());
    }

    #[test]
    fn test_add_dimension_evaluator_multiple_dimensions() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let id_symbol = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.id".to_string())
            .unwrap();
        let source_symbol = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.source".to_string())
            .unwrap();
        let created_at_symbol = test_compiler
            .compiler
            .add_dimension_evaluator("visitors.created_at".to_string())
            .unwrap();

        assert_eq!(id_symbol.full_name(), "visitors.id");
        assert_eq!(id_symbol.as_dimension().unwrap().dimension_type(), "number");
        assert_eq!(source_symbol.full_name(), "visitors.source");
        assert_eq!(source_symbol.as_dimension().unwrap().dimension_type(), "string");
        assert_eq!(created_at_symbol.full_name(), "visitors.created_at");
        assert_eq!(created_at_symbol.as_dimension().unwrap().dimension_type(), "time");
        assert_eq!(id_symbol.get_dependencies().len(), 0);
        assert_eq!(source_symbol.get_dependencies().len(), 0);
        assert_eq!(created_at_symbol.get_dependencies().len(), 0);
    }

    #[test]
    fn test_add_measure_evaluator_count_measure() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol = test_compiler
            .compiler
            .add_measure_evaluator("visitor_checkins.count".to_string())
            .unwrap();

        assert!(symbol.is_measure());
        assert!(!symbol.is_dimension());
        assert_eq!(symbol.full_name(), "visitor_checkins.count");
        assert_eq!(symbol.cube_name(), "visitor_checkins");
        assert_eq!(symbol.name(), "count");
        assert_eq!(symbol.get_dependencies().len(), 0);
        assert_eq!(symbol.as_measure().unwrap().measure_type(), "count");
    }

    #[test]
    fn test_add_measure_evaluator_sum_measure() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol = test_compiler
            .compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        assert!(symbol.is_measure());
        assert!(!symbol.is_dimension());
        assert_eq!(symbol.full_name(), "visitors.total_revenue");
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "total_revenue");
        assert_eq!(symbol.get_dependencies().len(), 0);
        assert_eq!(symbol.as_measure().unwrap().measure_type(), "sum");
    }

    #[test]
    fn test_add_measure_evaluator_caching() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol1 = test_compiler
            .compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();
        let symbol2 = test_compiler
            .compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        assert_eq!(symbol1.full_name(), symbol2.full_name());
    }

    #[test]
    fn test_add_measure_evaluator_invalid_path() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let result = test_compiler
            .compiler
            .add_measure_evaluator("nonexistent.measure".to_string());

        assert!(result.is_err());
    }

    #[test]
    fn test_add_measure_evaluator_multiple_measures() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let count_symbol = test_compiler
            .compiler
            .add_measure_evaluator("visitor_checkins.count".to_string())
            .unwrap();
        let revenue_symbol = test_compiler
            .compiler
            .add_measure_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        assert_eq!(count_symbol.full_name(), "visitor_checkins.count");
        assert_eq!(count_symbol.as_measure().unwrap().measure_type(), "count");
        assert_eq!(revenue_symbol.full_name(), "visitors.total_revenue");
        assert_eq!(revenue_symbol.as_measure().unwrap().measure_type(), "sum");
        assert_eq!(count_symbol.get_dependencies().len(), 0);
        assert_eq!(revenue_symbol.get_dependencies().len(), 0);
    }

    #[test]
    fn test_add_auto_resolved_member_evaluator_dimension() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol = test_compiler
            .compiler
            .add_auto_resolved_member_evaluator("visitors.source".to_string())
            .unwrap();

        assert!(symbol.is_dimension());
        assert!(!symbol.is_measure());
        assert_eq!(symbol.full_name(), "visitors.source");
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "source");
        assert_eq!(symbol.get_dependencies().len(), 0);
        assert_eq!(symbol.as_dimension().unwrap().dimension_type(), "string");
    }

    #[test]
    fn test_add_auto_resolved_member_evaluator_measure() {
        let evaluator = create_visitors_schema().create_evaluator();
        let mut test_compiler = TestCompiler::new(evaluator);

        let symbol = test_compiler
            .compiler
            .add_auto_resolved_member_evaluator("visitors.total_revenue".to_string())
            .unwrap();

        assert!(symbol.is_measure());
        assert!(!symbol.is_dimension());
        assert_eq!(symbol.full_name(), "visitors.total_revenue");
        assert_eq!(symbol.cube_name(), "visitors");
        assert_eq!(symbol.name(), "total_revenue");
        assert_eq!(symbol.get_dependencies().len(), 0);
        assert_eq!(symbol.as_measure().unwrap().measure_type(), "sum");
    }
}