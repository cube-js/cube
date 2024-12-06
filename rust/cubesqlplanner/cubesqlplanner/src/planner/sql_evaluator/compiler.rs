use super::collectors::JoinHintsCollector;
use super::dependecy::DependenciesBuilder;
use super::{
    CubeNameSymbolFactory, CubeTableSymbolFactory, DimensionSymbolFactory, EvaluationNode,
    MeasureSymbolFactory, SimpleSqlSymbolFactory, SymbolFactory, TraversalVisitor,
};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::MemberSql;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;
pub struct Compiler {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    members: HashMap<(String, String), Rc<EvaluationNode>>,
}

impl Compiler {
    pub fn new(cube_evaluator: Rc<dyn CubeEvaluator>) -> Self {
        Self {
            cube_evaluator,
            members: HashMap::new(),
        }
    }

    pub fn add_evaluator<T: SymbolFactory + 'static>(
        &mut self,
        full_name: &String,
        factory: T,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        if let Some(exists) = self.exists_member::<T>(full_name) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(full_name, factory)
        }
    }

    pub fn add_measure_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
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
    ) -> Result<Rc<EvaluationNode>, CubeError> {
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
    ) -> Result<Rc<EvaluationNode>, CubeError> {
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
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        if let Some(exists) = self.exists_member::<CubeTableSymbolFactory>(&cube_name) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &cube_name,
                CubeTableSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn add_join_condition_evaluator(
        &mut self,
        cube_name: String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        self.add_evaluator_impl(
            &cube_name,
            SimpleSqlSymbolFactory::try_new(&cube_name, sql)?,
        )
    }

    pub fn add_simple_sql_evaluator(
        &mut self,
        cube_name: String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        self.add_evaluator_impl(
            &cube_name,
            SimpleSqlSymbolFactory::try_new(&cube_name, sql)?,
        )
    }

    pub fn join_hints(&self) -> Result<Vec<String>, CubeError> {
        let mut collector = JoinHintsCollector::new();
        for member in self.members.values() {
            collector.apply(member)?;
        }
        Ok(collector.extract_result())
    }

    fn exists_member<T: SymbolFactory>(&self, full_name: &String) -> Option<Rc<EvaluationNode>> {
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
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let cube_name = factory.cube_name();
        let dep_builder = DependenciesBuilder::new(self, self.cube_evaluator.clone());
        let deps = dep_builder.build(cube_name.clone(), factory.member_sql())?;

        let node = factory.build(deps, self)?;
        let key = (T::symbol_name().to_string(), full_name.clone());
        if T::is_cachable() {
            self.members.insert(key, node.clone());
        }
        Ok(node)
    }
}
