use super::dependecy::DependenciesBuilder;
use super::join_hints_collector::JoinHintsCollector;
use super::{
    CubeNameEvaluatorFactory, DimensionEvaluator, DimensionEvaluatorFactory, EvaluationNode,
    MeasureEvaluator, MeasureEvaluatorFactory, MemberEvaluator, MemberEvaluatorFactory,
    TraversalVisitor,
};
use crate::cube_bridge::evaluator::CubeEvaluator;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
pub struct Compiler {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    members: HashMap<String, Rc<EvaluationNode>>,
}

impl Compiler {
    pub fn new(cube_evaluator: Rc<dyn CubeEvaluator>) -> Self {
        Self {
            cube_evaluator,
            members: HashMap::new(),
        }
    }

    pub fn add_evaluator<T: MemberEvaluatorFactory + 'static>(
        &mut self,
        full_name: &String,
        factory: T,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        if let Some(exists) = self.members.get(full_name) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(full_name, factory)
        }
    }

    pub fn add_measure_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        if let Some(exists) = self.members.get(&measure) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &measure,
                MeasureEvaluatorFactory::try_new(&measure, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn add_dimension_evaluator(
        &mut self,
        dimension: String,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        if let Some(exists) = self.members.get(&dimension) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &dimension,
                DimensionEvaluatorFactory::try_new(&dimension, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn add_cube_name_evaluator(
        &mut self,
        cube_name: String,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        if let Some(exists) = self.members.get(&cube_name) {
            Ok(exists.clone())
        } else {
            self.add_evaluator_impl(
                &cube_name,
                CubeNameEvaluatorFactory::try_new(&cube_name, self.cube_evaluator.clone())?,
            )
        }
    }

    pub fn join_hints(&self) -> Vec<String> {
        let mut collector = JoinHintsCollector::new();
        for member in self.members.values() {
            collector.apply(member);
        }
        collector.extract_result()
    }

    fn add_evaluator_impl<T: MemberEvaluatorFactory + 'static>(
        &mut self,
        full_name: &String,
        factory: T,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let dep_names = factory.deps_names()?;
        let cube_name = factory.cube_name();
        let dep_builder = DependenciesBuilder::new(self, self.cube_evaluator.clone());
        let deps = dep_builder.build(cube_name.clone(), factory.member_sql())?;

        let node = factory.build(deps)?;
        self.members.insert(full_name.clone(), node.clone());
        Ok(node)
    }
}
