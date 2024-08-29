use super::dependecy::Dependency;
use super::{CubeNameEvaluator, DimensionEvaluator, MeasureEvaluator};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
pub trait MemberEvaluator {
    fn cube_name(&self) -> &String;
}

pub enum MemberEvaluatorType {
    Dimension(DimensionEvaluator),
    Measure(MeasureEvaluator),
    CubeName(CubeNameEvaluator),
}

pub struct EvaluationNode {
    evaluator: MemberEvaluatorType,
    deps: Vec<Dependency>,
}

impl EvaluationNode {
    pub fn new(evaluator: MemberEvaluatorType, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self { evaluator, deps })
    }

    pub fn new_measure(evaluator: MeasureEvaluator, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            evaluator: MemberEvaluatorType::Measure(evaluator),
            deps,
        })
    }

    pub fn new_dimension(evaluator: DimensionEvaluator, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            evaluator: MemberEvaluatorType::Dimension(evaluator),
            deps,
        })
    }

    pub fn new_cube_name(evaluator: CubeNameEvaluator) -> Rc<Self> {
        Rc::new(Self {
            evaluator: MemberEvaluatorType::CubeName(evaluator),
            deps: vec![],
        })
    }

    pub fn deps(&self) -> &Vec<Dependency> {
        &self.deps
    }

    pub fn evaluator(&self) -> &MemberEvaluatorType {
        &self.evaluator
    }
}

pub trait MemberEvaluatorFactory: Sized {
    fn cube_name(&self) -> &String;
    fn deps_names(&self) -> Result<Vec<String>, CubeError>;
    fn member_sql(&self) -> Option<Rc<dyn MemberSql>>;
    fn build(self, deps: Vec<Dependency>) -> Result<Rc<EvaluationNode>, CubeError>;
}
