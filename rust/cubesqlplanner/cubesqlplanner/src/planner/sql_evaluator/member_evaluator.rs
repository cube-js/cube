use super::dependecy::Dependency;
use super::{
    Compiler, CubeNameEvaluator, CubeTableEvaluator, CubeTableEvaluatorFactory, DimensionEvaluator,
    JoinConditionEvaluator, MeasureEvaluator, MeasureFilterEvaluator,
    MeasureFilterEvaluatorFactory,
};
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
    CubeTable(CubeTableEvaluator),
    JoinCondition(JoinConditionEvaluator),
    MeasureFilter(MeasureFilterEvaluator),
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

    pub fn new_cube_table(evaluator: CubeTableEvaluator, deps: Vec<Dependency>) -> Rc<Self> {
        Rc::new(Self {
            evaluator: MemberEvaluatorType::CubeTable(evaluator),
            deps,
        })
    }

    pub fn new_join_condition(
        evaluator: JoinConditionEvaluator,
        deps: Vec<Dependency>,
    ) -> Rc<Self> {
        Rc::new(Self {
            evaluator: MemberEvaluatorType::JoinCondition(evaluator),
            deps,
        })
    }

    pub fn new_measure_filter(
        evaluator: MeasureFilterEvaluator,
        deps: Vec<Dependency>,
    ) -> Rc<Self> {
        Rc::new(Self {
            evaluator: MemberEvaluatorType::MeasureFilter(evaluator),
            deps,
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
    fn evaluator_name() -> String; //FIXME maybe Enum should be used
    fn is_cachable() -> bool {
        true
    }
    fn cube_name(&self) -> &String;
    fn deps_names(&self) -> Result<Vec<String>, CubeError>;
    fn member_sql(&self) -> Option<Rc<dyn MemberSql>>;
    fn build(
        self,
        deps: Vec<Dependency>,
        compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError>;
}
