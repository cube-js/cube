use crate::planner::sql_evaluator::EvaluationNode;
use crate::planner::BaseDimension;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Clone)]
pub struct MultiStageApplyedState {
    dimensions: Vec<Rc<BaseDimension>>,
}

impl MultiStageApplyedState {
    pub fn new(dimensions: Vec<Rc<BaseDimension>>) -> Rc<Self> {
        Rc::new(Self { dimensions })
    }

    pub fn add_dimension(self: &Rc<Self>, dimension: Rc<BaseDimension>) -> Rc<Self> {
        self.add_dimensions(vec![dimension])
    }

    pub fn add_dimensions(self: &Rc<Self>, dimensions: Vec<Rc<BaseDimension>>) -> Rc<Self> {
        let new_dimensions = self
            .dimensions
            .iter()
            .cloned()
            .chain(dimensions.into_iter())
            .unique_by(|d| d.member_evaluator().full_name())
            .collect_vec();

        Rc::new(Self {
            dimensions: new_dimensions,
        })
    }

    pub fn dimensions(&self) -> &Vec<Rc<BaseDimension>> {
        &self.dimensions
    }
}

impl PartialEq for MultiStageApplyedState {
    fn eq(&self, other: &Self) -> bool {
        if !self.dimensions.len() == other.dimensions.len() {
            false
        } else {
            self.dimensions
                .iter()
                .zip(other.dimensions.iter())
                .all(|(a, b)| a.member_evaluator().full_name() == b.member_evaluator().full_name())
        }
    }
}

impl Debug for MultiStageApplyedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageApplyedState")
            .field(
                "dimensions",
                &self
                    .dimensions
                    .iter()
                    .map(|d| d.member_evaluator().full_name())
                    .join(", "),
            )
            .finish()
    }
}
