use crate::planner::base_measure::MeasureTimeShift;
use crate::planner::BaseDimension;
use itertools::Itertools;
use std::cmp::PartialEq;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Clone)]
pub struct MultiStageAppliedState {
    dimensions: Vec<Rc<BaseDimension>>,
    allowed_filter_members: HashSet<String>,
    time_shifts: HashMap<String, String>,
}

impl MultiStageAppliedState {
    pub fn new(
        dimensions: Vec<Rc<BaseDimension>>,
        allowed_filter_members: HashSet<String>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimensions,
            allowed_filter_members,
            time_shifts: HashMap::new(),
        })
    }

    pub fn clone_state(&self) -> Self {
        Self {
            dimensions: self.dimensions.clone(),
            allowed_filter_members: self.allowed_filter_members.clone(),
            time_shifts: self.time_shifts.clone(),
        }
    }

    pub fn add_dimensions(&mut self, dimensions: Vec<Rc<BaseDimension>>) {
        self.dimensions = self
            .dimensions
            .iter()
            .cloned()
            .chain(dimensions.into_iter())
            .unique_by(|d| d.member_evaluator().full_name())
            .collect_vec();
    }

    pub fn add_time_shifts(&mut self, time_shifts: Vec<MeasureTimeShift>) {
        for ts in time_shifts.into_iter() {
            self.time_shifts
                .insert(ts.time_dimension.clone(), ts.interval.clone());
        }
    }

    pub fn time_shifts(&self) -> &HashMap<String, String> {
        &self.time_shifts
    }

    pub fn is_filter_allowed(&self, name: &str) -> bool {
        self.allowed_filter_members.contains(name)
    }

    pub fn allowed_filter_members(&self) -> &HashSet<String> {
        &self.allowed_filter_members
    }

    pub fn disallow_filter(&mut self, name: &str) {
        self.allowed_filter_members.take(name);
    }

    pub fn dimensions(&self) -> &Vec<Rc<BaseDimension>> {
        &self.dimensions
    }
}

impl PartialEq for MultiStageAppliedState {
    fn eq(&self, other: &Self) -> bool {
        let dims_eq = if !self.dimensions.len() == other.dimensions.len() {
            false
        } else {
            self.dimensions
                .iter()
                .zip(other.dimensions.iter())
                .all(|(a, b)| a.member_evaluator().full_name() == b.member_evaluator().full_name())
        };
        dims_eq
            && self.allowed_filter_members == other.allowed_filter_members
            && self.time_shifts == other.time_shifts
    }
}

impl Debug for MultiStageAppliedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageAppliedState")
            .field(
                "dimensions",
                &self
                    .dimensions
                    .iter()
                    .map(|d| d.member_evaluator().full_name())
                    .join(", "),
            )
            .field("allowed_filter_members", &self.allowed_filter_members)
            .field("time_shifts", &self.time_shifts)
            .finish()
    }
}
