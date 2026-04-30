use super::{MultiStageAppliedState, MultiStageMember};
use crate::logical_plan::LogicalSchema;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::fmt::Debug;
use std::rc::Rc;

pub struct MultiStageQueryDescription {
    member: Rc<MultiStageMember>,
    state: Rc<MultiStageAppliedState>,
    input: Vec<Rc<MultiStageQueryDescription>>,
    alias: String,
}

impl Debug for MultiStageQueryDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageQueryDescription")
            .field(
                "member_node",
                &format!("node with path {}", self.member_node().full_name()),
            )
            .field("state", &self.state)
            .field("input", &self.input)
            .field("alias", &self.alias)
            .finish()
    }
}

impl MultiStageQueryDescription {
    pub fn new(
        member: Rc<MultiStageMember>,
        state: Rc<MultiStageAppliedState>,
        input: Vec<Rc<MultiStageQueryDescription>>,
        alias: String,
    ) -> Rc<Self> {
        Rc::new(Self {
            member,
            state,
            input,
            alias,
        })
    }

    pub fn schema(&self) -> Rc<LogicalSchema> {
        LogicalSchema::default()
            .set_time_dimensions(self.state.time_dimensions().clone())
            .set_dimensions(self.state.dimensions().clone())
            .set_measures(vec![self.member_node().clone()])
            .into_rc()
    }
    pub fn member_node(&self) -> &Rc<MemberSymbol> {
        &self.member.evaluation_node()
    }

    pub fn is_multi_stage_dimension(&self) -> bool {
        self.member.member_type().is_multi_stage_dimension()
    }

    pub fn member(&self) -> &Rc<MultiStageMember> {
        &self.member
    }

    pub fn state(&self) -> Rc<MultiStageAppliedState> {
        self.state.clone()
    }

    pub fn member_name(&self) -> String {
        self.member_node().full_name()
    }

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn input(&self) -> &Vec<Rc<MultiStageQueryDescription>> {
        &self.input
    }

    pub fn is_leaf(&self) -> bool {
        self.input.is_empty()
    }

    pub fn collect_all_non_multi_stage_dimension(
        &self,
    ) -> Result<(Vec<Rc<MemberSymbol>>, Vec<Rc<MemberSymbol>>), CubeError> {
        let mut dimensions = vec![];
        let mut time_dimensions = vec![];
        self.collect_all_non_multi_stage_dimension_impl(&mut dimensions, &mut time_dimensions);
        let dimensions = dimensions
            .into_iter()
            .unique_by(|d| d.full_name())
            .filter_map(|d| match d.is_basic_dimension() {
                Ok(res) => {
                    if res {
                        None
                    } else {
                        Some(Ok(d))
                    }
                }
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let time_dimensions = time_dimensions
            .into_iter()
            .unique_by(|d| d.full_name())
            .filter_map(|d| match d.is_basic_dimension() {
                Ok(res) => {
                    if res {
                        None
                    } else {
                        Some(Ok(d))
                    }
                }
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((dimensions, time_dimensions))
    }

    fn collect_all_non_multi_stage_dimension_impl(
        &self,
        dimensions: &mut Vec<Rc<MemberSymbol>>,
        time_dimensions: &mut Vec<Rc<MemberSymbol>>,
    ) {
        dimensions.extend(self.state.dimensions_symbols().iter().cloned());
        time_dimensions.extend(self.state.time_dimensions_symbols().iter().cloned());
        for child in self.input.iter() {
            child.collect_all_non_multi_stage_dimension_impl(dimensions, time_dimensions);
        }
    }

    pub fn is_match_member_and_state(
        &self,
        member_node: &Rc<MemberSymbol>,
        state: &Rc<MultiStageAppliedState>,
    ) -> bool {
        member_node.full_name() == self.member_name() && state == &self.state
    }
}
