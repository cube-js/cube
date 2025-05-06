use super::{
    MultiStageAppliedState, MultiStageInodeMember, MultiStageInodeMemberType,
    MultiStageLeafMemberType, MultiStageMember, MultiStageMemberQueryPlanner, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowPlanner,
};
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::BaseMember;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct MultiStageQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    rolling_window_planner: RollingWindowPlanner,
}

impl MultiStageQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            rolling_window_planner: RollingWindowPlanner::new(
                query_tools.clone(),
                query_properties.clone(),
            ),
            query_tools,
            query_properties,
        }
    }

    pub fn plan_queries(
        &self,
    ) -> Result<
        (
            Vec<Rc<LogicalMultiStageMember>>,
            Vec<Rc<MultiStageSubqueryRef>>,
        ),
        CubeError,
    > {
        let multi_stage_members = self
            .query_properties
            .all_members(false)
            .into_iter()
            .filter_map(|memb: Rc<dyn BaseMember>| -> Option<Result<_, CubeError>> {
                match has_multi_stage_members(&memb.member_evaluator(), false) {
                    Ok(true) => Some(Ok(memb)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if multi_stage_members.is_empty() {
            return Ok((vec![], vec![]));
        }
        let mut descriptions = Vec::new();
        let state = MultiStageAppliedState::new(
            self.query_properties.time_dimensions().clone(),
            self.query_properties.dimensions().clone(),
            self.query_properties.time_dimensions_filters().clone(),
            self.query_properties.dimensions_filters().clone(),
            self.query_properties.measures_filters().clone(),
            self.query_properties.segments().clone(),
        );

        let top_level_ctes = multi_stage_members
            .into_iter()
            .map(|memb| -> Result<_, CubeError> {
                let description = self.make_queries_descriptions(
                    memb.member_evaluator().clone(),
                    state.clone(),
                    &mut descriptions,
                )?;
                let result = (
                    description.alias().clone(),
                    vec![description.member_node().clone()],
                );
                Ok(result)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let all_queries = descriptions
            .into_iter()
            .map(|descr| -> Result<_, CubeError> {
                let planner = MultiStageMemberQueryPlanner::new(
                    self.query_tools.clone(),
                    self.query_properties.clone(),
                    descr.clone(),
                );
                let res = planner.plan_logical_query()?;
                Ok(res)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let top_level_ctes = top_level_ctes
            .iter()
            .map(|(alias, symbols)| {
                Rc::new(MultiStageSubqueryRef {
                    name: alias.clone(),
                    symbols: symbols.clone(),
                })
            })
            .collect_vec();

        Ok((all_queries, top_level_ctes))
    }

    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<MemberSymbol>,
    ) -> Result<(MultiStageInodeMember, bool), CubeError> {
        let inode = if let Some(measure) =
            BaseMeasure::try_new(base_member.clone(), self.query_tools.clone())?
        {
            let member_type = if measure.measure_type() == "rank" {
                MultiStageInodeMemberType::Rank
            } else if !measure.is_calculated() {
                MultiStageInodeMemberType::Aggregate
            } else {
                MultiStageInodeMemberType::Calculate
            };

            let time_shifts = measure.time_shifts();

            let is_ungrupped = match &member_type {
                MultiStageInodeMemberType::Rank | MultiStageInodeMemberType::Calculate => true,
                _ => self.query_properties.ungrouped(),
            };
            let (reduce_by, add_group_by, group_by) =
                if let Ok(measure_symbol) = measure.member_evaluator().as_measure() {
                    (
                        measure_symbol.reduce_by().clone().unwrap_or_default(),
                        measure_symbol.add_group_by().clone().unwrap_or_default(),
                        measure_symbol.group_by().clone(),
                    )
                } else {
                    (vec![], vec![], None)
                };
            (
                MultiStageInodeMember::new(
                    member_type,
                    reduce_by,
                    add_group_by,
                    group_by,
                    time_shifts,
                ),
                is_ungrupped,
            )
        } else {
            (
                MultiStageInodeMember::new(
                    MultiStageInodeMemberType::Calculate,
                    vec![],
                    vec![],
                    None,
                    vec![],
                ),
                self.query_properties.ungrouped(),
            )
        };
        Ok(inode)
    }

    fn make_queries_descriptions(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let member_name = member.full_name();
        if let Some(exists) = descriptions
            .iter()
            .find(|q| q.is_match_member_and_state(&member, &state))
        {
            return Ok(exists.clone());
        };

        if let Some(rolling_window_query) = self.rolling_window_planner.try_plan_rolling_window(
            member.clone(),
            state.clone(),
            descriptions,
        )? {
            return Ok(rolling_window_query);
        }

        let childs = member_childs(&member)?;
        let has_multi_stage_members = has_multi_stage_members(&member, false)?;
        let description = if childs.is_empty() || !has_multi_stage_members {
            if has_multi_stage_members {
                return Err(CubeError::internal(format!(
                    "Leaf multi stage query cannot contain multi stage member"
                )));
            }

            let alias = format!("cte_{}", descriptions.len());
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                    member.clone(),
                    self.query_properties.ungrouped(),
                    false,
                ),
                state.clone(),
                vec![],
                alias.clone(),
            )
        } else {
            let (multi_stage_member, is_ungrupped) =
                self.create_multi_stage_inode_member(member.clone())?;

            let dimensions_to_add = multi_stage_member
                .add_group_by_symbols()
                .iter()
                .map(|symbol| {
                    BaseDimension::try_new_required(symbol.clone(), self.query_tools.clone())
                })
                .collect::<Result<Vec<_>, _>>()?;

            let new_state = if !dimensions_to_add.is_empty()
                || !multi_stage_member.time_shifts().is_empty()
                || state.has_filters_for_member(&member_name)
            {
                let mut new_state = state.clone_state();
                if !dimensions_to_add.is_empty() {
                    new_state.add_dimensions(dimensions_to_add);
                }
                if !multi_stage_member.time_shifts().is_empty() {
                    new_state.add_time_shifts(multi_stage_member.time_shifts().clone());
                }
                if state.has_filters_for_member(&member_name) {
                    new_state.remove_filter_for_member(&member_name);
                }
                Rc::new(new_state)
            } else {
                state.clone()
            };

            let input = childs
                .into_iter()
                .map(
                    |child| -> Result<Rc<MultiStageQueryDescription>, CubeError> {
                        self.make_queries_descriptions(child, new_state.clone(), descriptions)
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;

            let alias = format!("cte_{}", descriptions.len());
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Inode(multi_stage_member),
                    member,
                    is_ungrupped,
                    false,
                ),
                state.clone(),
                input,
                alias.clone(),
            )
        };

        descriptions.push(description.clone());
        Ok(description)
    }
}
