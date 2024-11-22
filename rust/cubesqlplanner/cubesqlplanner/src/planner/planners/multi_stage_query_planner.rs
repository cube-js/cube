use super::multi_stage::{
    MultiStageAppliedState, MultiStageInodeMember, MultiStageInodeMemberType,
    MultiStageLeafMemberType, MultiStageMember, MultiStageMemberQueryPlanner, MultiStageMemberType,
    MultiStageQueryDescription, MultiStageTimeShift,
};
use crate::plan::{Cte, From, Schema, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::EvaluationNode;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultiStageQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
}

impl MultiStageQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }
    pub fn plan_queries(&self) -> Result<(Vec<Rc<Cte>>, Vec<Rc<Select>>), CubeError> {
        let multi_stage_members = self
            .query_properties
            .all_members(false)
            .into_iter()
            .filter_map(|memb| -> Option<Result<_, CubeError>> {
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
        let all_filter_members = self.query_properties.all_filtered_members();
        let state = MultiStageAppliedState::new(
            self.query_properties.dimensions().clone(),
            all_filter_members,
        );

        let top_level_ctes = multi_stage_members
            .into_iter()
            .map(|memb| -> Result<_, CubeError> {
                Ok(self
                    .make_queries_descriptions(
                        memb.member_evaluator().clone(),
                        state.clone(),
                        &mut descriptions,
                    )?
                    .alias()
                    .clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut cte_schemas = HashMap::new();
        let all_queries = descriptions
            .into_iter()
            .map(|descr| -> Result<_, CubeError> {
                let res = MultiStageMemberQueryPlanner::new(
                    self.query_tools.clone(),
                    self.query_properties.clone(),
                    descr.clone(),
                )
                .plan_query(&cte_schemas)?;
                cte_schemas.insert(descr.alias().clone(), Rc::new(res.make_schema()));
                Ok(res)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let cte_joins = top_level_ctes
            .iter()
            .map(|alias| self.cte_select(alias, &cte_schemas))
            .collect_vec();

        Ok((all_queries, cte_joins))
    }

    pub fn cte_select(
        &self,
        alias: &String,
        cte_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Rc<Select> {
        let schema = cte_schemas.get(alias).unwrap().clone();
        let select_builder = SelectBuilder::new(
            From::new_from_table_reference(alias.clone(), schema, None),
            VisitorContext::default(SqlNodesFactory::new()),
        );

        Rc::new(select_builder.build())
    }

    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<EvaluationNode>,
    ) -> Result<MultiStageInodeMember, CubeError> {
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

            let time_shifts = if let Some(refs) = measure.time_shift_references() {
                let time_shifts = refs
                    .iter()
                    .map(|r| MultiStageTimeShift::try_from_reference(r))
                    .collect::<Result<Vec<_>, _>>()?;
                time_shifts
            } else {
                vec![]
            };
            let is_ungrupped = match &member_type {
                MultiStageInodeMemberType::Rank | MultiStageInodeMemberType::Calculate => true,
                _ => false,
            };
            MultiStageInodeMember::new(
                member_type,
                measure.reduce_by().clone().unwrap_or_default(),
                measure.add_group_by().clone().unwrap_or_default(),
                measure.group_by().clone(),
                time_shifts,
                is_ungrupped,
            )
        } else {
            MultiStageInodeMember::new(
                MultiStageInodeMemberType::Calculate,
                vec![],
                vec![],
                None,
                vec![],
                false,
            )
        };
        Ok(inode)
    }

    fn add_time_seria(
        &self,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description =
            if let Some(description) = descriptions.iter().find(|d| d.alias() == "time_seria") {
                description.clone()
            } else {
                let time_dimensions = self.query_properties.time_dimensions();
                if time_dimensions.len() != 1 {
                    return Err(CubeError::internal(
                        "Rolling window requires one time dimension".to_string(),
                    ));
                }
                let time_dimension = time_dimensions[0].clone();
                let time_seria_node = MultiStageQueryDescription::new(
                    MultiStageMember::new(
                        MultiStageMemberType::Leaf(MultiStageLeafMemberType::TimeSeria(
                            time_dimension.clone(),
                        )),
                        time_dimension.member_evaluator(),
                    ),
                    state.clone(),
                    vec![],
                    "time_seria".to_string(),
                );
                descriptions.push(time_seria_node.clone());
                time_seria_node
            };
        Ok(description)
    }

    fn add_rolling_window_base(
        &self,
        member: Rc<EvaluationNode>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let alias = format!("cte_{}", descriptions.len());
        let description = MultiStageQueryDescription::new(
            MultiStageMember::new(
                MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                member.clone(),
            ),
            state.clone(),
            vec![],
            alias.clone(),
        );
        descriptions.push(description.clone());
        Ok(description)
    }

    fn try_make_rolling_window(
        &self,
        member: Rc<EvaluationNode>,
        state: Rc<MultiStageAppliedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Option<Rc<MultiStageQueryDescription>>, CubeError> {
        if let Some(measure) = BaseMeasure::try_new(member.clone(), self.query_tools.clone())? {
            if let Some(rolling_window) = measure.rolling_window() {
                self.add_time_seria(state.clone(), descriptions)?;
                let input = vec![self.add_rolling_window_base(
                    member.clone(),
                    state.clone(),
                    descriptions,
                )?];

                let alias = format!("cte_{}", descriptions.len());

                let inode_member = MultiStageInodeMember::new(
                    MultiStageInodeMemberType::RollingWindow,
                    vec![],
                    vec![],
                    None,
                    vec![],
                    false,
                );

                let description = MultiStageQueryDescription::new(
                    MultiStageMember::new(MultiStageMemberType::Inode(inode_member), member),
                    state.clone(),
                    input,
                    alias.clone(),
                );
                descriptions.push(description.clone());
                Ok(Some(description))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn make_queries_descriptions(
        &self,
        member: Rc<EvaluationNode>,
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

        if let Some(rolling_window_query) =
            self.try_make_rolling_window(member.clone(), state.clone(), descriptions)?
        {
            return Ok(rolling_window_query);
        }

        let childs = member_childs(&member)?;

        let description = if childs.is_empty() {
            if has_multi_stage_members(&member, false)? {
                return Err(CubeError::internal(format!(
                    "Leaf multi stage query cannot contain multi stage member"
                )));
            }

            let alias = format!("cte_{}", descriptions.len());
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                    member.clone(),
                ),
                state.clone(),
                vec![],
                alias.clone(),
            )
        } else {
            let multi_stage_member = self.create_multi_stage_inode_member(member.clone())?;

            let dimensions_to_add = multi_stage_member
                .add_group_by()
                .iter()
                .map(|name| self.compile_dimension(name))
                .collect::<Result<Vec<_>, _>>()?;

            let new_state = if !dimensions_to_add.is_empty()
                || !multi_stage_member.time_shifts().is_empty()
                || state.is_filter_allowed(&member_name)
            {
                let mut new_state = state.clone_state();
                if !dimensions_to_add.is_empty() {
                    new_state.add_dimensions(dimensions_to_add);
                }
                if !multi_stage_member.time_shifts().is_empty() {
                    new_state.add_time_shifts(multi_stage_member.time_shifts().clone());
                }
                if state.is_filter_allowed(&member_name) {
                    new_state.disallow_filter(&member_name);
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
                MultiStageMember::new(MultiStageMemberType::Inode(multi_stage_member), member),
                state.clone(),
                input,
                alias.clone(),
            )
        };

        descriptions.push(description.clone());
        Ok(description)
    }

    fn compile_dimension(&self, name: &String) -> Result<Rc<BaseDimension>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let evaluator = evaluator_compiler.add_dimension_evaluator(name.clone())?;
        BaseDimension::try_new_required(evaluator, self.query_tools.clone())
    }
}
