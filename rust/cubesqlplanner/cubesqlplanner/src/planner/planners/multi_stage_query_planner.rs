use super::multi_stage::MultiStageMemberQueryPlanner;
use super::multi_stage::{MultiStageApplyedState, MultiStageQueryDescription};
use super::{FullKeyAggregateQueryPlanner, OrderPlanner, SimpleQueryPlanner};
use crate::plan::{
    Expr, From, FromSource, Join, JoinItem, JoinSource, OrderBy, QueryPlan, Select, Subquery,
};
use crate::planner::base_join_condition::DimensionJoinCondition;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::member_childs;
use crate::planner::sql_evaluator::sql_nodes::{
    multi_stage_rank_node_processor, with_render_references_default_node_processor,
};
use crate::planner::sql_evaluator::EvaluationNode;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, BaseMember, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultiStageQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    order_planner: OrderPlanner,
}

impl MultiStageQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            order_planner: OrderPlanner::new(query_properties.clone()),
            query_tools,
            query_properties,
        }
    }
    pub fn get_cte_queries(
        &self,
        multi_stage_members: &Vec<Rc<BaseMeasure>>,
    ) -> Result<(Vec<Rc<Subquery>>, Vec<String>), CubeError> {
        let mut descriptions = Vec::new();
        let state = MultiStageApplyedState::new(self.query_properties.dimensions().clone());

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

        let all_queries = descriptions
            .into_iter()
            .map(|descr| {
                MultiStageMemberQueryPlanner::new(
                    self.query_tools.clone(),
                    self.query_properties.clone(),
                    descr.clone(),
                )
                .plan_query()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((all_queries, top_level_ctes))
    }

    pub fn cte_select(&self, alias: &String) -> Rc<Select> {
        Rc::new(Select {
            projection: vec![Expr::Asterix],
            from: From::new_from_table_reference(alias.clone(), None),
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            context: VisitorContext::default(),
            ctes: vec![],
            is_distinct: false,
        })
    }

    fn make_queries_descriptions(
        &self,
        member: Rc<EvaluationNode>,
        state: Rc<MultiStageApplyedState>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        if let Some(exists) = descriptions
            .iter()
            .find(|q| q.is_match_member_and_state(&member, &state))
        {
            return Ok(exists.clone());
        };

        let new_state = if let Some(measure) =
            BaseMeasure::try_new_from_precompiled(member.clone(), self.query_tools.clone())
        {
            if let Some(add_group_by) = measure.add_group_by() {
                let dimensions_to_add = add_group_by
                    .iter()
                    .map(|name| self.compile_dimension(name))
                    .collect::<Result<Vec<_>, _>>()?;
                state.add_dimensions(dimensions_to_add)
            } else {
                state.clone()
            }
        } else {
            state.clone()
        };

        let childs = member_childs(&member)?;
        let input = childs
            .into_iter()
            .map(
                |child| -> Result<Rc<MultiStageQueryDescription>, CubeError> {
                    self.make_queries_descriptions(child, new_state.clone(), descriptions)
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        let alias = format!("cte_{}", descriptions.len());
        let description =
            MultiStageQueryDescription::new(member, state.clone(), input, alias.clone());
        descriptions.push(description.clone());
        Ok(description)
    }

    fn compile_dimension(&self, name: &String) -> Result<Rc<BaseDimension>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let evaluator = evaluator_compiler.add_dimension_evaluator(name.clone())?;
        BaseDimension::try_new(name.clone(), self.query_tools.clone(), evaluator)
    }
}
