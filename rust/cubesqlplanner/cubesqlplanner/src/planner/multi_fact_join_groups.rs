use crate::cube_bridge::join_definition::JoinDefinition;
use crate::plan::FilterItem;
use crate::planner::join_hints::JoinHints;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{collect_join_hints, has_multi_stage_members};
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Clone)]
pub struct MultiFactJoinGroups {
    query_tools: Rc<QueryTools>,
    base_hints: JoinHints,
    groups: Vec<(Rc<dyn JoinDefinition>, Vec<Rc<MemberSymbol>>)>,
}

impl MultiFactJoinGroups {
    pub fn empty(query_tools: Rc<QueryTools>) -> Self {
        Self {
            query_tools,
            base_hints: JoinHints::new(),
            groups: vec![],
        }
    }

    pub fn try_new(
        query_tools: Rc<QueryTools>,
        query_join_hints: &JoinHints,
        measures: &[Rc<MemberSymbol>],
        dimensions: &[Rc<MemberSymbol>],
        order_dimensions: &[Rc<MemberSymbol>],
        time_dimensions: &[Rc<MemberSymbol>],
        time_dimensions_filters: &[FilterItem],
        dimensions_filters: &[FilterItem],
        measures_filters: &[FilterItem],
        segments: &[FilterItem],
    ) -> Result<Self, CubeError> {
        let mut base_hints = query_join_hints.clone();

        for sym in dimensions
            .iter()
            .chain(order_dimensions.iter())
            .chain(time_dimensions.iter())
        {
            base_hints.extend(&collect_join_hints(sym)?);
        }

        let mut filter_symbols = Vec::new();
        for filter_vec in [
            time_dimensions_filters,
            dimensions_filters,
            segments,
            // TODO This is not quite correct. Decide on how to handle it.
            // Keeping it here just to blow up on unsupported case
            measures_filters,
        ] {
            for item in filter_vec.iter() {
                item.find_all_member_evaluators(&mut filter_symbols);
            }
        }
        for sym in filter_symbols.iter() {
            base_hints.extend(&collect_join_hints(sym)?);
        }

        Self::from_base_hints(query_tools, base_hints, measures)
    }

    pub fn for_measures(
        &self,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<Self, CubeError> {
        Self::from_base_hints(self.query_tools.clone(), self.base_hints.clone(), measures)
    }

    fn from_base_hints(
        query_tools: Rc<QueryTools>,
        base_hints: JoinHints,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<Self, CubeError> {
        let mut filtered_measures = Vec::new();
        for m in measures {
            if !has_multi_stage_members(m, true)? {
                filtered_measures.push(m.clone());
            }
        }

        let measures_to_join = if filtered_measures.is_empty() {
            if base_hints.is_empty() {
                vec![]
            } else {
                let (key, join) = query_tools.join_for_hints(&base_hints)?;
                vec![(Vec::new(), key, join)]
            }
        } else {
            filtered_measures
                .iter()
                .map(|m| -> Result<_, CubeError> {
                    let mut hints = base_hints.clone();
                    hints.extend(&collect_join_hints(m)?);
                    let (key, join) = query_tools.join_for_hints(&hints)?;
                    Ok((vec![m.clone()], key, join))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let groups = measures_to_join
            .into_iter()
            .into_group_map_by(|(_, key, _)| key.clone())
            .into_values()
            .map(|group| {
                let join = group.first().unwrap().2.clone();
                let all_measures = group.into_iter().flat_map(|(m, _, _)| m).collect::<Vec<_>>();
                (join, all_measures)
            })
            .collect_vec();

        Ok(Self {
            query_tools,
            base_hints,
            groups,
        })
    }

    pub fn is_multi_fact(&self) -> bool {
        self.groups.len() > 1
    }

    pub fn groups(&self) -> &[(Rc<dyn JoinDefinition>, Vec<Rc<MemberSymbol>>)] {
        &self.groups
    }

    pub fn single_join(&self) -> Result<Option<Rc<dyn JoinDefinition>>, CubeError> {
        if self.groups.is_empty() {
            return Ok(None);
        }
        if self.groups.len() > 1 {
            return Err(CubeError::internal(format!(
                "Expected just one multi-fact join group for simple query but got multiple: {}",
                self.groups
                    .iter()
                    .map(|(_, measures)| format!(
                        "({})",
                        measures.iter().map(|m| m.full_name()).join(", ")
                    ))
                    .join(", ")
            )));
        }
        Ok(Some(self.groups.first().unwrap().0.clone()))
    }
}
