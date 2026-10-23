use crate::logical_plan::*;
use crate::planner::MemberId;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// Gives every query the CTE computing the multi-stage dimensions it
/// selects as one of its inputs. A CTE is found by the set of dimensions
/// it resolves, and a query can only read the CTEs defined before it.
pub struct MultiStageDimensionInputs {
    inputs: HashMap<Vec<MemberId>, MultiStageDimensionInput>,
}

impl MultiStageDimensionInputs {
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
        }
    }

    pub fn apply(mut self, root: Rc<RootQuery>) -> Result<Rc<RootQuery>, CubeError> {
        let mut ctes = Vec::with_capacity(root.ctes().len());
        for cte in root.ctes().iter() {
            let cte: Rc<LogicalMultiStageMember> = match self.rewrite(cte.as_plan_node())? {
                Some(rewritten) => rewritten.into_logical_node()?,
                None => cte.clone(),
            };
            if let MultiStageMemberLogicalType::DimensionCalculation(calculation) = &cte.member_type
            {
                let cte_ref = MultiStageSubqueryRef::builder()
                    .name(cte.name.clone())
                    .schema(calculation.schema().clone())
                    .build();
                self.inputs.insert(
                    calculation.resolved_dimensions()?,
                    MultiStageDimensionInput {
                        cte: Rc::new(cte_ref),
                        keys: calculation.join_dimensions()?,
                    },
                );
            }
            ctes.push(cte);
        }
        let query = match self.rewrite(root.query().as_plan_node())? {
            Some(rewritten) => rewritten.into_logical_node()?,
            None => root.query().clone(),
        };
        Ok(Rc::new(
            RootQuery::builder().ctes(ctes).query(query).build(),
        ))
    }

    fn rewrite(&self, node: PlanNode) -> Result<Option<PlanNode>, CubeError> {
        // Served by a pre-aggregation, the subqueries of a multiplied
        // measure are never rendered.
        if let PlanNode::AggregateMultipliedSubquery(aggregate) = &node {
            if aggregate.pre_aggregation_override.is_some() {
                return Ok(None);
            }
        }
        let mut inputs = node.inputs();
        let mut changed = false;
        for input in inputs.iter_mut() {
            if let Some(rewritten) = self.rewrite(input.clone())? {
                *input = rewritten;
                changed = true;
            }
        }
        let node = if changed {
            node.with_inputs(inputs)?
        } else {
            node
        };
        if let PlanNode::Query(query) = &node {
            if let Some(query) = self.attach(query)? {
                return Ok(Some(query.as_plan_node()));
            }
        }
        Ok(changed.then_some(node))
    }

    fn attach(&self, query: &Rc<Query>) -> Result<Option<Rc<Query>>, CubeError> {
        // A full key aggregate over CTEs reads the multi-stage dimensions
        // from them; only one with no CTEs reads its dimensions alone.
        let source = match query.source() {
            QuerySource::QueryInputs(inputs) => inputs.clone(),
            QuerySource::FullKeyAggregate(aggregate) if aggregate.is_empty() => {
                Rc::new(QueryInputs::builder().build())
            }
            _ => return Ok(None),
        };
        let mut dimensions = query
            .schema()
            .multi_stage_dimensions()?
            .into_iter()
            .map(|member| member.resolve_reference_chain().id().clone())
            .collect_vec();
        if dimensions.is_empty() {
            return Ok(None);
        }
        // A multi-stage dimension brings its own cube into the join, so
        // inputs without one have nowhere to read the rest of the query from.
        if let QuerySource::QueryInputs(inputs) = query.source() {
            if inputs.join().is_none() {
                return Err(CubeError::internal(format!(
                    "Multi-stage dimensions {} are selected by a query without a join",
                    dimensions.iter().join(", ")
                )));
            }
        }

        dimensions.sort();
        dimensions.dedup();
        let Some(input) = self.inputs.get(&dimensions) else {
            return Err(CubeError::internal(format!(
                "Cannot find source for resolve multi stage dimensions {}",
                dimensions.iter().join(", ")
            )));
        };
        let mut query = (**query).clone();
        query.set_source(source.with_multi_stage_dimension(input.clone()).into());
        Ok(Some(Rc::new(query)))
    }
}
