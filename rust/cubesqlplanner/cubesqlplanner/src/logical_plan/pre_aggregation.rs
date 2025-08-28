use super::*;
use crate::{plan::QualifiedColumnName, planner::sql_evaluator::MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::{collections::HashMap, rc::Rc};

pub struct PreAggregation {
    pub name: String,
    pub schema: Rc<LogicalSchema>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<(Rc<MemberSymbol>, Option<String>)>,
    pub external: bool,
    pub granularity: Option<String>,
    pub source: Rc<PreAggregationSource>,
    pub cube_name: String,
}

impl LogicalNode for PreAggregation {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::PreAggregation(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![] // PreAggregation has no inputs
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn node_name(&self) -> &'static str {
        "PreAggregation"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::PreAggregation(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "PreAggregation"))
        }
    }
}

impl PreAggregation {
    pub fn all_dimensions_refererences(&self) -> HashMap<String, QualifiedColumnName> {
        let mut res = HashMap::new();

        for dim in self.dimensions.iter() {
            let alias = dim.alias();
            res.insert(
                dim.full_name(),
                QualifiedColumnName::new(None, alias.clone()),
            );
        }
        for (dim, granularity) in self.time_dimensions.iter() {
            let base_symbol = if let Ok(td) = dim.as_time_dimension() {
                td.base_symbol().clone()
            } else {
                dim.clone()
            };
            let suffix = if let Some(granularity) = &granularity {
                format!("_{}", granularity.clone())
            } else {
                "".to_string()
            };
            let alias = format!("{}{}", base_symbol.alias(), suffix);
            res.insert(
                dim.full_name(),
                QualifiedColumnName::new(None, alias.clone()),
            );
        }

        if let PreAggregationSource::Join(join) = self.source.as_ref() {
            for item in join.items.iter() {
                for member in item.from_members.iter().chain(item.to_members.iter()) {
                    let alias = member.alias();
                    res.insert(
                        member.full_name(),
                        QualifiedColumnName::new(None, alias.clone()),
                    );
                }
            }
        }

        res
    }
    pub fn all_measures_refererences(&self) -> HashMap<String, QualifiedColumnName> {
        self.measures
            .iter()
            .map(|measure| {
                let alias = measure.alias();
                (
                    measure.full_name(),
                    QualifiedColumnName::new(None, alias.clone()),
                )
            })
            .collect()
    }
}

impl PrettyPrint for PreAggregation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("PreAggregation: ", state);
        let state = state.new_level();
        result.println(&format!("name: {}", self.name), &state);
        result.println(&format!("cube_name: {}", self.cube_name), &state);
        result.println(&format!("source:"), &state);
        match self.source.as_ref() {
            PreAggregationSource::Single(table) => {
                let state = state.new_level();
                result.println(
                    &format!("table: {}.{}", table.cube_name, table.name),
                    &state,
                );
            }
            PreAggregationSource::Join(_) => {
                let state = state.new_level();
                result.println(&format!("rollup join"), &state);
            }
            PreAggregationSource::Union(union) => {
                result.println("Union:", &state);
                let state = state.new_level();
                for item in union.items.iter() {
                    result.println(&format!("-{}.{}", item.cube_name, item.name), &state);
                }
            }
        }
        result.println(&format!("external: {}", self.external), &state);
        result.println(
            &format!(
                "granularity: {}",
                self.granularity.clone().unwrap_or("None".to_string())
            ),
            &state,
        );
        result.println(
            &format!(
                "-time_dimensions: {}",
                &self
                    .time_dimensions
                    .iter()
                    .map(|(d, granularity)| format!(
                        "({} {})",
                        d.full_name(),
                        granularity.clone().unwrap_or("None".to_string())
                    ))
                    .join(", ")
            ),
            &state,
        );
        result.println(
            &format!("-dimensions: {}", print_symbols(&self.dimensions)),
            &state,
        );
        result.println(
            &format!("-measures: {}", print_symbols(&self.measures)),
            &state,
        );
    }
}
