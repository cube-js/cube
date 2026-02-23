use super::*;
use crate::{plan::QualifiedColumnName, planner::sql_evaluator::MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::{collections::HashMap, rc::Rc};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct PreAggregation {
    name: String,
    schema: Rc<LogicalSchema>,
    #[builder(default)]
    measures: Vec<Rc<MemberSymbol>>,
    #[builder(default)]
    dimensions: Vec<Rc<MemberSymbol>>,
    #[builder(default)]
    time_dimensions: Vec<Rc<MemberSymbol>>,
    #[builder(default)]
    segments: Vec<Rc<MemberSymbol>>,
    external: bool,
    #[builder(default)]
    granularity: Option<String>,
    source: Rc<PreAggregationSource>,
    cube_name: String,
}

impl PreAggregation {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }

    pub fn dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.time_dimensions
    }

    pub fn segments(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.segments
    }

    pub fn external(&self) -> bool {
        self.external
    }

    pub fn granularity(&self) -> &Option<String> {
        &self.granularity
    }

    pub fn source(&self) -> &Rc<PreAggregationSource> {
        &self.source
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
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

        for dim in self.dimensions().iter() {
            let alias = dim.alias();
            res.insert(
                dim.full_name(),
                QualifiedColumnName::new(None, alias.clone()),
            );
        }
        for dim in self.time_dimensions().iter() {
            let (base_symbol, granularity) = if let Ok(td) = dim.as_time_dimension() {
                (td.base_symbol().clone(), td.granularity().clone())
            } else {
                (dim.clone(), None)
            };
            let suffix = if let Some(granularity) = &granularity {
                format!("_{}", granularity.clone())
            } else {
                "".to_string()
            };
            let alias = format!("{}{}", base_symbol.alias(), suffix);
            res.insert(
                base_symbol.full_name(),
                QualifiedColumnName::new(None, alias.clone()),
            );
        }

        for segment in self.segments().iter() {
            let alias = segment.alias();
            res.insert(segment.full_name(), QualifiedColumnName::new(None, alias));
        }

        if let PreAggregationSource::Join(join) = self.source().as_ref() {
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
        self.measures()
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
        result.println(&format!("name: {}", self.name()), &state);
        result.println(&format!("cube_name: {}", self.cube_name()), &state);
        result.println(&format!("source:"), &state);
        match self.source().as_ref() {
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
        result.println(&format!("external: {}", self.external()), &state);
        result.println(
            &format!(
                "granularity: {}",
                self.granularity().clone().unwrap_or("None".to_string())
            ),
            &state,
        );
        result.println(
            &format!(
                "-time_dimensions: {}",
                &self
                    .time_dimensions()
                    .iter()
                    .map(|d| d.full_name())
                    .join(", ")
            ),
            &state,
        );
        result.println(
            &format!("-dimensions: {}", print_symbols(self.dimensions())),
            &state,
        );
        result.println(
            &format!("-measures: {}", print_symbols(self.measures())),
            &state,
        );
    }
}
