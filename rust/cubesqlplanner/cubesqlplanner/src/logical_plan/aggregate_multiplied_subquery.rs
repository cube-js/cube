use super::pretty_print::*;
use super::*;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub enum AggregateMultipliedSubquerySouce {
    Cube(Rc<Cube>),
    MeasureSubquery(Rc<MeasureSubquery>),
}

impl AggregateMultipliedSubquerySouce {
    fn as_plan_node(&self) -> PlanNode {
        match self {
            Self::Cube(item) => item.as_plan_node(),
            Self::MeasureSubquery(item) => item.as_plan_node(),
        }
    }
    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match self {
            Self::Cube(_) => Self::Cube(plan_node.into_logical_node()?),
            Self::MeasureSubquery(_) => Self::MeasureSubquery(plan_node.into_logical_node()?),
        })
    }
}

pub struct AggregateMultipliedSubquery {
    pub schema: Rc<LogicalSchema>,
    pub keys_subquery: Rc<KeysSubQuery>,
    pub source: AggregateMultipliedSubquerySouce,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
}

impl LogicalNode for AggregateMultipliedSubquery {
    type InputsType = AggregateMultipliedSubqueryInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::AggregateMultipliedSubquery(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let keys_subquery = self.keys_subquery.as_plan_node();
        let source = self.source.as_plan_node();
        let dimension_subqueries = self
            .dimension_subqueries
            .iter()
            .map(|itm| itm.as_plan_node())
            .collect_vec();
        AggregateMultipliedSubqueryInput {
            keys_subquery,
            source,
            dimension_subqueries,
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let AggregateMultipliedSubqueryInput {
            keys_subquery,
            source,
            dimension_subqueries,
        } = inputs;

        let result = Self {
            schema: self.schema.clone(),
            keys_subquery: keys_subquery.into_logical_node()?,
            source: self.source.with_plan_node(source)?,
            dimension_subqueries: dimension_subqueries
                .into_iter()
                .map(|itm| itm.into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
        };

        Ok(Rc::new(result))
    }

    fn node_name(&self) -> &'static str {
        "AggregateMultipliedSubquery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::AggregateMultipliedSubquery(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "AggregateMultipliedSubquery"))
        }
    }
}

pub struct AggregateMultipliedSubqueryInput {
    pub keys_subquery: PlanNode,
    pub source: PlanNode,
    pub dimension_subqueries: Vec<PlanNode>,
}

impl NodeInputs for AggregateMultipliedSubqueryInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(
            std::iter::once(&self.keys_subquery)
                .chain(std::iter::once(&self.source))
                .chain(self.dimension_subqueries.iter()),
        )
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(
            std::iter::once(&mut self.keys_subquery)
                .chain(std::iter::once(&mut self.source))
                .chain(self.dimension_subqueries.iter_mut()),
        )
    }
}

impl PrettyPrint for AggregateMultipliedSubquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("AggregateMultipliedSubquery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("keys_subquery:", &state);
        self.keys_subquery.pretty_print(result, &details_state);
        result.println("source:", &state);
        match &self.source {
            AggregateMultipliedSubquerySouce::Cube(cube) => {
                result.println("Cube:", &details_state);
                cube.pretty_print(result, &details_state.new_level());
            }
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => {
                result.println(&format!("MeasureSubquery: "), &details_state);
                measure_subquery.pretty_print(result, &details_state);
            }
        }
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            let details_state = state.new_level();
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
    }
}
